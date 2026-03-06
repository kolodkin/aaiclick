"""Task execution utilities for orchestration backend."""

from __future__ import annotations

import asyncio
import importlib
from datetime import datetime
from typing import Any, Callable, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from aaiclick.data.data_context import (
    _get_data_state,
    data_context,
    register_object,
)
from aaiclick.data.lifecycle import LifecycleHandler
from aaiclick.data.object import Object, View

from .context import get_orch_session
from .decorators import TaskFactory
from .logging import capture_task_output
from .models import Job, JobStatus, Task, TaskStatus


def import_callback(entrypoint: str) -> Callable:
    """
    Import a callback function from an entrypoint string.

    If the imported attribute is a TaskFactory (from @task decorator),
    unwraps it to get the original function.

    Args:
        entrypoint: Dot-separated module path and function name
                   (e.g., "mymodule.submodule.my_function")

    Returns:
        Callable: The imported function

    Raises:
        ImportError: If module cannot be imported
        AttributeError: If function not found in module
    """
    parts = entrypoint.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid entrypoint format: {entrypoint}. Expected 'module.function'")

    module_path, function_name = parts
    module = importlib.import_module(module_path)
    attr = getattr(module, function_name)

    if isinstance(attr, TaskFactory):
        return attr.func

    return attr


async def _resolve_upstream_ref(ref: dict, session: AsyncSession) -> Any:
    """Resolve an upstream task reference to its result.

    Args:
        ref: Dict with ref_type="upstream" and task_id
        session: Database session for querying task result

    Returns:
        The upstream task's result (Object, View, or native value)

    Raises:
        ValueError: If upstream task not found or not completed
    """
    task_id = ref["task_id"]
    db_result = await session.execute(
        select(Task.result, Task.status, Task.job_id).where(Task.id == task_id)
    )
    row = db_result.one_or_none()

    if row is None:
        raise ValueError(f"Upstream task {task_id} not found")

    result, status, job_id = row
    if status != TaskStatus.COMPLETED:
        raise ValueError(f"Upstream task {task_id} is not completed (status: {status})")

    if result is None:
        return None

    # Result is already serialized - deserialize it
    # Add job_id for lifecycle claim if it's an Object/View
    if isinstance(result, dict) and "object_type" in result:
        result["job_id"] = job_id

    return result


async def _deserialize_value(value: Any, session: AsyncSession) -> Any:
    """Recursively deserialize a value from JSON format.

    Handles:
    - Upstream references (ref_type="upstream"): Resolves to task result
    - Object references (object_type="object"): Reconstructs Object
    - View references (object_type="view"): Reconstructs View
    - Lists/dicts: Recursively deserializes contents
    - Native Python types: Passed through unchanged

    Args:
        value: Serialized value from task kwargs
        session: Database session for resolving upstream refs

    Returns:
        Deserialized value ready for function call
    """
    if not isinstance(value, dict):
        if isinstance(value, list):
            return [await _deserialize_value(v, session) for v in value]
        return value

    # Check for upstream reference
    if value.get("ref_type") == "upstream":
        upstream_result = await _resolve_upstream_ref(value, session)
        # Recursively deserialize the upstream result
        return await _deserialize_value(upstream_result, session)

    # Check for Object/View reference
    if "object_type" in value:
        state = _get_data_state()
        obj_type = value["object_type"]

        if obj_type == "object":
            obj = Object(table=value["table"])
            obj._register()
            register_object(obj)
            if "job_id" in value and state.lifecycle is not None:
                await state.lifecycle.claim(value["table"], value["job_id"])
            return obj

        elif obj_type == "view":
            source = Object(table=value["table"])
            source._register()
            register_object(source)
            view = View(
                source=source,
                where=value.get("where"),
                limit=value.get("limit"),
                offset=value.get("offset"),
                order_by=value.get("order_by"),
                selected_fields=value.get("selected_fields"),
            )
            register_object(view)
            if "job_id" in value and state.lifecycle is not None:
                await state.lifecycle.claim(value["table"], value["job_id"])
            return view

        else:
            raise ValueError(f"Unknown object_type: {obj_type}")

    # Regular dict - recursively deserialize values
    return {k: await _deserialize_value(v, session) for k, v in value.items()}


async def deserialize_task_params(serialized_params: dict) -> dict:
    """
    Deserialize task parameters from JSON format.

    Supports:
    - Upstream references: Resolved to completed task results
    - Object/View references: Reconstructed and registered with DataContext
    - Native Python values: Passed through unchanged
    - Nested structures: Lists and dicts are recursively processed

    For parameters with job_id, releases the job-scoped pin ref
    via lifecycle.claim() (ownership transfer from orchestration to consumer).

    Args:
        serialized_params: Task kwargs from database (JSON-deserialized)

    Returns:
        dict: Deserialized kwargs ready for function call

    Raises:
        ValueError: Unknown object_type or upstream task not completed
    """
    if not serialized_params:
        return {}

    async with get_orch_session() as session:
        return {
            k: await _deserialize_value(v, session)
            for k, v in serialized_params.items()
        }


async def execute_task(
    task: Task,
    lifecycle: LifecycleHandler | None = None,
) -> Any:
    """
    Execute a single task with both DataContext and OrchContext available.

    Imports the callback function, deserializes kwargs inside a DataContext,
    captures output, and executes the function. When a lifecycle handler is
    provided, it is injected into DataContext for distributed refcounting.
    The caller is responsible for starting/stopping the lifecycle handler.

    After execution, if the result is an Object or View, it is pinned
    under the job scope so it survives lifecycle stop().

    Args:
        task: Task to execute
        lifecycle: Optional pre-started LifecycleHandler for distributed
                   refcounting with pin/claim ownership.

    Returns:
        Any: Result of the task function

    Raises:
        Exception: Re-raises any exception from the task function
    """
    func = import_callback(task.entrypoint)

    with capture_task_output(task.id):
        async with data_context(lifecycle=lifecycle):
            kwargs = await deserialize_task_params(task.kwargs)
            if asyncio.iscoroutinefunction(func):
                result = await func(**kwargs)
            else:
                result = func(**kwargs)

    if lifecycle is not None and isinstance(result, (Object, View)):
        lifecycle.pin(result.table)

    return result


def serialize_task_result(result: Any, job_id: int) -> Optional[dict]:
    """
    Serialize a task result to JSON-storable format.

    Handles Object and View types by creating reference dicts that include
    job_id for ownership tracking during deserialization (lifecycle.claim).

    Args:
        result: Task function return value
        job_id: ID of the job this task belongs to

    Returns:
        dict: Serialized result reference, or None if result is None
    """
    if result is None:
        return None

    if isinstance(result, Object):
        ref = result._serialize_ref()
        ref["job_id"] = job_id
        return ref

    return None


async def run_job_tasks(job: Job) -> None:
    """
    Execute all tasks for a job synchronously (test mode).

    This simulates worker behavior but runs in the current process.
    Tasks are fetched and executed one at a time in order of creation (snowflake ID).

    Args:
        job: Job whose tasks to execute

    Raises:
        Exception: If any task fails
    """
    async with get_orch_session() as session:
        # Update job to RUNNING
        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        session.add(job)
        await session.commit()

    job_failed = False
    error_msg = None

    # Fetch and execute one task at a time until no more pending tasks
    while True:
        async with get_orch_session() as session:
            # Fetch next pending task for this job
            result = await session.execute(
                select(Task)
                .where(Task.job_id == job.id, Task.status == TaskStatus.PENDING)
                .order_by(Task.id)
                .limit(1)
            )
            task = result.scalar_one_or_none()

            if task is None:
                # No more pending tasks
                break

            # Update task to RUNNING
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.utcnow()
            session.add(task)
            await session.commit()

            # Store IDs for later use (task object detaches after session closes)
            task_id = task.id
            task_job_id = task.job_id

        try:
            result = await execute_task(task)

            # Serialize result (Object or View) to JSON-storable reference
            result_ref = serialize_task_result(result, task_job_id)

            async with get_orch_session() as session:
                # Reload and update task to COMPLETED
                db_result = await session.execute(select(Task).where(Task.id == task_id))
                task = db_result.scalar_one()

                task.status = TaskStatus.COMPLETED
                task.completed_at = datetime.utcnow()
                task.result = result_ref

                session.add(task)
                await session.commit()

        except Exception as e:
            job_failed = True
            error_msg = str(e)

            async with get_orch_session() as session:
                # Reload and update task to FAILED
                db_result = await session.execute(select(Task).where(Task.id == task_id))
                task = db_result.scalar_one()

                task.status = TaskStatus.FAILED
                task.completed_at = datetime.utcnow()
                task.error = str(e)
                session.add(task)
                await session.commit()

            break

    async with get_orch_session() as session:
        # Reload job and update final status
        result = await session.execute(select(Job).where(Job.id == job.id))
        db_job = result.scalar_one()

        if job_failed:
            db_job.status = JobStatus.FAILED
            db_job.error = error_msg
        else:
            db_job.status = JobStatus.COMPLETED

        db_job.completed_at = datetime.utcnow()
        session.add(db_job)
        await session.commit()

        # Update in-memory job object
        job.status = db_job.status
        job.completed_at = db_job.completed_at
        job.error = db_job.error
