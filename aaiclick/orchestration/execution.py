"""Task execution utilities for orchestration backend."""

from __future__ import annotations

import asyncio
import importlib
from datetime import datetime
from typing import Any, Callable, Optional

from sqlmodel import select

from aaiclick.data import DataContext, get_data_context
from aaiclick.data.lifecycle import LifecycleHandler
from aaiclick.data.object import Object, View

from .context import get_orch_context_session
from .logging import capture_task_output
from .models import Job, JobStatus, Task, TaskStatus
from .pg_lifecycle import claim_table


def import_callback(entrypoint: str) -> Callable:
    """
    Import a callback function from an entrypoint string.

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
    return getattr(module, function_name)


async def deserialize_task_params(kwargs: dict) -> dict:
    """
    Deserialize task parameters from JSON format.

    All parameters must be aaiclick Objects or Views - native Python values
    are not supported. Reconstructs Object/View instances from serialized
    references and registers them with the current DataContext.

    For parameters with source_job_id, releases the job-scoped pin ref
    via claim_table() (ownership transfer from orchestration to consumer).

    Args:
        kwargs: Task kwargs from database (JSON-deserialized)

    Returns:
        dict: Deserialized kwargs ready for function call

    Raises:
        ValueError: Unknown object_type or missing required fields
    """
    if not kwargs:
        return {}

    result = {}
    ctx = get_data_context()

    for key, value in kwargs.items():
        if not isinstance(value, dict) or "object_type" not in value:
            raise ValueError(
                f"Parameter '{key}' must be an Object or View reference with 'object_type' field. "
                "Native Python values are not supported."
            )

        obj_type = value["object_type"]
        if obj_type == "object":
            obj = Object(table=value["table"])
            obj._register(ctx)
            ctx._register_object(obj)
            if "source_job_id" in value:
                await claim_table(value["table"], value["source_job_id"])
            result[key] = obj
        elif obj_type == "view":
            source = Object(table=value["table"])
            source._register(ctx)
            ctx._register_object(source)
            view = View(
                source=source,
                where=value.get("where"),
                limit=value.get("limit"),
                offset=value.get("offset"),
                order_by=value.get("order_by"),
                selected_fields=value.get("selected_fields"),
            )
            ctx._register_object(view)
            if "source_job_id" in value:
                await claim_table(value["table"], value["source_job_id"])
            result[key] = view
        else:
            raise ValueError(f"Unknown object_type: {obj_type}. Must be 'object' or 'view'.")

    return result


async def execute_task(
    task: Task,
    lifecycle_factory: Callable[[int], LifecycleHandler] | None = None,
) -> Any:
    """
    Execute a single task with both DataContext and OrchContext available.

    Imports the callback function, deserializes kwargs inside a DataContext,
    captures output, and executes the function. When a lifecycle_factory is
    provided, creates a per-task PgLifecycleHandler that:
    - Tracks execution refs via incref/decref
    - Pins result tables under job_id (survives stop)
    - Destructively cleans intermediates on stop

    Args:
        task: Task to execute
        lifecycle_factory: Optional factory that creates a LifecycleHandler
                          from job_id. When provided, DataContext uses it for
                          distributed refcounting with pin/claim ownership.

    Returns:
        Any: Result of the task function

    Raises:
        Exception: Re-raises any exception from the task function
    """
    func = import_callback(task.entrypoint)

    lifecycle = None
    if lifecycle_factory is not None:
        lifecycle = lifecycle_factory(task.job_id)
        await lifecycle.start()

    try:
        with capture_task_output(task.id):
            async with DataContext(lifecycle=lifecycle):
                kwargs = await deserialize_task_params(task.kwargs)
                if asyncio.iscoroutinefunction(func):
                    result = await func(**kwargs)
                else:
                    result = func(**kwargs)

        # PIN: transfer result ownership to job scope (after DataContext exit)
        if lifecycle is not None and isinstance(result, (Object, View)):
            lifecycle.pin(result.table)
    finally:
        if lifecycle is not None:
            await lifecycle.stop()

    return result


def serialize_task_result(
    result: Any, task_id: int, job_id: int
) -> Optional[dict]:
    """
    Serialize a task result to JSON-storable format.

    Handles Object and View types by creating reference dicts that include
    source_task_id and source_job_id for ownership tracking during
    deserialization (claim_table).

    Args:
        result: Task function return value
        task_id: ID of the task that produced this result
        job_id: ID of the job this task belongs to

    Returns:
        dict: Serialized result reference, or None if result is None
    """
    if result is None:
        return None

    # Check View first since View is a subclass of Object
    if isinstance(result, View):
        return {
            "object_type": "view",
            "table": result.table,
            "where": result.where,
            "limit": result.limit,
            "offset": result.offset,
            "order_by": result.order_by,
            "selected_fields": result.selected_fields,
            "source_task_id": task_id,
            "source_job_id": job_id,
        }
    elif isinstance(result, Object):
        return {
            "object_type": "object",
            "table": result.table,
            "source_task_id": task_id,
            "source_job_id": job_id,
        }

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
    async with get_orch_context_session() as session:
        # Update job to RUNNING
        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        session.add(job)
        await session.commit()

    job_failed = False
    error_msg = None

    # Fetch and execute one task at a time until no more pending tasks
    while True:
        async with get_orch_context_session() as session:
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
            result_ref = serialize_task_result(result, task_id, task_job_id)

            async with get_orch_context_session() as session:
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

            async with get_orch_context_session() as session:
                # Reload and update task to FAILED
                db_result = await session.execute(select(Task).where(Task.id == task_id))
                task = db_result.scalar_one()

                task.status = TaskStatus.FAILED
                task.completed_at = datetime.utcnow()
                task.error = str(e)
                session.add(task)
                await session.commit()

            break

    async with get_orch_context_session() as session:
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
