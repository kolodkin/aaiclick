"""Task execution utilities for orchestration backend."""

from __future__ import annotations

import asyncio
import importlib
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from aaiclick.data.data_context import (
    get_ch_client,
    get_data_lifecycle,
    register_object,
)
from aaiclick.data.ingest import _get_table_schema
from aaiclick.data.models import Schema
from aaiclick.data.object import Object, View

from .context import commit_tasks, get_orch_session, task_scope
from .decorators import JobFactory, TaskFactory
from .logging import capture_task_output
from .models import Dependency, Group, Job, JobStatus, Task, TaskStatus
from .worker_context import set_current_task_info


@dataclass
class TaskResult:
    """Explicit return type for tasks that yield both data and dynamic child tasks.

    Both fields default to None:
    - TaskResult(tasks=[t1, t2])        — tasks only, no data
    - TaskResult(data=value)            — data only, no tasks
    - TaskResult(data=value, tasks=[t]) — both
    """

    data: Any = None
    tasks: list = field(default_factory=list)


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

    if isinstance(attr, JobFactory):
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

    # Check for callable reference
    if value.get("ref_type") == "callable":
        return import_callback(value["entrypoint"])

    # Check for group_results reference (from reduce() collecting map results)
    if value.get("ref_type") == "group_results":
        group_id = value["group_id"]
        result = await session.execute(
            select(Task.result, Task.job_id)
            .where(
                Task.group_id == group_id,
                Task.status == TaskStatus.COMPLETED,
            )
            .order_by(Task.id)
        )
        rows = result.all()
        deserialized = []
        for row in rows:
            task_result, job_id = row
            if task_result is not None and isinstance(task_result, dict):
                task_result["job_id"] = job_id
            deserialized.append(await _deserialize_value(task_result, session))
        return deserialized

    # Check for native value wrapper
    if "native_value" in value and len(value) == 1:
        return value["native_value"]

    # Check for Object/View reference
    if "object_type" in value:
        obj_type = value["object_type"]

        if obj_type == "object":
            table = value["table"]
            is_persistent = value.get("persistent", False)
            fieldtype, columns = await _get_table_schema(table, get_ch_client())
            schema = Schema(fieldtype=fieldtype, columns=columns)
            obj = Object(table=table, schema=schema)
            if not is_persistent:
                obj._register()
            register_object(obj)
            if not is_persistent and "job_id" in value:
                lifecycle = get_data_lifecycle()
                if lifecycle is not None:
                    await lifecycle.claim(table, value["job_id"])
            return obj

        elif obj_type == "view":
            table = value["table"]
            fieldtype, columns = await _get_table_schema(table, get_ch_client())
            schema = Schema(fieldtype=fieldtype, columns=columns)
            source = Object(table=table, schema=schema)
            source._register()
            register_object(source)
            view = View(
                source=source,
                where=value.get("where"),
                limit=value.get("limit"),
                offset=value.get("offset"),
                order_by=value.get("order_by"),
                selected_fields=value.get("selected_fields"),
                renamed_columns=value.get("renamed_columns"),
            )
            register_object(view)
            if "job_id" in value:
                lifecycle = get_data_lifecycle()
                if lifecycle is not None:
                    await lifecycle.claim(value["table"], value["job_id"])
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


async def execute_task(task: Task) -> Any:
    """
    Execute a single task inside orch_context.

    Imports the callback function, deserializes kwargs inside a task_scope,
    captures output, and executes the function. Uses the active orch_context
    for ClickHouse client and distributed lifecycle refcounting.

    After execution, if the result is an Object or View, it is pinned
    under the job scope so it survives lifecycle stop().

    Args:
        task: Task to execute

    Returns:
        Any: Result of the task function

    Raises:
        Exception: Re-raises any exception from the task function
    """
    func = import_callback(task.entrypoint)

    # Set task context so expander tasks can access job_id/task_id
    set_current_task_info(task_id=task.id, job_id=task.job_id)

    with capture_task_output(task.id):
        async with task_scope(task_id=task.id, job_id=task.job_id):
            kwargs = await deserialize_task_params(task.kwargs)
            if asyncio.iscoroutinefunction(func):
                result = await func(**kwargs)
            else:
                result = func(**kwargs)

            # Pin INSIDE task_scope while lifecycle is still active
            pin_target = result.data if isinstance(result, TaskResult) else result
            if isinstance(pin_target, (Object, View)) and not pin_target.persistent:
                lifecycle = get_data_lifecycle()
                if lifecycle is not None:
                    lifecycle.pin(pin_target.table)

    return result


def _sanitize_for_json(value: Any) -> Any:
    """Replace NaN/Inf floats with None for JSON compatibility."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_json(v) for v in value]
    return value


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

    if isinstance(result, Task):
        return {"ref_type": "upstream", "task_id": result.id}

    if isinstance(result, Object):
        ref = result._serialize_ref()
        ref["job_id"] = job_id
        return ref

    # Store JSON-serializable values (dict, list, int, float, str, bool) directly
    return {"native_value": _sanitize_for_json(result)}


def _flatten_item(item: Any) -> list:
    """Recursively extract Task/Group items, including Group._tasks."""
    if isinstance(item, Task):
        return [item]

    if isinstance(item, Group):
        return [item, *item.get_tasks()]

    if isinstance(item, (list, tuple)):
        result = []
        for sub in item:
            result.extend(_flatten_item(sub))
        return result

    return []


async def register_returned_tasks(result: Any, parent_task_id: int, job_id: int) -> Any:
    """Register dynamic child tasks returned from a task function.

    Handles three return shapes:
    - None                          → no tasks, return None
    - TaskResult(data, tasks)       → register .tasks, return .data
    - list/tuple of Task/Group      → register all, return None
    - (Object, list[Group])         → register groups from [1], return [0]
    - Any other value               → pure data, return as-is

    Args:
        result: The raw return value from the task function
        parent_task_id: ID of the task that returned this result
        job_id: ID of the job these tasks belong to

    Returns:
        The data portion of the result for serialization, or None.
    """
    if result is None:
        return None

    elif isinstance(result, TaskResult):
        task_items = _flatten_item(result.tasks)
        data_result = result.data
    elif isinstance(result, (Task, Group)):
        # Job entry tasks can return a single Task or Group directly
        task_items = _flatten_item(result)
        data_result = None
    else:
        return result

    if not task_items:
        return data_result

    # Wire dependency: each returned item depends on the parent task
    for item in task_items:
        if isinstance(item, (Task, Group)):
            dep = Dependency(
                previous_id=parent_task_id,
                previous_type="task",
                next_id=item.id,
                next_type="task" if isinstance(item, Task) else "group",
            )
            item.previous_dependencies.append(dep)

    await commit_tasks(task_items, job_id)

    return data_result


_READY_TASK_SQL = """
    SELECT t.id FROM tasks t
    JOIN jobs j ON t.job_id = j.id
    WHERE t.job_id = :job_id
    AND t.status = :pending_status
    AND (t.retry_after IS NULL OR t.retry_after <= :now)
    -- Check task → task dependencies
    AND NOT EXISTS (
        SELECT 1 FROM dependencies d
        JOIN tasks prev ON d.previous_id = prev.id
        WHERE d.next_id = t.id
        AND d.next_type = 'task'
        AND d.previous_type = 'task'
        AND prev.status != :completed_status
    )
    -- Check group → task dependencies
    AND NOT EXISTS (
        SELECT 1 FROM dependencies d
        JOIN tasks prev ON prev.group_id = d.previous_id
        WHERE d.next_id = t.id
        AND d.next_type = 'task'
        AND d.previous_type = 'group'
        AND prev.status != :completed_status
    )
    -- Check task → group dependencies
    AND NOT EXISTS (
        SELECT 1 FROM dependencies d
        JOIN tasks prev ON d.previous_id = prev.id
        WHERE d.next_id = t.group_id
        AND d.next_type = 'group'
        AND d.previous_type = 'task'
        AND prev.status != :completed_status
        AND t.group_id IS NOT NULL
    )
    -- Check group → group dependencies
    AND NOT EXISTS (
        SELECT 1 FROM dependencies d
        JOIN tasks prev ON prev.group_id = d.previous_id
        WHERE d.next_id = t.group_id
        AND d.next_type = 'group'
        AND d.previous_type = 'group'
        AND prev.status != :completed_status
        AND t.group_id IS NOT NULL
    )
    ORDER BY t.id ASC
    LIMIT 1
"""


async def run_job_tasks(job: Job) -> None:
    """
    Execute all tasks for a job synchronously (test mode).

    This simulates worker behavior but runs in the current process.
    Tasks are fetched and executed one at a time, respecting dependencies.
    Supports dynamic task creation (tasks returned by other tasks).

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

    # Fetch and execute one task at a time until no more ready tasks
    while True:
        async with get_orch_session() as session:
            # Fetch next ready task (dependency-aware)
            now = datetime.utcnow()
            result = await session.execute(
                text(_READY_TASK_SQL),
                {
                    "job_id": job.id,
                    "pending_status": TaskStatus.PENDING.value,
                    "completed_status": TaskStatus.COMPLETED.value,
                    "now": now,
                },
            )
            row = result.fetchone()

            if row is None:
                break

            task_id = row[0]

            # Fetch the full task and update to RUNNING
            db_result = await session.execute(
                select(Task).where(Task.id == task_id)
            )
            task = db_result.scalar_one()

            task.status = TaskStatus.RUNNING
            task.started_at = now
            session.add(task)
            await session.commit()

            task_job_id = task.job_id

        try:
            result = await execute_task(task)

            # Register any returned Task/Group objects to the job
            data_result = await register_returned_tasks(result, task_id, task_job_id)

            # Serialize the data portion of the result
            result_ref = serialize_task_result(data_result, task_job_id)

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
