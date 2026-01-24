"""Task execution utilities for orchestration backend."""

from __future__ import annotations

import asyncio
import importlib
import inspect
from datetime import datetime
from typing import Any, Callable

from sqlmodel import select

from .context import get_orch_context_session
from .logging import capture_task_output
from .models import Job, JobStatus, Task, TaskStatus


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


def deserialize_task_params(kwargs: dict) -> dict:
    """
    Deserialize task parameters from JSON format.

    All parameters must be aaiclick Objects or Views - native Python values
    are not supported. This ensures type safety and enables distributed
    processing where data remains in ClickHouse.

    Supported object_type values:
    - object: Reference to aaiclick Object
    - view: Reference to aaiclick View

    Args:
        kwargs: Task kwargs from database (JSON-deserialized)

    Returns:
        dict: Deserialized kwargs ready for function call

    Raises:
        NotImplementedError: Object/View deserialization not yet implemented
        ValueError: Unknown object_type
    """
    if not kwargs:
        return {}

    result = {}
    for key, value in kwargs.items():
        if not isinstance(value, dict) or "object_type" not in value:
            raise ValueError(
                f"Parameter '{key}' must be an Object or View reference with 'object_type' field. "
                "Native Python values are not supported."
            )

        obj_type = value["object_type"]
        if obj_type == "object":
            raise NotImplementedError("Object parameter type not yet implemented")
        elif obj_type == "view":
            raise NotImplementedError("View parameter type not yet implemented")
        else:
            raise ValueError(f"Unknown object_type: {obj_type}. Must be 'object' or 'view'.")

    return result


async def execute_task(task: Task) -> Any:
    """
    Execute a single task.

    Imports the callback function, deserializes kwargs,
    captures output, and executes the function.

    Args:
        task: Task to execute

    Returns:
        Any: Result of the task function

    Raises:
        Exception: Re-raises any exception from the task function
    """
    func = import_callback(task.entrypoint)
    kwargs = deserialize_task_params(task.kwargs)

    with capture_task_output(task.id):
        if asyncio.iscoroutinefunction(func):
            result = await func(**kwargs)
        else:
            result = func(**kwargs)

    return result


async def run_job_tasks(job: Job) -> None:
    """
    Execute all tasks for a job synchronously (test mode).

    This simulates worker behavior but runs in the current process.
    Tasks are executed in order of creation (snowflake ID).

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

    async with get_orch_context_session() as session:
        # Get all pending tasks for this job
        result = await session.execute(
            select(Task).where(Task.job_id == job.id, Task.status == TaskStatus.PENDING).order_by(Task.id)
        )
        tasks = list(result.scalars().all())

    job_failed = False
    error_msg = None

    for task in tasks:
        async with get_orch_context_session() as session:
            # Reload task to get fresh state
            result = await session.execute(select(Task).where(Task.id == task.id))
            task = result.scalar_one()

            # Update task to RUNNING
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.utcnow()
            session.add(task)
            await session.commit()

        try:
            result = await execute_task(task)

            async with get_orch_context_session() as session:
                # Reload and update task to COMPLETED
                db_result = await session.execute(select(Task).where(Task.id == task.id))
                task = db_result.scalar_one()

                task.status = TaskStatus.COMPLETED
                task.completed_at = datetime.utcnow()

                # Convert result to Object and store reference
                if result is not None:
                    from aaiclick import create_object_from_value

                    obj = await create_object_from_value(result)
                    task.result = {"object_type": "object", "table_id": obj.table_id}

                session.add(task)
                await session.commit()

        except Exception as e:
            job_failed = True
            error_msg = str(e)

            async with get_orch_context_session() as session:
                # Reload and update task to FAILED
                db_result = await session.execute(select(Task).where(Task.id == task.id))
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
