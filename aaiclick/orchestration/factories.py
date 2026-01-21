"""
aaiclick.orchestration.factories - Factory functions for creating jobs and tasks.

This module provides factory functions for creating Job and Task instances,
with automatic snowflake ID generation and database persistence.
"""

from __future__ import annotations

from typing import Any, Dict, Union

from aaiclick.snowflake import get_snowflake_id

from .database import get_postgres_connection
from .models import Job, JobStatus, Task, TaskStatus


def create_task(callback: str, kwargs: Dict[str, Any] = None) -> Task:
    """
    Create a new Task with automatic snowflake ID generation.

    This function creates a Task object but does NOT commit it to the database.
    Tasks should be committed via OrchContext.apply() or as part of create_job().

    Args:
        callback: Python callback string (e.g., "mymodule.task1")
        kwargs: Optional dictionary of keyword arguments for the task

    Returns:
        Task: New Task instance (not yet persisted)

    Example:
        >>> task = create_task("mymodule.process_data", {"limit": 100})
        >>> # Task is not yet in database - commit via context.apply()
    """
    task_id = get_snowflake_id()

    return Task(
        id=task_id,
        entrypoint=callback,
        kwargs=kwargs or {},
        status=TaskStatus.PENDING,
    )


async def create_job(name: str, entry: Union[str, Task]) -> Job:
    """
    Create a new Job with entry point task and persist to PostgreSQL.

    This function:
    1. Generates snowflake ID for the job
    2. Creates Job record
    3. Creates or updates entry Task with job_id
    4. Commits both to PostgreSQL in a transaction
    5. Returns Job with populated id

    Args:
        name: Human-readable job name
        entry: Either a callback string (e.g., "mymodule.task1") or a Task object

    Returns:
        Job: New Job instance with id populated

    Example:
        >>> # Using callback string
        >>> job = await create_job("my_job", "mymodule.task1")
        >>> print(f"Job {job.id} created")
        >>>
        >>> # Using Task object
        >>> task = create_task("mymodule.task1", {"param": "value"})
        >>> job = await create_job("my_job", task)
    """
    # Generate snowflake ID for job
    job_id = get_snowflake_id()

    # Create Job instance
    job = Job(
        id=job_id,
        name=name,
        status=JobStatus.PENDING,
    )

    # Create or use existing Task
    if isinstance(entry, str):
        # Create task from callback string
        task = create_task(entry)
    else:
        # Use provided Task
        task = entry

    # Set job_id on task
    task.job_id = job_id

    # Commit to database in transaction
    async with get_postgres_connection() as conn:
        async with conn.transaction():
            # Insert Job
            await conn.execute(
                """
                INSERT INTO jobs (id, name, status, created_at)
                VALUES ($1, $2, $3, NOW())
                """,
                job.id,
                job.name,
                job.status.value,
            )

            # Insert Task
            await conn.execute(
                """
                INSERT INTO tasks (
                    id, job_id, entrypoint, kwargs, status, created_at
                ) VALUES ($1, $2, $3, $4, $5, NOW())
                """,
                task.id,
                task.job_id,
                task.entrypoint,
                task.kwargs,
                task.status.value,
            )

    return job
