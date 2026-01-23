"""Factory functions for creating orchestration objects."""

from __future__ import annotations

from datetime import datetime
from typing import Union

from aaiclick.snowflake import get_snowflake_id

from .context import get_orch_context_session
from .models import Job, JobStatus, Task, TaskStatus


def create_task(callback: str, kwargs: dict = None) -> Task:
    """Create a Task object (not committed to database).

    Args:
        callback: Entrypoint as string (e.g., "mymodule.task1")
        kwargs: Keyword arguments for the task function (default: empty dict)

    Returns:
        Task object with generated snowflake ID

    Example:
        task = create_task("mymodule.task1", {"param": "value"})
    """
    task_id = get_snowflake_id()

    return Task(
        id=task_id,
        entrypoint=callback,
        kwargs=kwargs or {},
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow(),
    )


async def create_job(name: str, entry: Union[str, Task]) -> Job:
    """Create a Job and commit it to the database.

    Args:
        name: Job name
        entry: Either a callback string (e.g., "mymodule.task1") or a Task object

    Returns:
        Job object with id populated after database commit

    Example:
        # Using callback string
        job = await create_job("my_job", "mymodule.task1")

        # Using Task object
        task = create_task("mymodule.task1", {"param": "value"})
        job = await create_job("my_job", task)
    """
    # Generate job ID
    job_id = get_snowflake_id()

    # Create Job object
    job = Job(
        id=job_id,
        name=name,
        status=JobStatus.PENDING,
        created_at=datetime.utcnow(),
    )

    # Create task from entry if it's a string
    if isinstance(entry, str):
        task = create_task(entry)
    else:
        task = entry

    # Set task's job_id
    task.job_id = job_id

    # Commit to database using OrchContext session
    async with get_orch_context_session() as session:
        # Add job and task using ORM
        session.add(job)
        session.add(task)

        # Commit transaction
        await session.commit()

    return job
