"""Factory functions for creating orchestration objects."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Union

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aaiclick.snowflake import get_snowflake_id

from .models import Job, JobStatus, Task, TaskStatus


# Lazy-initialized async engine
_engine: list[object] = [None]


async def get_async_engine():
    """Get or create the async SQLAlchemy engine."""
    if _engine[0] is None:
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        user = os.getenv("POSTGRES_USER", "aaiclick")
        password = os.getenv("POSTGRES_PASSWORD", "secret")
        database = os.getenv("POSTGRES_DB", "aaiclick")

        database_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"
        _engine[0] = create_async_engine(database_url, echo=False)

    return _engine[0]


async def reset_async_engine():
    """Reset the async SQLAlchemy engine.

    Disposes the existing engine and sets it to None, forcing
    a new engine to be created on next get_async_engine() call.

    Used primarily for test cleanup to ensure test isolation.
    """
    if _engine[0] is not None:
        await _engine[0].dispose()
        _engine[0] = None


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

    # Commit to database using SQLAlchemy ORM
    engine = await get_async_engine()
    async with AsyncSession(engine, expire_on_commit=False) as session:
        # Add job and task using ORM
        session.add(job)
        session.add(task)

        # Commit transaction
        await session.commit()

    return job
