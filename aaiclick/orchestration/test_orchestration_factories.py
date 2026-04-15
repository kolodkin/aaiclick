"""Tests for orchestration factory functions."""

from datetime import datetime

from sqlalchemy import select

from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.jobs import get_task
from aaiclick.orchestration.result import data_list
from aaiclick.orchestration.models import Job, JobStatus, Task, TaskStatus
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.result import data_list


async def test_create_task_unique_ids(orch_ctx):
    """Test that each task gets a unique snowflake ID."""
    task1 = create_task("mymodule.task1")
    task2 = create_task("mymodule.task2")

    assert task1.id != task2.id
    assert task1.id > 0
    assert task2.id > 0


async def test_create_job_with_string(orch_ctx):
    """Test job creation with string callback."""
    job = await create_job("test_job", "mymodule.task1")

    assert job.id > 0
    assert job.name == "test_job"
    assert job.status == JobStatus.PENDING
    assert isinstance(job.created_at, datetime)
    assert job.started_at is None
    assert job.completed_at is None
    assert job.error is None

    # Verify job was persisted to database using ORM
    async with get_sql_session() as session:
        # Query for the job
        result = await session.execute(select(Job).where(Job.id == job.id))
        db_job = result.scalar_one_or_none()
        assert db_job is not None
        assert db_job.name == "test_job"
        assert db_job.status == JobStatus.PENDING

        # Verify task was created and persisted
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        tasks = result.scalars().all()
        assert len(tasks) == 1
        assert tasks[0].entrypoint == "mymodule.task1"
        assert tasks[0].status == TaskStatus.PENDING
        assert tasks[0].kwargs == {}


async def test_create_job_with_task(orch_ctx):
    """Test job creation with Task object."""
    task = create_task("mymodule.task2", {"param": "value"})
    job = await create_job("test_job_2", task)

    assert job.id > 0
    assert job.name == "test_job_2"

    # Verify task has job_id assigned via the public query helper
    db_task = await get_task(task.id)
    assert db_task is not None
    assert db_task.job_id == job.id
    assert db_task.entrypoint == "mymodule.task2"
    assert db_task.kwargs == {"param": "value"}


async def test_create_job_unique_ids(orch_ctx):
    """Test that each job gets a unique snowflake ID."""
    job1 = await create_job("job1", "mymodule.task1")
    job2 = await create_job("job2", "mymodule.task2")

    assert job1.id != job2.id
    assert job1.id > 0
    assert job2.id > 0


async def test_job_task_relationship(orch_ctx):
    """Test that job and task have correct relationship."""
    job = await create_job("relationship_test", "mymodule.task3")

    # Verify job and task relationship using ORM
    async with get_sql_session() as session:
        # Get job
        result = await session.execute(select(Job).where(Job.id == job.id))
        db_job = result.scalar_one_or_none()
        assert db_job is not None

        # Get tasks for this job
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        tasks = result.scalars().all()
        assert len(tasks) == 1
        assert tasks[0].job_id == job.id


def test_data_list_single(orch_ctx):
    """Test data_list() with a single item returns TaskResult with that item as data."""
    result = data_list("only")
    assert result.data == "only"
    assert result.tasks == []


def test_data_list_multiple(orch_ctx):
    """Test data_list() with multiple items returns TaskResult with a list."""
    result = data_list("a", "b", "c")
    assert result.data == ["a", "b", "c"]
    assert result.tasks == []
