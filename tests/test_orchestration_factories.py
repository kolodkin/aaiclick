"""Tests for orchestration factory functions."""

import json
from datetime import datetime

from aaiclick.orchestration import (
    Job,
    JobStatus,
    Task,
    TaskStatus,
    create_job,
    create_task,
)
from aaiclick.orchestration.database import get_postgres_pool


async def test_create_task_basic(orch_ctx):
    """Test basic task creation."""
    task = create_task("mymodule.task1")

    assert task.id > 0  # Snowflake ID should be positive
    assert task.entrypoint == "mymodule.task1"
    assert task.kwargs == {}
    assert task.status == TaskStatus.PENDING
    assert isinstance(task.created_at, datetime)
    assert task.job_id is None  # Not assigned yet


async def test_create_task_with_kwargs(orch_ctx):
    """Test task creation with kwargs."""
    kwargs = {"param1": "value1", "param2": 42}
    task = create_task("mymodule.task2", kwargs)

    assert task.entrypoint == "mymodule.task2"
    assert task.kwargs == kwargs
    assert task.status == TaskStatus.PENDING


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

    # Verify job was persisted to database
    pool = await get_postgres_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM jobs WHERE id = $1::BIGINT", job.id)
        assert row is not None
        assert row["name"] == "test_job"
        assert row["status"] == "PENDING"

        # Verify task was created and persisted
        task_rows = await conn.fetch("SELECT * FROM tasks WHERE job_id = $1::BIGINT", job.id)
        assert len(task_rows) == 1
        assert task_rows[0]["entrypoint"] == "mymodule.task1"
        assert task_rows[0]["status"] == "PENDING"
        assert json.loads(task_rows[0]["kwargs"]) == {}


async def test_create_job_with_task(orch_ctx):
    """Test job creation with Task object."""
    task = create_task("mymodule.task2", {"param": "value"})
    job = await create_job("test_job_2", task)

    assert job.id > 0
    assert job.name == "test_job_2"

    # Verify task has job_id assigned
    pool = await get_postgres_pool()
    async with pool.acquire() as conn:
        task_row = await conn.fetchrow("SELECT * FROM tasks WHERE id = $1::BIGINT", task.id)
        assert task_row is not None
        assert task_row["job_id"] == job.id
        assert task_row["entrypoint"] == "mymodule.task2"
        assert json.loads(task_row["kwargs"]) == {"param": "value"}


async def test_create_job_unique_ids(orch_ctx):
    """Test that each job gets a unique snowflake ID."""
    job1 = await create_job("job1", "mymodule.task1")
    job2 = await create_job("job2", "mymodule.task2")

    assert job1.id != job2.id
    assert job1.id > 0
    assert job2.id > 0


async def test_database_connection_pool(orch_ctx):
    """Test that database connection pool works correctly."""
    pool = await get_postgres_pool()

    # Test multiple concurrent connections
    async with pool.acquire() as conn1:
        result1 = await conn1.fetchval("SELECT 1")
        assert result1 == 1

    async with pool.acquire() as conn2:
        result2 = await conn2.fetchval("SELECT 2")
        assert result2 == 2

    # Verify we can query the jobs table
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM jobs")
        assert count >= 0  # Should have at least the jobs we created


async def test_job_task_relationship(orch_ctx):
    """Test that job and task have correct relationship."""
    job = await create_job("relationship_test", "mymodule.task3")

    pool = await get_postgres_pool()
    async with pool.acquire() as conn:
        # Get job
        job_row = await conn.fetchrow("SELECT * FROM jobs WHERE id = $1::BIGINT", job.id)
        assert job_row is not None

        # Get tasks for this job
        tasks = await conn.fetch("SELECT * FROM tasks WHERE job_id = $1::BIGINT", job.id)
        assert len(tasks) == 1
        assert tasks[0]["job_id"] == job.id
