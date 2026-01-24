"""Tests for orchestration execution and Job.test() functionality."""

import asyncio
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from aaiclick.orchestration import (
    JobStatus,
    Task,
    TaskStatus,
    create_job,
    create_task,
    execute_task,
    get_logs_dir,
    run_job_tasks,
    run_job_test_async,
)
from aaiclick.orchestration.context import get_orch_context_session
from aaiclick.orchestration.execution import deserialize_task_params, import_callback
from aaiclick.orchestration.logging import capture_task_output


# Logging tests


async def test_get_logs_dir_default(orch_ctx, monkeypatch):
    """Test get_logs_dir returns OS-dependent default."""
    monkeypatch.delenv("AAICLICK_LOG_DIR", raising=False)

    # Mock Path.mkdir to avoid permission issues in CI
    mock_mkdir = MagicMock()
    monkeypatch.setattr(Path, "mkdir", mock_mkdir)

    log_dir = get_logs_dir()

    if sys.platform == "darwin":
        assert log_dir == os.path.expanduser("~/.aaiclick/logs")
    else:
        assert log_dir == "/var/log/aaiclick"

    # Verify mkdir was called
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)


async def test_get_logs_dir_custom(orch_ctx, monkeypatch):
    """Test get_logs_dir with custom directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        custom_dir = os.path.join(tmpdir, "custom_logs")
        monkeypatch.setenv("AAICLICK_LOG_DIR", custom_dir)

        log_dir = get_logs_dir()

        assert log_dir == custom_dir
        assert os.path.exists(custom_dir)


async def test_capture_task_output_stdout(orch_ctx, monkeypatch):
    """Test that stdout is captured to log file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        task_id = 12345

        with capture_task_output(task_id) as log_path:
            print("Hello, world!")

        assert os.path.exists(log_path)
        with open(log_path) as f:
            content = f.read()
            assert "Hello, world!" in content


async def test_capture_task_output_stderr(orch_ctx, monkeypatch):
    """Test that stderr is captured to log file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        task_id = 12346

        with capture_task_output(task_id) as log_path:
            print("Error message", file=sys.stderr)

        with open(log_path) as f:
            content = f.read()
            assert "Error message" in content


# Execution tests


async def test_import_callback_basic(orch_ctx):
    """Test importing a callback function."""
    func = import_callback("aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    assert callable(func)
    func()  # Should not raise


async def test_import_callback_async(orch_ctx):
    """Test importing an async callback function."""
    func = import_callback("aaiclick.orchestration.fixtures.sample_tasks.async_task")

    assert callable(func)
    assert asyncio.iscoroutinefunction(func)
    await func()  # Should not raise


async def test_import_callback_invalid_format(orch_ctx):
    """Test that invalid entrypoint format raises error."""
    with pytest.raises(ValueError, match="Invalid entrypoint format"):
        import_callback("no_dot_in_name")


async def test_deserialize_task_params_empty(orch_ctx):
    """Test deserializing empty parameters."""
    result = deserialize_task_params({})
    assert result == {}


async def test_deserialize_task_params_rejects_native_python(orch_ctx):
    """Test that native Python values are rejected."""
    kwargs = {"x": 5, "y": 10}

    with pytest.raises(ValueError, match="must be an Object or View"):
        deserialize_task_params(kwargs)


async def test_deserialize_task_params_rejects_unknown_type(orch_ctx):
    """Test that unknown object_type is rejected."""
    kwargs = {"x": {"object_type": "unknown", "value": 5}}

    with pytest.raises(ValueError, match="Unknown object_type"):
        deserialize_task_params(kwargs)


async def test_deserialize_task_params_object_not_implemented(orch_ctx):
    """Test that object type raises NotImplementedError (not yet implemented)."""
    kwargs = {"data": {"object_type": "object", "table_id": "t123"}}

    with pytest.raises(NotImplementedError):
        deserialize_task_params(kwargs)


async def test_deserialize_task_params_view_not_implemented(orch_ctx):
    """Test that view type raises NotImplementedError (not yet implemented)."""
    kwargs = {"data": {"object_type": "view", "table_id": "t123", "limit": 100}}

    with pytest.raises(NotImplementedError):
        deserialize_task_params(kwargs)


async def test_execute_task_sync_function(orch_ctx, monkeypatch):
    """Test executing a sync task function with no parameters."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        task = create_task("aaiclick.orchestration.fixtures.sample_tasks.simple_task")
        task.job_id = 1  # Set a dummy job_id

        await execute_task(task)  # Should not raise


async def test_execute_task_async_function(orch_ctx, monkeypatch):
    """Test executing an async task function with no parameters."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        task = create_task("aaiclick.orchestration.fixtures.sample_tasks.async_task")
        task.job_id = 1

        await execute_task(task)  # Should not raise


# run_job_tasks tests


async def test_run_job_tasks_single_task(orch_ctx, monkeypatch):
    """Test running a job with a single task."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        job = await create_job("test_job", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

        await run_job_tasks(job)

        assert job.status == JobStatus.COMPLETED
        assert job.completed_at is not None

        # Verify task completed in database
        async with get_orch_context_session() as session:
            result = await session.execute(select(Task).where(Task.job_id == job.id))
            tasks = list(result.scalars().all())
            assert len(tasks) == 1
            assert tasks[0].status == TaskStatus.COMPLETED


async def test_run_job_tasks_failing_task(orch_ctx, monkeypatch):
    """Test running a job with a failing task."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        job = await create_job("test_job_fail", "aaiclick.orchestration.fixtures.sample_tasks.failing_task")

        await run_job_tasks(job)

        assert job.status == JobStatus.FAILED
        assert job.error is not None
        assert "intentionally" in job.error

        # Verify task failed in database
        async with get_orch_context_session() as session:
            result = await session.execute(select(Task).where(Task.job_id == job.id))
            tasks = list(result.scalars().all())
            assert len(tasks) == 1
            assert tasks[0].status == TaskStatus.FAILED
            assert tasks[0].error is not None


async def test_run_job_tasks_creates_log_file(orch_ctx, monkeypatch):
    """Test that task execution creates log file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        job = await create_job("test_job_log", "aaiclick.orchestration.fixtures.sample_tasks.task_with_output")

        await run_job_tasks(job)

        # Get the task to find log file
        async with get_orch_context_session() as session:
            result = await session.execute(select(Task).where(Task.job_id == job.id))
            task = result.scalar_one()

        log_path = os.path.join(tmpdir, f"{task.id}.log")
        assert os.path.exists(log_path)

        with open(log_path) as f:
            content = f.read()
            assert "This is stdout" in content
            assert "Error message" in content


# run_job_test() tests


async def test_run_job_test_simple(orch_ctx, monkeypatch):
    """Test run_job_test() executes a simple task synchronously.

    Note: run_job_test() uses asyncio.run() internally, which is tested
    via run_job_test_async() in the async context to avoid nested event loops.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        # Create the job
        job = await create_job("test_sync", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

        # Test execution via the async helper (same code path as run_job_test())
        await run_job_test_async(job)

        assert job.status == JobStatus.COMPLETED
