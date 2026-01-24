"""Tests for orchestration execution and Job.test() functionality."""

import os
import sys
import tempfile

from sqlalchemy import select

from aaiclick.orchestration import (
    Job,
    JobStatus,
    Task,
    TaskStatus,
    create_job,
    create_task,
    execute_task,
    get_logs_dir,
    run_job_tasks,
)
from aaiclick.orchestration.context import get_orch_context_session
from aaiclick.orchestration.execution import deserialize_task_params, import_callback
from aaiclick.orchestration.logging import capture_task_output


class TestLogging:
    """Tests for logging module."""

    async def test_get_logs_dir_default(self, orch_ctx, monkeypatch):
        """Test get_logs_dir returns OS-dependent default."""
        monkeypatch.delenv("AAICLICK_LOG_DIR", raising=False)

        log_dir = get_logs_dir()

        if sys.platform == "darwin":
            assert log_dir == os.path.expanduser("~/.aaiclick/logs")
        else:
            assert log_dir == "/var/log/aaiclick"

    async def test_get_logs_dir_custom(self, orch_ctx, monkeypatch):
        """Test get_logs_dir with custom directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_dir = os.path.join(tmpdir, "custom_logs")
            monkeypatch.setenv("AAICLICK_LOG_DIR", custom_dir)

            log_dir = get_logs_dir()

            assert log_dir == custom_dir
            assert os.path.exists(custom_dir)

    async def test_capture_task_output_stdout(self, orch_ctx, monkeypatch):
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

    async def test_capture_task_output_stderr(self, orch_ctx, monkeypatch):
        """Test that stderr is captured to log file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

            task_id = 12346

            with capture_task_output(task_id) as log_path:
                print("Error message", file=sys.stderr)

            with open(log_path) as f:
                content = f.read()
                assert "Error message" in content


class TestExecution:
    """Tests for execution module."""

    async def test_import_callback_basic(self, orch_ctx):
        """Test importing a callback function."""
        func = import_callback("aaiclick.orchestration.fixtures.sample_tasks.simple_task")

        assert callable(func)
        func()  # Should not raise

    async def test_import_callback_async(self, orch_ctx):
        """Test importing an async callback function."""
        import asyncio

        func = import_callback("aaiclick.orchestration.fixtures.sample_tasks.async_task")

        assert callable(func)
        assert asyncio.iscoroutinefunction(func)
        await func()  # Should not raise

    async def test_import_callback_invalid_format(self, orch_ctx):
        """Test that invalid entrypoint format raises error."""
        import pytest

        with pytest.raises(ValueError, match="Invalid entrypoint format"):
            import_callback("no_dot_in_name")

    async def test_deserialize_task_params_empty(self, orch_ctx):
        """Test deserializing empty parameters."""
        result = deserialize_task_params({})
        assert result == {}

    async def test_deserialize_task_params_rejects_native_python(self, orch_ctx):
        """Test that native Python values are rejected."""
        import pytest

        kwargs = {"x": 5, "y": 10}

        with pytest.raises(ValueError, match="must be an Object or View"):
            deserialize_task_params(kwargs)

    async def test_deserialize_task_params_rejects_unknown_type(self, orch_ctx):
        """Test that unknown object_type is rejected."""
        import pytest

        kwargs = {"x": {"object_type": "unknown", "value": 5}}

        with pytest.raises(ValueError, match="Unknown object_type"):
            deserialize_task_params(kwargs)

    async def test_deserialize_task_params_object_not_implemented(self, orch_ctx):
        """Test that object type raises NotImplementedError (not yet implemented)."""
        import pytest

        kwargs = {"data": {"object_type": "object", "table_id": "t123"}}

        with pytest.raises(NotImplementedError):
            deserialize_task_params(kwargs)

    async def test_deserialize_task_params_view_not_implemented(self, orch_ctx):
        """Test that view type raises NotImplementedError (not yet implemented)."""
        import pytest

        kwargs = {"data": {"object_type": "view", "table_id": "t123", "limit": 100}}

        with pytest.raises(NotImplementedError):
            deserialize_task_params(kwargs)

    async def test_execute_task_sync_function(self, orch_ctx, monkeypatch):
        """Test executing a sync task function with no parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

            task = create_task("aaiclick.orchestration.fixtures.sample_tasks.simple_task")
            task.job_id = 1  # Set a dummy job_id

            await execute_task(task)  # Should not raise

    async def test_execute_task_async_function(self, orch_ctx, monkeypatch):
        """Test executing an async task function with no parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

            task = create_task("aaiclick.orchestration.fixtures.sample_tasks.async_task")
            task.job_id = 1

            await execute_task(task)  # Should not raise


class TestRunJobTasks:
    """Tests for run_job_tasks function."""

    async def test_run_job_tasks_single_task(self, orch_ctx, monkeypatch):
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

    async def test_run_job_tasks_failing_task(self, orch_ctx, monkeypatch):
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

    async def test_run_job_tasks_creates_log_file(self, orch_ctx, monkeypatch):
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


class TestJobTest:
    """Tests for Job.test() method."""

    def test_job_test_simple(self, monkeypatch):
        """Test Job.test() executes a simple task synchronously."""
        import asyncio

        with tempfile.TemporaryDirectory() as tmpdir:
            monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

            async def create_and_test():
                from aaiclick.orchestration.context import OrchContext

                async with OrchContext():
                    job = await create_job("test_sync", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")
                    return job

            job = asyncio.run(create_and_test())
            job.test()

            assert job.status == JobStatus.COMPLETED
