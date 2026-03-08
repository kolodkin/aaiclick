"""Tests for orchestration execution and Job.test() functionality."""

import asyncio
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from aaiclick.data.data_context import data_context
from aaiclick.data.object import Object, View
from aaiclick.orchestration.context import get_orch_session
from aaiclick.orchestration.debug_execution import ajob_test
from aaiclick.orchestration.execution import (
    _extract_task_items,
    deserialize_task_params,
    execute_task,
    import_callback,
    register_returned_tasks,
    run_job_tasks,
    serialize_task_result,
)
from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.fixtures.sample_tasks import (
    chain_pipeline,
    dynamic_pipeline,
)
from aaiclick.orchestration.logging import capture_task_output, get_logs_dir
from aaiclick.orchestration.models import Dependency, Group, JobStatus, Task, TaskStatus


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
    result = await deserialize_task_params({})
    assert result == {}


async def test_deserialize_task_params_native_python(orch_ctx):
    """Test that native Python values are passed through unchanged."""
    kwargs = {"x": 5, "y": 10, "name": "test", "items": [1, 2, 3]}

    async with data_context():
        result = await deserialize_task_params(kwargs)
        assert result["x"] == 5
        assert result["y"] == 10
        assert result["name"] == "test"
        assert result["items"] == [1, 2, 3]


async def test_deserialize_task_params_rejects_unknown_type(orch_ctx):
    """Test that unknown object_type is rejected."""
    kwargs = {"x": {"object_type": "unknown", "value": 5}}

    async with data_context():
        with pytest.raises(ValueError, match="Unknown object_type"):
            await deserialize_task_params(kwargs)


async def test_deserialize_task_params_object(orch_ctx):
    """Test deserializing an Object parameter."""
    kwargs = {"data": {"object_type": "object", "table": "t123"}}

    async with data_context():
        result = await deserialize_task_params(kwargs)
        assert "data" in result
        assert isinstance(result["data"], Object)
        assert result["data"].table == "t123"


async def test_deserialize_task_params_view(orch_ctx):
    """Test deserializing a View parameter with constraints."""
    kwargs = {
        "data": {
            "object_type": "view",
            "table": "t456",
            "where": "value > 10",
            "limit": 100,
            "offset": 50,
            "order_by": "aai_id ASC",
        }
    }

    async with data_context():
        result = await deserialize_task_params(kwargs)
        assert "data" in result
        assert isinstance(result["data"], View)
        assert result["data"].table == "t456"
        assert result["data"]._build_where() == "(value > 10)"
        assert result["data"].limit == 100
        assert result["data"].offset == 50
        assert result["data"].order_by == "aai_id ASC"


async def test_serialize_task_result_none(orch_ctx):
    """Test serializing None result."""
    assert serialize_task_result(None, job_id=2) is None


async def test_serialize_task_result_object(orch_ctx):
    """Test serializing an Object result."""
    obj = Object(table="t789")
    result = serialize_task_result(obj, job_id=200)
    assert result == {
        "object_type": "object",
        "table": "t789",
        "job_id": 200,
    }


async def test_serialize_task_result_non_object(orch_ctx):
    """Test serializing a non-Object/View result wraps in native_value."""
    assert serialize_task_result(42, job_id=2) == {"native_value": 42}
    assert serialize_task_result("hello", job_id=2) == {"native_value": "hello"}


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
        async with get_orch_session() as session:
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
        async with get_orch_session() as session:
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
        async with get_orch_session() as session:
            result = await session.execute(select(Task).where(Task.job_id == job.id))
            task = result.scalar_one()

        log_path = os.path.join(tmpdir, f"{task.id}.log")
        assert os.path.exists(log_path)

        with open(log_path) as f:
            content = f.read()
            assert "This is stdout" in content
            assert "Error message" in content


# job_test() tests


async def test_job_test_simple(orch_ctx, monkeypatch):
    """Test job_test() executes a simple task synchronously.

    Note: job_test() uses asyncio.run() internally, which is tested
    via ajob_test() in the async context to avoid nested event loops.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        # Create the job
        job = await create_job("test_sync", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

        # Test execution via the async helper (same code path as job_test())
        await ajob_test(job)

        assert job.status == JobStatus.COMPLETED


# _extract_task_items tests


def test_extract_task_items_single_task(orch_ctx):
    """Single Task return is extracted."""
    t = create_task("mod.func")
    items, data = _extract_task_items(t)
    assert items == [t]
    assert data is None


def test_extract_task_items_single_group(orch_ctx):
    """Single Group return is extracted."""
    from aaiclick.snowflake_id import get_snowflake_id

    g = Group(id=get_snowflake_id(), name="g1")
    items, data = _extract_task_items(g)
    assert items == [g]
    assert data is None


def test_extract_task_items_list_of_tasks(orch_ctx):
    """List of Tasks is extracted."""
    t1 = create_task("mod.func1")
    t2 = create_task("mod.func2")
    items, data = _extract_task_items([t1, t2])
    assert len(items) == 2
    assert data is None


def test_extract_task_items_native_value(orch_ctx):
    """Native values are not extracted."""
    items, data = _extract_task_items(42)
    assert items == []
    assert data == 42


def test_extract_task_items_none(orch_ctx):
    """None is treated as pure data."""
    items, data = _extract_task_items(None)
    assert items == []
    assert data is None


def test_extract_task_items_list_of_non_tasks(orch_ctx):
    """List of non-task values is not extracted."""
    items, data = _extract_task_items([1, 2, 3])
    assert items == []
    assert data == [1, 2, 3]


def test_extract_task_items_mixed_task_and_group(orch_ctx):
    """Mixed list of Task and Group is extracted."""
    from aaiclick.snowflake_id import get_snowflake_id

    t = create_task("mod.func")
    g = Group(id=get_snowflake_id(), name="g1")
    items, data = _extract_task_items([t, g])
    assert len(items) == 2
    assert t in items
    assert g in items
    assert data is None


def test_extract_task_items_mixed_with_explicit_dependency(orch_ctx):
    """Task with explicit >> dependency is extracted with deps preserved."""
    from aaiclick.snowflake_id import get_snowflake_id

    t1 = create_task("mod.step1")
    t2 = create_task("mod.step2")
    g = Group(id=get_snowflake_id(), name="g1")
    t2 >> t1  # explicit dependency: t1 depends on t2

    items, data = _extract_task_items([t1, g])
    assert t1 in items
    assert g in items

    # Explicit dependency is preserved
    dep_ids = {d.previous_id for d in t1.previous_dependencies}
    assert t2.id in dep_ids


# register_returned_tasks tests


async def test_register_returned_tasks_no_tasks(orch_ctx):
    """Non-task return values pass through unchanged."""
    result = await register_returned_tasks(42, parent_task_id=1, job_id=1)
    assert result == 42


async def test_register_returned_tasks_with_task(orch_ctx):
    """Returned Task is registered with dependency on parent."""
    job = await create_job("reg_test", "mod.func")
    parent = create_task("mod.parent")
    parent.job_id = job.id

    child = create_task("mod.child")

    data_result = await register_returned_tasks(child, parent_task_id=parent.id, job_id=job.id)
    assert data_result is None

    # Verify child was committed with dependency on parent
    async with get_orch_session() as session:
        result = await session.execute(select(Task).where(Task.id == child.id))
        db_child = result.scalar_one()
        assert db_child.job_id == job.id

        result = await session.execute(
            select(Dependency).where(
                Dependency.next_id == child.id,
                Dependency.previous_id == parent.id,
            )
        )
        dep = result.scalar_one()
        assert dep.previous_type == "task"
        assert dep.next_type == "task"


async def test_register_returned_tasks_list(orch_ctx):
    """List of Tasks is registered with dependencies on parent."""
    job = await create_job("reg_list_test", "mod.func")
    parent = create_task("mod.parent")
    parent.job_id = job.id

    c1 = create_task("mod.child1")
    c2 = create_task("mod.child2")

    data_result = await register_returned_tasks([c1, c2], parent_task_id=parent.id, job_id=job.id)
    assert data_result is None

    async with get_orch_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id, Task.entrypoint.in_(["mod.child1", "mod.child2"]))
        )
        children = result.scalars().all()
        assert len(children) == 2


# Dynamic pipeline integration tests


async def test_dynamic_pipeline_creates_entry_task(orch_ctx, monkeypatch):
    """@job creates a Job with an entry point task."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        job = await dynamic_pipeline()

        assert job.status == JobStatus.PENDING

        # Verify entry point task was created
        async with get_orch_session() as session:
            result = await session.execute(
                select(Task).where(Task.job_id == job.id)
            )
            tasks = list(result.scalars().all())
            assert len(tasks) == 1
            assert tasks[0].name == "dynamic_pipeline"
            assert "sample_tasks.dynamic_pipeline" in tasks[0].entrypoint


async def test_dynamic_pipeline_execution(orch_ctx, monkeypatch):
    """@job entry point runs and its returned tasks get registered and executed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        job = await dynamic_pipeline()
        await run_job_tasks(job)

        assert job.status == JobStatus.COMPLETED

        # Entry point + 2 child tasks = 3 tasks total
        async with get_orch_session() as session:
            result = await session.execute(
                select(Task).where(Task.job_id == job.id).order_by(Task.id)
            )
            tasks = list(result.scalars().all())
            assert len(tasks) == 3

            # All tasks should be completed
            for t in tasks:
                assert t.status == TaskStatus.COMPLETED

            # Child tasks should have results
            child_tasks = [t for t in tasks if t.name != "dynamic_pipeline"]
            assert len(child_tasks) == 2
            for ct in child_tasks:
                assert ct.result is not None


async def test_chain_pipeline_execution(orch_ctx, monkeypatch):
    """Chained dynamic creation: task A returns task B, task B returns task C."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        job = await chain_pipeline()
        await run_job_tasks(job)

        assert job.status == JobStatus.COMPLETED

        # chain_pipeline -> step_one -> step_two = 3 tasks
        async with get_orch_session() as session:
            result = await session.execute(
                select(Task).where(Task.job_id == job.id).order_by(Task.id)
            )
            tasks = list(result.scalars().all())
            assert len(tasks) == 3

            for t in tasks:
                assert t.status == TaskStatus.COMPLETED
