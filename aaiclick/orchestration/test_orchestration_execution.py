"""Tests for orchestration execution and Job.test() functionality."""

import asyncio
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel
from sqlalchemy import select

from aaiclick.data.object import Object, View
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.debug_execution import ajob_test
from aaiclick.orchestration.execution import (
    TaskResult,
    deserialize_task_params,
    execute_task,
    import_callback,
    register_returned_tasks,
    run_job_tasks,
    serialize_task_result,
)
from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.examples.orchestration_dynamic import (
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

    result = await deserialize_task_params(kwargs)
    assert result["x"] == 5
    assert result["y"] == 10
    assert result["name"] == "test"
    assert result["items"] == [1, 2, 3]


async def test_deserialize_task_params_rejects_unknown_type(orch_ctx):
    """Test that unknown object_type is rejected."""
    kwargs = {"x": {"object_type": "unknown", "value": 5}}

    with pytest.raises(ValueError, match="Unknown object_type"):
        await deserialize_task_params(kwargs)


async def test_deserialize_task_params_object(orch_ctx):
    """Test deserializing an Object parameter."""
    kwargs = {"data": {"object_type": "object", "table": "t123"}}

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


class _SampleModel(BaseModel):
    name: str
    count: int
    ratio: float | None = None


async def test_serialize_task_result_pydantic_model(orch_ctx):
    """Pydantic BaseModel results are serialized with pydantic_type + data keys."""
    model = _SampleModel(name="test", count=42, ratio=0.5)
    result = serialize_task_result(model, job_id=1)
    assert result["pydantic_type"].endswith("._SampleModel")
    assert result["data"] == {"name": "test", "count": 42, "ratio": 0.5}


async def test_deserialize_pydantic_model_round_trip(orch_ctx):
    """Pydantic model survives serialize → deserialize round-trip via task result."""
    from aaiclick.orchestration.execution import _deserialize_value

    model = _SampleModel(name="hello", count=7, ratio=None)
    serialized = serialize_task_result(model, job_id=1)

    async with get_sql_session() as session:
        recovered = await _deserialize_value(serialized, session)

    assert isinstance(recovered, _SampleModel)
    assert recovered.name == "hello"
    assert recovered.count == 7
    assert recovered.ratio is None


async def test_execute_task_sync_function(orch_ctx):
    """Test executing a sync task function with no parameters."""
    task = create_task("aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    task.job_id = 1  # Set a dummy job_id

    await execute_task(task)  # Should not raise


async def test_execute_task_async_function(orch_ctx):
    """Test executing an async task function with no parameters."""
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
        async with get_sql_session() as session:
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
        async with get_sql_session() as session:
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
        async with get_sql_session() as session:
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


# TaskResult tests


def test_task_result_defaults():
    """TaskResult has None data and empty tasks by default."""
    r = TaskResult()
    assert r.data is None
    assert r.tasks == []


def test_task_result_tasks_only(orch_ctx):
    """TaskResult with tasks only."""
    t = create_task("mod.func")
    r = TaskResult(tasks=[t])
    assert r.data is None
    assert r.tasks == [t]


def test_task_result_data_only():
    """TaskResult with data only."""
    r = TaskResult(data=42)
    assert r.data == 42
    assert r.tasks == []


def test_task_result_both(orch_ctx):
    """TaskResult with both data and tasks."""
    from aaiclick.snowflake_id import get_snowflake_id

    t = create_task("mod.func")
    g = Group(id=get_snowflake_id(), name="g1")
    r = TaskResult(data="result", tasks=[t, g])
    assert r.data == "result"
    assert t in r.tasks
    assert g in r.tasks


def test_task_result_preserves_explicit_dependency(orch_ctx):
    """Explicit >> dependency on tasks inside TaskResult is preserved."""
    from aaiclick.snowflake_id import get_snowflake_id

    t1 = create_task("mod.step1")
    t2 = create_task("mod.step2")
    g = Group(id=get_snowflake_id(), name="g1")
    t2 >> t1  # t1 depends on t2

    r = TaskResult(tasks=[t1, g])
    dep_ids = {d.previous_id for d in t1.previous_dependencies}
    assert t2.id in dep_ids


# register_returned_tasks tests


async def test_register_returned_tasks_none(orch_ctx):
    """None passes through as None."""
    result = await register_returned_tasks(None, parent_task_id=1, job_id=1)
    assert result is None


async def test_register_returned_tasks_pure_data(orch_ctx):
    """Non-TaskResult values pass through unchanged as data."""
    result = await register_returned_tasks(42, parent_task_id=1, job_id=1)
    assert result == 42


async def test_register_returned_tasks_task_result_tasks_only(orch_ctx):
    """TaskResult with tasks registers them and returns None data."""
    job = await create_job("reg_test", "mod.func")
    parent = create_task("mod.parent")
    parent.job_id = job.id

    child = create_task("mod.child")

    data_result = await register_returned_tasks(
        TaskResult(tasks=[child]), parent_task_id=parent.id, job_id=job.id
    )
    assert data_result is None

    async with get_sql_session() as session:
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


async def test_register_returned_tasks_task_result_with_data(orch_ctx):
    """TaskResult with data and tasks registers tasks and returns data."""
    job = await create_job("reg_data_test", "mod.func")
    parent = create_task("mod.parent")
    parent.job_id = job.id

    c1 = create_task("mod.child1")
    c2 = create_task("mod.child2")

    data_result = await register_returned_tasks(
        TaskResult(data="my_data", tasks=[c1, c2]), parent_task_id=parent.id, job_id=job.id
    )
    assert data_result == "my_data"

    async with get_sql_session() as session:
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
        async with get_sql_session() as session:
            result = await session.execute(
                select(Task).where(Task.job_id == job.id)
            )
            tasks = list(result.scalars().all())
            assert len(tasks) == 1
            assert tasks[0].name == "dynamic_pipeline"
            assert "orchestration_dynamic.dynamic_pipeline" in tasks[0].entrypoint


async def test_dynamic_pipeline_execution(orch_ctx, monkeypatch):
    """@job entry point runs and its returned tasks get registered and executed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        job = await dynamic_pipeline()
        await run_job_tasks(job)

        assert job.status == JobStatus.COMPLETED

        # Entry point + 2 child tasks = 3 tasks total
        async with get_sql_session() as session:
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
        async with get_sql_session() as session:
            result = await session.execute(
                select(Task).where(Task.job_id == job.id).order_by(Task.id)
            )
            tasks = list(result.scalars().all())
            assert len(tasks) == 3

            for t in tasks:
                assert t.status == TaskStatus.COMPLETED


# =============================================================================
# Object fieldtype preservation through serialize/deserialize roundtrip
# This test would have caught the bug where DICT Objects received as task
# parameters were reconstructed as FIELDTYPE_ARRAY, causing explode() to fail.
# =============================================================================


async def test_object_dict_fieldtype_preserved_through_roundtrip(orch_ctx):
    """DICT Object fieldtype survives serialize → deserialize used for task params."""
    from aaiclick import create_object_from_value
    from aaiclick.data.models import FIELDTYPE_DICT

    obj = await create_object_from_value({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    assert obj._schema.fieldtype == FIELDTYPE_DICT

    # Simulate what the worker does: serialize the result, then deserialize as param
    serialized = serialize_task_result(obj, job_id=1)
    result = await deserialize_task_params({"obj": serialized})

    deserialized = result["obj"]
    assert isinstance(deserialized, Object)
    assert deserialized._schema.fieldtype == FIELDTYPE_DICT


async def test_object_array_fieldtype_preserved_through_roundtrip(orch_ctx):
    """ARRAY Object fieldtype survives serialize → deserialize used for task params."""
    from aaiclick import create_object_from_value
    from aaiclick.data.models import FIELDTYPE_ARRAY

    obj = await create_object_from_value([10, 20, 30])
    assert obj._schema.fieldtype == FIELDTYPE_ARRAY

    serialized = serialize_task_result(obj, job_id=1)
    result = await deserialize_task_params({"obj": serialized})

    deserialized = result["obj"]
    assert isinstance(deserialized, Object)
    assert deserialized._schema.fieldtype == FIELDTYPE_ARRAY


async def test_dict_object_explode_works_after_roundtrip(orch_ctx):
    """explode() succeeds on a DICT Object that went through task param roundtrip.

    Regression test: before the fix, _get_table_schema returned FIELDTYPE_ARRAY
    for DICT objects, causing explode() to raise 'can only be used on dict Objects'.
    """
    from aaiclick import create_object_from_value

    obj = await create_object_from_value({"genre": ["Action,Drama", "Comedy"], "title": ["A", "B"]})

    serialized = serialize_task_result(obj, job_id=1)
    result = await deserialize_task_params({"obj": serialized})
    deserialized = result["obj"]

    # This must not raise "explode() can only be used on dict Objects"
    with_split = deserialized.with_split_by_char("genre", ",", element_type="String", alias="g")
    exploded = with_split.explode("g")
    data = await (await exploded.copy()).data()
    assert set(data["g"]) == {"Action", "Drama", "Comedy"}
