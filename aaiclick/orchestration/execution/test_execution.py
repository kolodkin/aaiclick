"""Tests for orchestration execution and Job.test() functionality."""

import asyncio
import inspect
import sys
import time

import pytest
from pydantic import BaseModel
from sqlalchemy import select
from sqlmodel import col

from aaiclick import create_object_from_value
from aaiclick.data.object import Object, View
from aaiclick.internal_api.tasks import get_task_logs
from aaiclick.orchestration.examples.orchestration_dynamic import (
    chain_pipeline,
    dynamic_pipeline,
)
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.execution.runner import (
    _materialize_lazies,
    deserialize_task_params,
    execute_shell_task,
    execute_task,
    import_callback,
    register_returned_tasks,
    register_run,
    run_job_tasks,
    serialize_task_result,
)
from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.jobs import get_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.logging import capture_task_output, read_task_logs
from aaiclick.orchestration.models import (
    JOB_COMPLETED,
    JOB_FAILED,
    JOB_PENDING,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_RUNNING,
    Dependency,
    Group,
    Task,
)
from aaiclick.orchestration.orch_context import commit_tasks, get_sql_session
from aaiclick.orchestration.result import TaskResult, data_list, task_result, tasks_list
from aaiclick.testing import seed_registry_row

# Logging tests


async def test_capture_task_output_stdout(orch_ctx):
    """stdout printed inside the capture scope lands in CH task_logs."""
    task_id, job_id, run_id = 12345, 99, 555

    async with capture_task_output(task_id, job_id, run_id):
        print("Hello, world!")

    lines = await read_task_logs(task_id, run_id)
    assert any(line.text == "Hello, world!" and line.stream == "stdout" for line in lines)


async def test_capture_task_output_stderr(orch_ctx):
    """stderr printed inside the capture scope lands in CH task_logs."""
    task_id, job_id, run_id = 12346, 99, 556

    async with capture_task_output(task_id, job_id, run_id):
        print("Error message", file=sys.stderr)

    lines = await read_task_logs(task_id, run_id)
    assert any(line.text == "Error message" and line.stream == "stderr" for line in lines)


async def _persisted_shell_task(command, command_env=None) -> Task:
    """A shell Task committed under a real job, so register_run has a row."""
    job = await create_job("shell_stream_job", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    task = create_task(None, entry_type="shell", command=command, command_env=command_env)
    await commit_tasks(task, job.id)
    return task


async def test_execute_shell_task_streams_mid_run(orch_ctx, monkeypatch, tmp_path):
    """Shell output reaches task_logs while the process is still running."""
    monkeypatch.setattr("aaiclick.orchestration.logging.LOG_FLUSH_INTERVAL", 0.05)
    # The process blocks on a gate file the test controls, so "second" cannot be
    # emitted until the mid-run state has been verified.
    gate = tmp_path / "gate"
    script = f"echo first; until [ -e '{gate}' ]; do sleep 0.05; done; echo second"
    task = await _persisted_shell_task(["sh", "-c", script])

    exec_task = asyncio.create_task(execute_shell_task(task))

    async def _current_lines() -> list[str]:
        return [line.text for line in (await get_task_logs(task.id)).lines]

    try:
        deadline = time.monotonic() + 30
        while not (mid := await _current_lines()):
            assert time.monotonic() < deadline, "'first' was never flushed to task_logs"
            await asyncio.sleep(0.05)
    finally:
        # An assertion failure must not orphan the gate-blocked shell.
        gate.touch()
        await asyncio.wait_for(exec_task, timeout=30)

    assert mid == ["first"]
    assert await _current_lines() == ["first", "second"]


async def test_execute_shell_task_splits_streams(orch_ctx):
    """Shell stdout and stderr keep their streams; stderr defaults to WARNING."""
    task = await _persisted_shell_task(["sh", "-c", "echo out line; echo err line 1>&2"])
    await execute_shell_task(task)
    refreshed = await get_task(task.id)
    assert refreshed is not None
    lines = await read_task_logs(task.id, refreshed.run_ids[-1])
    # Cross-stream ordering is approximate (two pipes) — compare as a set.
    assert {(line.stream, line.level, line.text) for line in lines} == {
        ("stdout", "INFO", "out line"),
        ("stderr", "WARNING", "err line"),
    }


async def test_capture_task_output_streams_mid_run(orch_ctx, monkeypatch):
    """Completed lines are readable from task_logs while the task body is still running."""
    monkeypatch.setattr("aaiclick.orchestration.logging.LOG_FLUSH_INTERVAL", 0.05)
    task_id, job_id, run_id = 71, 1, 9101
    mid_run_lines: list[str] = []
    async with capture_task_output(task_id, job_id, run_id):
        print("early line")
        await asyncio.sleep(0.3)  # let the flusher tick
        mid_run_lines = [line.text for line in await read_task_logs(task_id, run_id)]
        print("late line")
    assert mid_run_lines == ["early line"]
    final = [line.text for line in await read_task_logs(task_id, run_id)]
    assert final == ["early line", "late line"]


async def test_register_run_appends_run_ids_and_statuses(orch_ctx):
    """Each register_run call mints a distinct run_id and appends RUNNING."""
    job = await create_job("register_run_job", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    task = (await get_tasks_for_job(job.id))[0]

    run1 = await register_run(task.id)
    run2 = await register_run(task.id)

    assert run1 != run2
    refreshed = await get_task(task.id)
    assert refreshed is not None
    assert refreshed.run_ids == [run1, run2]
    assert refreshed.run_statuses == [TASK_RUNNING, TASK_RUNNING]


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
    assert inspect.iscoroutinefunction(func)
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
    await seed_registry_row("t123")
    kwargs = {"data": {"object_type": "object", "table": "t123"}}

    result = await deserialize_task_params(kwargs)
    assert "data" in result
    assert isinstance(result["data"], Object)
    assert result["data"].table == "t123"


async def test_deserialize_task_params_view(orch_ctx):
    """Test deserializing a View parameter with constraints."""
    await seed_registry_row("t456")
    kwargs = {
        "data": {
            "object_type": "view",
            "table": "t456",
            "where": "value > 10",
            "limit": 100,
            "offset": 50,
            "order_by": "value ASC",
        }
    }

    result = await deserialize_task_params(kwargs)
    assert "data" in result
    assert isinstance(result["data"], View)
    assert result["data"].table == "t456"
    assert result["data"]._build_where() == "(value > 10)"
    assert result["data"].limit == 100
    assert result["data"].offset == 50
    assert result["data"].order_by == "value ASC"


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


async def test_materialize_lazies_bare_lazy(orch_ctx):
    """A LazyOperator returned from a task body materializes to Object."""
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    b = await create_object_from_value([10, 20, 30], aai_id=True)

    lazy = a + b  # LazyOperator — no DB call
    materialized = await _materialize_lazies(lazy)

    assert isinstance(materialized, Object)
    assert materialized.table.startswith("t_")
    assert await materialized.data() == [11, 22, 33]


async def test_materialize_lazies_in_task_result_data(orch_ctx):
    """TaskResult.data may be a LazyOperator — get unwrapped, .tasks untouched."""
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    b = await create_object_from_value([10, 20, 30], aai_id=True)

    tr = TaskResult(data=a + b, tasks=[])
    out = await _materialize_lazies(tr)

    assert isinstance(out, TaskResult)
    assert isinstance(out.data, Object)
    assert await out.data.data() == [11, 22, 33]


async def test_materialize_lazies_passthrough_for_non_lazy(orch_ctx):
    """Non-lazy values flow through unchanged (identity-preserving)."""
    obj = Object(table="t789")
    assert await _materialize_lazies(obj) is obj
    assert await _materialize_lazies(None) is None
    assert await _materialize_lazies(42) == 42
    assert await _materialize_lazies("hello") == "hello"


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
    assert result is not None
    assert result["pydantic_type"].endswith("._SampleModel")
    assert result["data"] == {"name": "test", "count": 42, "ratio": 0.5}


async def test_deserialize_pydantic_model_round_trip(orch_ctx):
    """Pydantic model survives serialize → deserialize round-trip via task result."""
    from aaiclick.orchestration.execution.runner import _deserialize_value

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


async def test_run_job_tasks_single_task(orch_ctx):
    """Test running a job with a single task."""
    job = await create_job("test_job", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    await run_job_tasks(job)

    assert job.status == JOB_COMPLETED
    assert job.completed_at is not None

    # Verify task completed in database
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        tasks = list(result.scalars().all())
        assert len(tasks) == 1
        assert tasks[0].status == TASK_COMPLETED


async def test_run_job_tasks_failing_task(orch_ctx):
    """Test running a job with a failing task."""
    job = await create_job("test_job_fail", "aaiclick.orchestration.fixtures.sample_tasks.failing_task")

    await run_job_tasks(job)

    assert job.status == JOB_FAILED
    assert job.error is not None
    assert "intentionally" in job.error

    # Verify task failed in database
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        tasks = list(result.scalars().all())
        assert len(tasks) == 1
        assert tasks[0].status == TASK_FAILED
        assert tasks[0].error is not None


async def test_run_job_tasks_streams_logs_to_clickhouse(orch_ctx):
    """Task stdout/stderr is readable cross-host via the CH task_logs stream."""
    job = await create_job("test_job_ch_log", "aaiclick.orchestration.fixtures.sample_tasks.task_with_output")

    await run_job_tasks(job)

    task = (await get_tasks_for_job(job.id))[0]
    logs = await get_task_logs(task.id)

    assert logs.available is True
    by_text = {line.text: line.stream for line in logs.lines}
    assert by_text.get("This is stdout") == "stdout"
    assert by_text.get("Error message") == "stderr"


async def test_run_job_tasks_shell_task(orch_ctx):
    """A shell entry task runs in-process and flushes its output inline to CH."""
    entry = create_task(None, entry_type="shell", command=["sh", "-c", "echo shell line"])
    job = await create_job("test_job_shell", entry)

    await run_job_tasks(job)

    assert job.status == JOB_COMPLETED
    task = (await get_tasks_for_job(job.id))[0]
    assert task.status == TASK_COMPLETED
    logs = await get_task_logs(task.id)
    assert [line.text for line in logs.lines] == ["shell line"]


async def test_run_job_tasks_failing_shell_task(orch_ctx):
    """A shell task's nonzero exit fails the job; output is still flushed."""
    entry = create_task(None, entry_type="shell", command=["sh", "-c", "echo boom; exit 3"])
    job = await create_job("test_job_shell_fail", entry)

    await run_job_tasks(job)

    assert job.status == JOB_FAILED
    assert "exit 3" in (job.error or "")
    task = (await get_tasks_for_job(job.id))[0]
    assert task.status == TASK_FAILED
    logs = await get_task_logs(task.id)
    assert [line.text for line in logs.lines] == ["boom"]


async def test_run_job_tasks_shell_command_env(orch_ctx):
    """command_env is overlaid onto the shell task's environment."""
    cmd = ["python", "-c", "import os,sys; sys.exit(0 if os.environ.get('K')=='v' else 3)"]
    entry = create_task(None, entry_type="shell", command=cmd, command_env={"K": "v"})
    job = await create_job("test_job_shell_env", entry)

    await run_job_tasks(job)

    assert job.status == JOB_COMPLETED


# job_test() tests


async def test_job_test_simple(orch_ctx):
    """Test job_test() executes a simple task synchronously.

    Note: job_test() uses asyncio.run() internally, which is tested
    via ajob_test() in the async context to avoid nested event loops.
    """
    job = await create_job("test_sync", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")

    # Test execution via the async helper (same code path as job_test())
    await ajob_test(job)

    assert job.status == JOB_COMPLETED


# TaskResult tests


def test_task_result_defaults():
    """TaskResult has None data and empty tasks by default."""
    r = TaskResult()
    assert r.data is None
    assert r.tasks == []


def test_task_result_tasks_only(orch_ctx):
    """TaskResult with tasks only."""
    t = create_task("mod.func")
    r = tasks_list(t)
    assert r.data is None
    assert r.tasks == [t]


def test_task_result_data_only():
    """TaskResult with data only."""
    r = data_list(42)
    assert r.data == 42
    assert r.tasks == []


def test_task_result_both(orch_ctx):
    """TaskResult with both data and tasks."""
    from aaiclick.snowflake import get_snowflake_id

    t = create_task("mod.func")
    g = Group(id=get_snowflake_id(), name="g1")
    r = task_result(data="result", tasks=[t, g])
    assert r.data == "result"
    assert t in r.tasks
    assert g in r.tasks


def test_task_result_preserves_explicit_dependency(orch_ctx):
    """Explicit >> dependency on tasks inside TaskResult is preserved."""
    from aaiclick.snowflake import get_snowflake_id

    t1 = create_task("mod.step1")
    t2 = create_task("mod.step2")
    g = Group(id=get_snowflake_id(), name="g1")
    t2 >> t1  # t1 depends on t2

    tasks_list(t1, g)
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

    data_result = await register_returned_tasks(tasks_list(child), parent_task_id=parent.id, job_id=job.id)
    assert data_result is None

    db_child = await get_task(child.id)
    assert db_child is not None
    assert db_child.job_id == job.id

    async with get_sql_session() as session:
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
        task_result(data="my_data", tasks=[c1, c2]), parent_task_id=parent.id, job_id=job.id
    )
    assert data_result == "my_data"

    async with get_sql_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id, col(Task.entrypoint).in_(["mod.child1", "mod.child2"]))
        )
        children = result.scalars().all()
        assert len(children) == 2


# Dynamic pipeline integration tests


async def test_dynamic_pipeline_creates_entry_task(orch_ctx):
    """@job creates a Job with an entry point task."""
    job = await dynamic_pipeline()

    assert job.status == JOB_PENDING

    # Verify entry point task was created
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        tasks = list(result.scalars().all())
        assert len(tasks) == 1
        assert tasks[0].name == "dynamic_pipeline"
        assert "orchestration_dynamic.dynamic_pipeline" in tasks[0].entrypoint


async def test_dynamic_pipeline_execution(orch_ctx):
    """@job entry point runs and its returned tasks get registered and executed."""
    job = await dynamic_pipeline()
    await run_job_tasks(job)

    assert job.status == JOB_COMPLETED

    # Entry point + 2 child tasks = 3 tasks total
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id).order_by(Task.id))
        tasks = list(result.scalars().all())
        assert len(tasks) == 3

        # All tasks should be completed
        for t in tasks:
            assert t.status == TASK_COMPLETED

        # Child tasks should have results
        child_tasks = [t for t in tasks if t.name != "dynamic_pipeline"]
        assert len(child_tasks) == 2
        for ct in child_tasks:
            assert ct.result is not None


async def test_chain_pipeline_execution(orch_ctx):
    """Chained dynamic creation: task A returns task B, task B returns task C."""
    job = await chain_pipeline()
    await run_job_tasks(job)

    assert job.status == JOB_COMPLETED

    # chain_pipeline -> step_one -> step_two = 3 tasks
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id).order_by(Task.id))
        tasks = list(result.scalars().all())
        assert len(tasks) == 3

        for t in tasks:
            assert t.status == TASK_COMPLETED


# =============================================================================
# Object fieldtype preservation through serialize/deserialize roundtrip
# This test would have caught the bug where DICT Objects received as task
# parameters were reconstructed as FIELDTYPE_ARRAY, causing explode() to fail.
# =============================================================================


async def test_object_dict_fieldtype_preserved_through_roundtrip(orch_ctx):
    """DICT Object fieldtype survives serialize → deserialize used for task params."""
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

    obj = await create_object_from_value({"genre": ["Action,Drama", "Comedy"], "title": ["A", "B"]})

    serialized = serialize_task_result(obj, job_id=1)
    result = await deserialize_task_params({"obj": serialized})
    deserialized = result["obj"]

    # This must not raise "explode() can only be used on dict Objects"
    with_split = deserialized.with_split_by_char("genre", ",", element_type="String", alias="g")
    exploded = with_split.explode("g")
    data = await (await exploded.copy()).data()
    assert set(data["g"]) == {"Action", "Drama", "Comedy"}
