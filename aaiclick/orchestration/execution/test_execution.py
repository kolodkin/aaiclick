"""Tests for orchestration execution and Job.test() functionality."""

import asyncio
import inspect
import sys
import time

import pytest
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlmodel import col

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context
from aaiclick.data.models import FIELDTYPE_ARRAY, FIELDTYPE_DICT
from aaiclick.data.object import Object, View
from aaiclick.data.object.refs import ViewRef
from aaiclick.internal_api.tasks import get_task_logs
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.examples.orchestration_dynamic import (
    chain_pipeline,
    dynamic_pipeline,
)
from aaiclick.orchestration.execution.claiming import update_task_status
from aaiclick.orchestration.execution.db_handler import DEPENDENCY_WHERE
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.execution.runner import (
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
from aaiclick.orchestration.jobs import get_job_result, get_task
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
from aaiclick.orchestration.result import TaskResult, task_result, tasks_list
from aaiclick.snowflake import get_snowflake_id
from aaiclick.testing import seed_registry_row


class _SampleModel(BaseModel):
    name: str
    count: int
    ratio: float | None = None


# --- Tasks and jobs (module-level so entrypoints resolve) ---


@task
async def make_object(value: list | dict) -> Object:
    return await create_object_from_value(value)


@task
async def add_lazily() -> Object:
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    b = await create_object_from_value([10, 20, 30], aai_id=True)
    return a + b  # LazyOperator — the runner materializes it


@task
async def add_lazily_in_task_result() -> TaskResult:
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    b = await create_object_from_value([10, 20, 30], aai_id=True)
    return task_result(data=a + b)  # the runner unwraps and materializes .data


@task
def echo(value: int | str) -> int | str:
    return value


@task
def make_model() -> _SampleModel:
    return _SampleModel(name="hello", count=7, ratio=None)


@task
def bump_model(model: _SampleModel) -> _SampleModel:
    return model.model_copy(update={"count": model.count + 1})


@task
def object_fieldtype(obj: Object) -> str:
    return obj.schema.fieldtype


@task
async def explode_genres(obj: Object) -> list[str]:
    exploded = obj.with_split_by_char("genre", ",", element_type="String", alias="g").explode("g")
    return sorted((await (await exploded.copy()).data())["g"])


@task
def first_step() -> None:
    pass


@task
def second_step() -> None:
    pass


@job("object_result_job")
def object_result_job():
    obj = make_object(value=[10, 20, 30])
    return task_result(data=obj, tasks=[obj])


@job("lazy_result_job")
def lazy_result_job():
    total = add_lazily()
    return task_result(data=total, tasks=[total])


@job("lazy_task_result_job")
def lazy_task_result_job():
    total = add_lazily_in_task_result()
    return task_result(data=total, tasks=[total])


@job("native_result_job")
def native_result_job(value: int | str):
    echoed = echo(value=value)
    return task_result(data=echoed, tasks=[echoed])


@job("pydantic_handoff_job")
def pydantic_handoff_job():
    made = make_model()
    bumped = bump_model(model=made)
    return task_result(data=bumped, tasks=[made, bumped])


@job("object_fieldtype_job")
def object_fieldtype_job(value: list | dict):
    obj = make_object(value=value)
    fieldtype = object_fieldtype(obj=obj)
    return task_result(data=fieldtype, tasks=[obj, fieldtype])


@job("explode_after_handoff_job")
def explode_after_handoff_job():
    obj = make_object(value={"genre": ["Action,Drama", "Comedy"], "title": ["A", "B"]})
    genres = explode_genres(obj=obj)
    return task_result(data=genres, tasks=[obj, genres])


@job("explicit_dependency_job")
def explicit_dependency_job():
    first = first_step()
    second = second_step()
    first >> second
    return tasks_list(second)  # only ``second`` is returned; the >> edge must pull ``first`` in


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
        deadline = time.monotonic() + 30
        while not (mid_run_lines := [line.text for line in await read_task_logs(task_id, run_id)]):
            assert time.monotonic() < deadline, "'early line' was never flushed to task_logs"
            await asyncio.sleep(0.05)
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


async def test_task_object_result_round_trips(orch_ctx):
    """An Object returned by a task is stored as a reference and read back as data."""
    job = await object_result_job()
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, job.error
    async with data_context():
        result = await get_job_result(job)
        assert await result.data() == [10, 20, 30]


@pytest.mark.parametrize(
    "pipeline",
    [
        # A task body returning ``a + b`` without an explicit await.
        pytest.param(lazy_result_job, id="bare_lazy"),
        # TaskResult.data may be a LazyOperator — it gets unwrapped too.
        pytest.param(lazy_task_result_job, id="task_result_data_lazy"),
    ],
)
async def test_task_lazy_result_is_materialized(orch_ctx, pipeline):
    """A LazyOperator returned from a task is materialized before serialization."""
    job = await pipeline()
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, job.error
    async with data_context():
        result = await get_job_result(job)
        assert await result.data() == [11, 22, 33]


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(42, id="int"),
        pytest.param("hello", id="str"),
    ],
)
async def test_task_native_result_round_trips(orch_ctx, value):
    """A non-Object/View task result is stored as a native value and read back unchanged."""
    job = await native_result_job(value=value)
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, job.error
    async with data_context():
        assert await get_job_result(job) == value


async def test_task_pydantic_result_passes_between_tasks(orch_ctx):
    """A pydantic model result reaches a downstream task and the job result as the model."""
    job = await pydantic_handoff_job()
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, job.error
    async with data_context():
        assert await get_job_result(job) == _SampleModel(name="hello", count=8, ratio=None)


@pytest.mark.parametrize(
    "entrypoint",
    [
        pytest.param("aaiclick.orchestration.fixtures.sample_tasks.simple_task", id="sync"),
        pytest.param("aaiclick.orchestration.fixtures.sample_tasks.async_task", id="async"),
    ],
)
async def test_execute_task_no_parameters(orch_ctx, entrypoint):
    """Test executing a task function with no parameters."""
    task = create_task(entrypoint)
    task.job_id = 1  # Set a dummy job_id

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


def test_task_result_both(orch_ctx):
    """TaskResult with both data and tasks."""
    t = create_task("mod.func")
    g = Group(id=get_snowflake_id(), name="g1")
    r = task_result(data="result", tasks=[t, g])
    assert r.data == "result"
    assert t in r.tasks
    assert g in r.tasks


async def test_returned_tasks_keep_explicit_dependency(orch_ctx):
    """An explicit >> edge between returned tasks is persisted and honored."""
    job = await explicit_dependency_job()
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, job.error
    tasks = {t.name: t for t in await get_tasks_for_job(job.id)}
    assert {t.status for t in tasks.values()} == {TASK_COMPLETED}
    async with get_sql_session() as session:
        result = await session.execute(
            select(Dependency).where(
                col(Dependency.previous_id) == tasks["first_step"].id,
                col(Dependency.next_id) == tasks["second_step"].id,
            )
        )
        assert result.scalar_one_or_none() is not None


# register_returned_tasks tests


async def _is_ready(task_id: int) -> bool:
    """Whether the scheduler's dependency check lets ``task_id`` be claimed."""
    async with get_sql_session() as session:
        row = await session.execute(
            text(f"SELECT t.id FROM tasks t WHERE t.id = :task_id {DEPENDENCY_WHERE}"),
            {"task_id": task_id, "completed_status": TASK_COMPLETED},
        )
        return row.first() is not None


async def test_hold_covers_members_added_to_a_successor_group_later(orch_ctx):
    """parent >> G with G empty at registration: a member added afterwards still waits for the data task."""
    job = await create_job("hold_late_member", "mod.func")
    parent = create_task("mod.parent")
    group = Group(id=get_snowflake_id(), name="later")
    parent >> group
    await commit_tasks([parent, group], job_id=job.id)

    child = create_task("mod.child")
    await register_returned_tasks(task_result(data=child, tasks=[child]), parent_task_id=parent.id, job_id=job.id)

    member = create_task("mod.member")
    member.group_id = group.id
    await commit_tasks([member], job_id=job.id)
    await update_task_status(parent.id, TASK_COMPLETED)

    assert not await _is_ready(member.id)
    await update_task_status(child.id, TASK_COMPLETED)
    assert await _is_ready(member.id)


async def test_register_returned_tasks_pins_child_input_tables(orch_ctx):
    """Dynamic children are pinned on every ephemeral table their kwargs reference.

    The parent is their producer in fact but not by any edge that exists when
    it pins, so registration inserts the pins itself. Persistent refs are not
    lifecycle-managed and stay unpinned.
    """
    job = await create_job("reg_pins", "mod.func")
    parent = create_task("mod.parent")
    parent.job_id = job.id
    child = create_task(
        "mod.child",
        kwargs={
            "data": Object(table="t_source")._serialize_ref(),
            "nested": [ViewRef(table="t_view", limit=2, offset=0, order_by="tuple()").to_dict()],
            "keep": {"object_type": "object", "table": "p_keep", "persistent": True},
        },
    )

    await register_returned_tasks(tasks_list(child), parent_task_id=parent.id, job_id=job.id)

    async with get_sql_session() as session:
        rows = await session.execute(text("SELECT table_name, task_id FROM table_pin_refs"))
        assert {tuple(r) for r in rows} == {("t_source", child.id), ("t_view", child.id)}


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


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(None, id="none"),
        # Non-TaskResult values pass through unchanged as data.
        pytest.param(42, id="scalar_data"),
        # A list holding no Task/Group stays pure data.
        pytest.param([1, 2, 3], id="list_of_data"),
    ],
)
async def test_register_returned_tasks_passes_data_through(orch_ctx, value):
    """Returns holding no Task/Group register nothing and come back unchanged."""
    assert await register_returned_tasks(value, parent_task_id=1, job_id=1) == value


@pytest.mark.parametrize(
    "shape",
    [
        pytest.param(list, id="flat_list"),
        # ``return a, b`` — no brackets needed.
        pytest.param(tuple, id="bare_tuple"),
    ],
)
async def test_register_returned_tasks_flat_shapes(orch_ctx, shape):
    """A flat list or tuple of Tasks registers them and returns None data."""
    job = await create_job("reg_shape", "mod.func")
    parent = create_task("mod.parent")
    parent.job_id = job.id

    payload = shape(create_task(f"mod.child{i}") for i in range(4))

    assert await register_returned_tasks(payload, parent_task_id=parent.id, job_id=job.id) is None

    async with get_sql_session() as session:
        result = await session.execute(
            select(Task).where(Task.job_id == job.id, col(Task.entrypoint).like("mod.child%"))
        )
        assert len(result.scalars().all()) == 4


@pytest.mark.parametrize(
    "build_payload",
    [
        pytest.param(lambda a, b: ([a], [b]), id="tuple_of_lists"),
        pytest.param(lambda a, b: [(a,), (b,)], id="list_of_tuples"),
        pytest.param(lambda a, b: [a, [b]], id="deep"),
        # The same flatness rule applies to task_result(tasks=[...]).
        pytest.param(lambda a, b: task_result(tasks=[[a, b]]), id="nested_task_result"),
    ],
)
async def test_register_returned_tasks_rejects_nesting(orch_ctx, build_payload):
    """Nesting carries no meaning in the graph, so it is rejected, not flattened."""
    payload = build_payload(create_task("mod.child0"), create_task("mod.child1"))

    with pytest.raises(TypeError, match="nested"):
        await register_returned_tasks(payload, parent_task_id=1, job_id=1)


async def test_register_returned_tasks_list_mixing_tasks_and_data(orch_ctx):
    """Mixing Tasks with data in a list is rejected rather than silently dropped."""
    job = await create_job("reg_mixed_test", "mod.func")
    parent = create_task("mod.parent")
    parent.job_id = job.id

    with pytest.raises(TypeError, match="task_result"):
        await register_returned_tasks([create_task("mod.child1"), "my_data"], parent_task_id=parent.id, job_id=job.id)


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


@pytest.mark.parametrize(
    "value, expected",
    [
        # Regression: DICT Objects received as task parameters were
        # reconstructed as FIELDTYPE_ARRAY, causing explode() to fail.
        pytest.param({"x": [1, 2, 3], "y": ["a", "b", "c"]}, FIELDTYPE_DICT, id="dict"),
        pytest.param([10, 20, 30], FIELDTYPE_ARRAY, id="array"),
    ],
)
async def test_object_fieldtype_preserved_between_tasks(orch_ctx, value, expected):
    """An Object handed to a downstream task keeps its fieldtype."""
    job = await object_fieldtype_job(value=value)
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, job.error
    async with data_context():
        assert await get_job_result(job) == expected


async def test_dict_object_explode_works_after_handoff(orch_ctx):
    """explode() succeeds on a DICT Object received as a task parameter.

    Regression test: before the fix, _get_table_schema returned FIELDTYPE_ARRAY
    for DICT objects, causing explode() to raise 'can only be used on dict Objects'.
    """
    job = await explode_after_handoff_job()
    await ajob_test(job)

    assert job.status == JOB_COMPLETED, job.error
    async with data_context():
        assert await get_job_result(job) == ["Action", "Comedy", "Drama"]
