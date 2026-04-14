"""
Tests for ``replay_job`` — Phase 3b of the lineage plan.

Split into:
- **Pure unit tests**: classification helpers and kwarg/dep rewriting that
  operate on in-memory dicts. No DB.
- **DB-backed tests**: craft synthetic ``Job`` / ``Task`` / ``Dependency``
  rows and assert the cloned graph looks right after ``replay_job``.
- **End-to-end test**: run a real decorator pipeline, then replay it and
  verify the replayed job runs successfully using the persistent inputs.
"""

from __future__ import annotations

import pytest
from sqlmodel import select

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.data_context.ch_client import create_ch_client
from aaiclick.data.object import Object
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.execution.runner import run_job_tasks
from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.models import (
    Dependency,
    Job,
    JobStatus,
    PreservationMode,
    RunType,
    Task,
    TaskStatus,
)
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.replay import (
    _RewriteCtx,
    _clone_dependencies,
    _is_wiring_task,
    _rewrite_value,
    replay_job,
)
from aaiclick.snowflake_id import get_snowflake_id


def _task_row(task_id, result=None, kwargs=None, name="t", entrypoint="mod.fn") -> Task:
    return Task(
        id=task_id,
        job_id=1,
        entrypoint=entrypoint,
        name=name,
        kwargs=kwargs or {},
        result=result,
    )


# ---------------------------------------------------------------------
# Pure unit tests
# ---------------------------------------------------------------------


def test_is_wiring_task_none_result():
    """None-result task with dynamically-spawned children is wiring."""
    assert (
        _is_wiring_task(_task_row(1, result=None), dynamic_parent_ids={1})
        is True
    )


def test_is_wiring_task_none_result_no_dynamic_children():
    """None-result task that did NOT spawn children at runtime is NOT
    wiring — e.g. a legitimate compute task that happened to return
    None. It should be cloned and re-executed rather than skipped."""
    assert (
        _is_wiring_task(_task_row(1, result=None), dynamic_parent_ids=set())
        is False
    )


def test_is_wiring_task_upstream_ref_result():
    """Task that returned a bare Task ref AND spawned children is wiring."""
    task_row = _task_row(1, result={"ref_type": "upstream", "task_id": 99})
    assert _is_wiring_task(task_row, dynamic_parent_ids={1}) is True


def test_is_wiring_task_object_result():
    """Compute tasks with Object results are never wiring, even if they
    happened to produce dynamic children — the data result takes
    precedence because replay can reuse it."""
    task_row = _task_row(1, result={"object_type": "object", "table": "t_1"})
    assert _is_wiring_task(task_row, dynamic_parent_ids={1}) is False


def _empty_ctx(
    *,
    task_id_map=None,
    input_task_refs=None,
    wiring_targets=None,
) -> _RewriteCtx:
    return _RewriteCtx(
        task_id_map=task_id_map or {},
        input_task_refs=input_task_refs or {},
        wiring_targets=wiring_targets or {},
    )


def test_rewrite_value_replaces_input_ref():
    """An upstream ref targeting an input task becomes an inlined Object ref."""
    rewritten = _rewrite_value(
        {"left": {"ref_type": "upstream", "task_id": 10}},
        _empty_ctx(
            input_task_refs={
                10: {"object_type": "object", "table": "p_foo", "persistent": True}
            },
        ),
    )
    assert rewritten == {
        "left": {"object_type": "object", "table": "p_foo", "persistent": True},
    }


def test_rewrite_value_remaps_compute_ref():
    """An upstream ref targeting a cloned compute task gets its new id."""
    rewritten = _rewrite_value(
        {"x": {"ref_type": "upstream", "task_id": 20}},
        _empty_ctx(task_id_map={20: 2000}),
    )
    assert rewritten == {"x": {"ref_type": "upstream", "task_id": 2000}}


def test_rewrite_value_unknown_ref_raises():
    """A dangling upstream ref signals a broken graph — fail loudly."""
    with pytest.raises(ValueError, match="neither an input task"):
        _rewrite_value(
            {"x": {"ref_type": "upstream", "task_id": 99}},
            _empty_ctx(),
        )


def test_rewrite_value_preserves_nested_structures():
    """Native values, lists, and non-ref dicts are passed through."""
    rewritten = _rewrite_value(
        {
            "n": 42,
            "s": "hello",
            "lst": [1, {"ref_type": "upstream", "task_id": 10}],
            "obj": {"object_type": "object", "table": "t_x"},
        },
        _empty_ctx(task_id_map={10: 100}),
    )
    assert rewritten == {
        "n": 42,
        "s": "hello",
        "lst": [1, {"ref_type": "upstream", "task_id": 100}],
        "obj": {"object_type": "object", "table": "t_x"},
    }


def test_clone_dependencies_drops_edges_outside_map():
    """Dependencies with endpoints outside ``task_id_map`` are dropped —
    those endpoints belonged to skipped (input or wiring) tasks."""
    deps = [
        Dependency(previous_id=1, previous_type="task", next_id=3, next_type="task"),
        Dependency(previous_id=2, previous_type="task", next_id=3, next_type="task"),
        Dependency(previous_id=3, previous_type="task", next_id=4, next_type="task"),
    ]
    cloned = _clone_dependencies(deps, task_id_map={3: 300, 4: 400})
    assert len(cloned) == 1
    only = cloned[0]
    assert only.previous_id == 300
    assert only.next_id == 400


# ---------------------------------------------------------------------
# DB-backed synthetic tests
# ---------------------------------------------------------------------


async def _insert_synthetic_job(
    *,
    job_name: str,
    make_left_result: dict,
    make_right_result: dict,
    add_result: dict,
) -> tuple[int, int, int, int]:
    """Insert a flat 3-task graph (make_left, make_right, _add) + deps.

    The ``add_them`` task's kwargs are built here using the pre-allocated
    input task IDs, so callers don't need to patch placeholder refs after
    the insert.

    Returns ``(job_id, make_left_id, make_right_id, add_id)``.
    """
    job_id = get_snowflake_id()
    ml_id = get_snowflake_id()
    mr_id = get_snowflake_id()
    add_id = get_snowflake_id()

    add_kwargs = {
        "left": {"ref_type": "upstream", "task_id": ml_id},
        "right": {"ref_type": "upstream", "task_id": mr_id},
    }

    async with get_sql_session() as session:
        session.add(
            Job(
                id=job_id,
                name=job_name,
                status=JobStatus.COMPLETED,
                run_type=RunType.MANUAL,
                preservation_mode=PreservationMode.NONE,
            )
        )
        session.add(
            Task(
                id=ml_id,
                job_id=job_id,
                entrypoint="mod.make_left",
                name="make_left",
                kwargs={},
                status=TaskStatus.COMPLETED,
                result=make_left_result,
            )
        )
        session.add(
            Task(
                id=mr_id,
                job_id=job_id,
                entrypoint="mod.make_right",
                name="make_right",
                kwargs={},
                status=TaskStatus.COMPLETED,
                result=make_right_result,
            )
        )
        session.add(
            Task(
                id=add_id,
                job_id=job_id,
                entrypoint="mod.add_them",
                name="add_them",
                kwargs=add_kwargs,
                status=TaskStatus.COMPLETED,
                result=add_result,
            )
        )
        session.add(
            Dependency(previous_id=ml_id, previous_type="task", next_id=add_id, next_type="task")
        )
        session.add(
            Dependency(previous_id=mr_id, previous_type="task", next_id=add_id, next_type="task")
        )
        await session.commit()

    return job_id, ml_id, mr_id, add_id


async def test_replay_job_clones_compute_tasks_and_inlines_inputs(orch_ctx_no_ch):
    """The canonical 2-inputs-plus-1-compute replay described in the plan."""
    job_id, ml_id, mr_id, add_id = await _insert_synthetic_job(
        job_name="synthetic",
        make_left_result={
            "object_type": "object",
            "table": "p_synth_left",
            "persistent": True,
        },
        make_right_result={
            "object_type": "object",
            "table": "p_synth_right",
            "persistent": True,
        },
        add_result={"object_type": "object", "table": "t_synth_sum"},
    )

    strategy = {"p_synth_left": "value = 10"}
    result = await replay_job(job_id, sampling_strategy=strategy)
    replayed = result.job

    assert replayed.name == "synthetic"  # inherited from the original
    assert replayed.id != job_id
    assert replayed.preservation_mode == PreservationMode.STRATEGY
    assert replayed.sampling_strategy == strategy
    assert replayed.status == JobStatus.PENDING
    assert add_id in result.task_id_map

    async with get_sql_session() as session:
        cloned_tasks = list(
            (
                await session.execute(select(Task).where(Task.job_id == replayed.id))
            ).scalars().all()
        )
        cloned_deps = list(
            (
                await session.execute(
                    select(Dependency).where(
                        Dependency.previous_type == "task",
                        Dependency.next_type == "task",
                    )
                )
            ).scalars().all()
        )

    # Only the compute task got cloned.
    assert len(cloned_tasks) == 1
    cloned = cloned_tasks[0]
    assert cloned.entrypoint == "mod.add_them"
    assert cloned.name == "add_them"
    assert cloned.status == TaskStatus.PENDING
    assert cloned.id != add_id
    assert result.task_id_map[add_id] == cloned.id

    # Kwargs had upstream refs replaced with persistent Object refs.
    assert cloned.kwargs == {
        "left": {"object_type": "object", "table": "p_synth_left", "persistent": True},
        "right": {"object_type": "object", "table": "p_synth_right", "persistent": True},
    }

    # Dependencies touching input tasks are dropped — the clone has none.
    clone_deps = [
        d for d in cloned_deps if d.previous_id == cloned.id or d.next_id == cloned.id
    ]
    assert clone_deps == []


async def test_replay_job_requires_non_empty_strategy(orch_ctx_no_ch):
    """An empty strategy has no lineage value — reject it up front."""
    job_id, *_ = await _insert_synthetic_job(
        job_name="synth2",
        make_left_result={
            "object_type": "object",
            "table": "p_x",
            "persistent": True,
        },
        make_right_result={
            "object_type": "object",
            "table": "p_y",
            "persistent": True,
        },
        add_result={"object_type": "object", "table": "t_sum"},
    )
    with pytest.raises(ValueError, match="non-empty sampling_strategy"):
        await replay_job(job_id, sampling_strategy={})


async def test_replay_job_missing_original_raises(orch_ctx_no_ch):
    with pytest.raises(ValueError, match="not found"):
        await replay_job(999999, sampling_strategy={"p_x": "id = 1"})


async def test_replay_job_all_input_tasks_raises(orch_ctx_no_ch):
    """A graph with only input tasks (no compute) is not replayable."""
    job_id = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(
            Job(
                id=job_id,
                name="all_inputs",
                status=JobStatus.COMPLETED,
                run_type=RunType.MANUAL,
            )
        )
        session.add(
            Task(
                id=get_snowflake_id(),
                job_id=job_id,
                entrypoint="mod.fetch",
                name="fetch",
                kwargs={},
                status=TaskStatus.COMPLETED,
                result={
                    "object_type": "object",
                    "table": "p_only",
                    "persistent": True,
                },
            )
        )
        await session.commit()

    with pytest.raises(ValueError, match="no compute tasks"):
        await replay_job(job_id, sampling_strategy={"p_only": "id = 1"})


# ---------------------------------------------------------------------
# End-to-end: run a real pipeline then replay it
# ---------------------------------------------------------------------


@task
async def _rp_make_left(suffix: str) -> Object:
    return await create_object_from_value([10, 20, 30], name=f"rp_left_{suffix}")


@task
async def _rp_make_right(suffix: str) -> Object:
    return await create_object_from_value([1, 2, 3], name=f"rp_right_{suffix}")


@task
async def _rp_add(left: Object, right: Object) -> Object:
    return await (left + right)


@job("_rp_pipeline")
def _rp_pipeline(suffix: str):
    left = _rp_make_left(suffix=suffix)
    right = _rp_make_right(suffix=suffix)
    return _rp_add(left=left, right=right)


async def _assert_completed(job_id: int) -> None:
    async with get_sql_session() as session:
        db_job = (
            await session.execute(select(Job).where(Job.id == job_id))
        ).scalar_one()
    assert db_job.status == JobStatus.COMPLETED, f"Job failed: {db_job.error}"


async def test_replay_job_end_to_end(orch_ctx):
    """Full lifecycle: run pipeline → replay under STRATEGY → verify lineage."""
    suffix = str(get_snowflake_id())
    left_table = f"p_rp_left_{suffix}"
    right_table = f"p_rp_right_{suffix}"

    original = await _rp_pipeline(suffix=suffix)
    await run_job_tasks(original)
    await _assert_completed(original.id)

    strategy = {left_table: "value = 10"}
    result = await replay_job(original.id, sampling_strategy=strategy)
    replayed = result.job

    assert replayed.name == original.name  # inherited
    assert replayed.id != original.id
    assert replayed.preservation_mode == PreservationMode.STRATEGY
    assert replayed.sampling_strategy == strategy

    # Exactly one compute task (the _rp_add clone) should be present.
    async with get_sql_session() as session:
        cloned_tasks = list(
            (
                await session.execute(select(Task).where(Task.job_id == replayed.id))
            ).scalars().all()
        )
    assert len(cloned_tasks) == 1
    clone = cloned_tasks[0]
    assert clone.entrypoint.endswith("_rp_add")
    assert clone.kwargs["left"] == {
        "object_type": "object",
        "table": left_table,
        "persistent": True,
    }
    assert clone.kwargs["right"] == {
        "object_type": "object",
        "table": right_table,
        "persistent": True,
    }

    ch = await create_ch_client()
    try:
        # Re-execute the replay and verify the compute task actually runs
        # against the persistent input tables without re-fetching them.
        await run_job_tasks(replayed)
        await _assert_completed(replayed.id)

        # The replayed job's oplog should now carry kwargs_aai_ids /
        # result_aai_ids populated by STRATEGY-mode sampling.
        rows = (
            await ch.query(
                "SELECT operation, kwargs_aai_ids, result_aai_ids "
                f"FROM operation_log WHERE job_id = {replayed.id}"
            )
        ).result_rows
        assert rows, f"No oplog rows for replayed job {replayed.id}"

        add_rows = [(op, k, r) for op, k, r in rows if op == "+"]
        assert add_rows, f"No '+' oplog row; got {[r[0] for r in rows]}"
        _, kwargs_aai_ids_raw, result_aai_ids_raw = add_rows[0]
        kwargs_aai_ids = (
            dict(kwargs_aai_ids_raw)
            if not isinstance(kwargs_aai_ids_raw, dict)
            else kwargs_aai_ids_raw
        )
        result_aai_ids = list(result_aai_ids_raw)
        assert set(kwargs_aai_ids.keys()) == {"left", "right"}
        assert len(result_aai_ids) == 1
    finally:
        await ch.command(f"DROP TABLE IF EXISTS {left_table}")
        await ch.command(f"DROP TABLE IF EXISTS {right_table}")
