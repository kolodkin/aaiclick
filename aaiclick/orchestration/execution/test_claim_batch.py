"""Batched claim operations the kubernetes reconciler uses: one statement per
tick for every in-flight run, fenced adoption, and claim release."""

from __future__ import annotations

from datetime import timedelta

from sqlalchemy import update
from sqlmodel import col, select

from ...datetime_utils import utc_now
from ...testing import get_task_runs
from ..factories import create_job
from ..jobs.queries import get_task
from ..models import TASK_CANCELLED, TASK_PENDING, TASK_RUNNING, ExecutionWorker, Task
from ..orch_context import get_sql_session
from .claiming import (
    RunKey,
    adopt_run,
    cancel_job,
    claim_next_task,
    clear_task,
    in_flight_status,
    release_claim,
    renew_claims,
)
from .execution_worker import deregister_execution_worker, register_execution_worker
from .runner import register_run

SIMPLE = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"


async def _claimed(worker_id: int, name: str) -> Task:
    await create_job(name, SIMPLE)
    task = await claim_next_task(worker_id)
    assert task is not None
    return task


def _key(task: Task) -> RunKey:
    return RunKey(task.id, task.run_epoch)


async def _age_heartbeat(worker_id: int, seconds: float) -> None:
    async with get_sql_session() as session:
        await session.execute(
            update(ExecutionWorker)
            .where(col(ExecutionWorker.id) == worker_id)
            .values(last_heartbeat=utc_now() - timedelta(seconds=seconds))
        )
        await session.commit()


async def test_renew_claims_only_owned_epochs(orch_ctx):
    w = await register_execution_worker()
    t1 = await _claimed(w.id, "renew_a")
    t2 = await _claimed(w.id, "renew_b")

    renewed = await renew_claims(w.id, [_key(t1), RunKey(t2.id, t2.run_epoch + 1)])

    assert renewed == {t1.id}


async def test_renew_claims_keeps_claim_fresh_across_ticks(orch_ctx):
    w = await register_execution_worker()
    t = await _claimed(w.id, "renew_fresh")
    first = (await get_task(t.id)).claim_expires_at
    async with get_sql_session() as session:
        await session.execute(
            update(Task).where(col(Task.id) == t.id).values(claim_expires_at=first - timedelta(minutes=5))
        )
        await session.commit()

    await renew_claims(w.id, [_key(t)])

    assert (await get_task(t.id)).claim_expires_at >= first


async def test_in_flight_status_flags_cancelled_and_cleared(orch_ctx):
    w = await register_execution_worker()
    live = await _claimed(w.id, "status_live")
    cancelled = await _claimed(w.id, "status_cancelled")
    cleared = await _claimed(w.id, "status_cleared")
    await cancel_job(cancelled.job_id)
    await clear_task(cleared.id)

    statuses = await in_flight_status([_key(live), _key(cancelled), _key(cleared), RunKey(999_999, 0)])

    assert {s.task_id: s.aborted for s in statuses} == {
        live.id: False,
        cancelled.id: True,
        cleared.id: True,
        999_999: False,
    }


async def test_in_flight_status_reports_run_start(orch_ctx):
    w = await register_execution_worker()
    started = await _claimed(w.id, "status_started")
    pending = await _claimed(w.id, "status_pending")
    await register_run(started.id)

    by_id = {s.task_id: s for s in await in_flight_status([_key(started), _key(pending)])}

    assert by_id[started.id].run_started_at is not None
    assert by_id[pending.id].run_started_at is None


async def test_adopt_run_from_stopped_worker(orch_ctx):
    old = await register_execution_worker()
    new = await register_execution_worker()
    t = await _claimed(old.id, "adopt_stopped")
    await deregister_execution_worker(old.id)

    assert await adopt_run(_key(t), old.id, new.id)
    assert (await get_task(t.id)).execution_worker_id == new.id


async def test_adopt_run_refuses_live_owner(orch_ctx):
    old = await register_execution_worker()
    new = await register_execution_worker()
    t = await _claimed(old.id, "adopt_live")

    assert not await adopt_run(_key(t), old.id, new.id)
    assert (await get_task(t.id)).execution_worker_id == old.id


async def test_adopt_run_from_silent_worker(orch_ctx):
    old = await register_execution_worker()
    new = await register_execution_worker()
    t = await _claimed(old.id, "adopt_silent")
    await _age_heartbeat(old.id, 120)

    assert await adopt_run(_key(t), old.id, new.id)


async def test_adopt_run_fenced_on_previous_owner(orch_ctx):
    old = await register_execution_worker()
    first = await register_execution_worker()
    second = await register_execution_worker()
    t = await _claimed(old.id, "adopt_fenced")
    await deregister_execution_worker(old.id)

    assert await adopt_run(_key(t), old.id, first.id)
    assert not await adopt_run(_key(t), old.id, second.id)
    assert (await get_task(t.id)).execution_worker_id == first.id


async def test_adopt_run_refuses_stale_epoch(orch_ctx):
    old = await register_execution_worker()
    new = await register_execution_worker()
    t = await _claimed(old.id, "adopt_epoch")
    await deregister_execution_worker(old.id)

    assert not await adopt_run(RunKey(t.id, t.run_epoch + 1), old.id, new.id)


async def test_release_claim_returns_task_to_pending(orch_ctx):
    w = await register_execution_worker()
    t = await _claimed(w.id, "release_pending")

    assert await release_claim(_key(t))

    after = await get_task(t.id)
    assert (after.status, after.run_epoch, after.execution_worker_id, after.started_at, after.attempt) == (
        TASK_PENDING,
        t.run_epoch + 1,
        None,
        None,
        t.attempt,
    )
    assert not await release_claim(_key(t))


async def test_release_claim_closes_host_run_row(orch_ctx):
    w = await register_execution_worker()
    t = await _claimed(w.id, "release_run_row")
    await register_run(t.id)

    assert await release_claim(_key(t))

    runs = await get_task_runs(t.id)
    assert [r.status for r in runs] == [TASK_CANCELLED]
    assert (await get_task(t.id)).status != TASK_RUNNING
    async with get_sql_session() as session:
        assert (await session.execute(select(Task.id).where(col(Task.id) == t.id))).scalar_one() == t.id
