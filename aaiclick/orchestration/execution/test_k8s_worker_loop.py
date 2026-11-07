"""The concurrent kubernetes worker loop, against a fake cluster: the watch is
driven by hand, ``kubectl`` is stubbed, host tasks are a stub ExecuteFn."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import update
from sqlmodel import col, select

from aaiclick.data.object.refs import native_ref

from ...datetime_utils import utc_now
from ...testing import get_task_runs, until
from ..factories import create_job, create_task
from ..jobs.queries import get_task
from ..kubernetes_config import K8sLimits
from ..models import (
    TASK_COMPLETED,
    TASK_PENDING,
    TASK_PENDING_CANCELLED_CLEANUP,
    TASK_PENDING_FAILURE_CLEANUP,
    TASK_RUNNING,
    ExecutionWorker,
    Task,
    TaskRun,
)
from ..orch_context import commit_tasks, get_sql_session
from ..runner_config import ENTRY_SHELL
from . import cli
from . import execution_worker as ew
from . import k8s_worker_loop as loop_mod
from . import kubernetes_worker as kw
from .claiming import cancel_job, claim_next_task, clear_task, update_task_status
from .execution_worker import deregister_execution_worker, register_execution_worker, request_execution_worker_stop
from .k8s_worker_loop import k8s_worker_main_loop
from .pod_watch import PodWatch, pod_labels
from .runner import register_run

SIMPLE = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"
LIMITS = K8sLimits(max_pending=2, max_pending_total=10, max_pods=None, pending_timeout=900.0)

pytestmark = pytest.mark.usefixtures("fast_poll")


_DETACH_AFTER_INTERVALS = loop_mod.DETACH_AFTER_INTERVALS


@pytest.fixture
def outage(cluster, monkeypatch):
    """The real detach threshold, for the tests that exercise a heartbeat outage."""
    monkeypatch.setattr(loop_mod, "DETACH_AFTER_INTERVALS", _DETACH_AFTER_INTERVALS)


async def _task(task_id: int) -> Task:
    """``get_task`` for a task the test created: never None."""
    task = await get_task(task_id)
    assert task is not None
    return task


def _pod_json(task: Task, task_state: dict, *, node: str | None = "n1") -> dict:
    spec: dict = {"containers": [{"name": "task"}]}
    if node:
        spec["nodeName"] = node
    status: dict = {"phase": "Running", "containerStatuses": [{"name": "task", "state": task_state}]}
    if node is None:
        status["conditions"] = [
            {"type": "PodScheduled", "status": "False", "reason": "Unschedulable", "message": "0/1 nodes: cpu"}
        ]
    name = kw._pod_name(task.id, task.run_epoch)
    return {
        "metadata": {"name": name, "labels": pod_labels(task.id, task.job_id, task.run_epoch)},
        "spec": spec,
        "status": status,
    }


class _FakeWatch(PodWatch):
    """A ``PodWatch`` that never talks to kubectl; tests feed ``apply``. Pods
    in ``preload`` appear on the first resync, as a restarted worker sees them."""

    preload: list[dict] = []

    async def resync(self) -> None:
        for obj in self.preload:
            self.apply(obj, deleted=False)
        self.preload = []

    async def run(self) -> None:
        await asyncio.Event().wait()


@pytest.fixture
def cluster(monkeypatch):
    """Fake cluster + worker wiring for ``k8s_worker_main_loop``."""
    monkeypatch.setenv("AAICLICK_RUNNER", "kubernetes")
    monkeypatch.setattr(loop_mod, "is_chdb", lambda: False)
    monkeypatch.setattr(loop_mod, "resolve_k8s_limits", lambda: state.limits)
    monkeypatch.setattr(loop_mod, "resolve_launch_image", AsyncMock(return_value="img:1"))
    monkeypatch.setattr(kw, "build_runner_env", lambda: {"X": "1"})
    monkeypatch.setattr(kw, "read_task_run_result", AsyncMock(return_value=kw.RunnerResult(True, native_ref(1), None)))
    monkeypatch.setattr(kw, "follow_vehicle_output", AsyncMock())
    monkeypatch.setattr(kw, "last_task_log_time", AsyncMock(return_value=None))
    # 1 s heartbeats keep the outage tests short. A loaded SQLite under xdist
    # can still stall past the real detach threshold, so only the ``outage``
    # tests run with it; everything else must never see a false detach.
    monkeypatch.setattr(ew, "HEARTBEAT_INTERVAL", 1.0)
    monkeypatch.setattr(loop_mod, "DETACH_AFTER_INTERVALS", float("inf"))

    created: list[dict] = []
    deleted: list[str] = []
    watches: list[PodWatch] = []

    async def create(manifest: dict, verb: str = "create") -> dict:
        if state.quota_error and manifest["kind"] == "Pod":
            raise cli.CommandError(("kubectl",), 1, "pods is forbidden: exceeded quota: compute")
        created.append(manifest)
        return {**manifest, "metadata": {**manifest["metadata"], "uid": "u"}}

    async def delete(name: str, namespace: str) -> None:
        deleted.append(name)

    def make_watch(namespace: str, kubectl: str) -> PodWatch:
        watch = _FakeWatch(namespace, kubectl)
        watch.preload = list(state.preload)
        watches.append(watch)
        return watch

    cli_calls: list[list[str]] = []

    async def fake_cli(*argv, check=True, stream=True, cwd=None, env=None):
        cli_calls.append(list(argv))
        if argv[1] == "exec":
            return 0, '{"_aaiclick_type": "native", "value": {"k": "v"}}', ""
        return 0, "", ""

    monkeypatch.setattr(kw.cli, "run", fake_cli)

    monkeypatch.setattr(kw, "_kubectl_create_from_file", create)
    monkeypatch.setattr(kw, "_kubectl_delete", delete)
    monkeypatch.setattr(loop_mod, "PodWatch", make_watch)

    def pods() -> list[dict]:
        return [m for m in created if m["kind"] == "Pod"]

    def start(worker_id: int | None = None, **kwargs) -> asyncio.Task:
        kwargs.setdefault("install_signal_handlers", False)
        kwargs.setdefault("max_empty_polls", 4)
        return asyncio.create_task(k8s_worker_main_loop(worker_id, **kwargs))

    state = SimpleNamespace(
        limits=LIMITS,
        quota_error=False,
        preload=[],
        created=created,
        deleted=deleted,
        cli=cli_calls,
        pods=pods,
        start=start,
        watches=watches,
    )
    state.watch = lambda: watches[-1]
    return state


async def _image_job(name: str, n: int = 1) -> list[Task]:
    tasks = []
    for i in range(n):
        job = await create_job(f"{name}_{i}", create_task(SIMPLE, image="img:1"))
        tasks.append((await _job_tasks(job.id))[0])
    return tasks


async def _job_tasks(job_id: int) -> list[Task]:
    async with get_sql_session() as session:
        return list((await session.execute(select(Task).where(col(Task.job_id) == job_id))).scalars().all())


async def test_runs_tasks_concurrently(orch_ctx, cluster):
    tasks = await _image_job("concurrent", 3)
    loop = cluster.start()

    await until(lambda: len(cluster.pods()) == 3)
    for t in tasks:
        cluster.watch().apply(_pod_json(t, {"terminated": {"exitCode": 0}}), deleted=False)

    assert await loop == 3
    assert {(await _task(t.id)).status for t in tasks} == {TASK_COMPLETED}
    assert sorted(cluster.deleted) == sorted(kw._pod_name(t.id, t.run_epoch) for t in tasks)


async def test_gate_blocks_saturated_job_only(orch_ctx, cluster):
    """Job A has two unscheduled Pods (its cap); job B still gets claimed."""
    job_a = await create_job("gate_a", create_task(SIMPLE, image="img:1"))
    await commit_tasks([create_task(SIMPLE, image="img:1") for _ in range(2)], job_a.id)
    (task_b,) = await _image_job("gate_b")
    a_tasks = await _job_tasks(job_a.id)
    loop = cluster.start()

    await until(lambda: len(cluster.pods()) >= 2)
    first_two = [
        t for t in a_tasks if kw._pod_name(t.id, t.run_epoch) in {m["metadata"]["name"] for m in cluster.pods()}
    ]
    for t in first_two:
        cluster.watch().apply(_pod_json(t, {"waiting": {}}, node=None), deleted=False)
    await until(lambda: any(m["metadata"]["name"] == kw._pod_name(task_b.id, task_b.run_epoch) for m in cluster.pods()))

    third = [t for t in a_tasks if t not in first_two]
    assert all(kw._pod_name(t.id, t.run_epoch) not in {m["metadata"]["name"] for m in cluster.pods()} for t in third)

    for t in [*a_tasks, task_b]:
        cluster.watch().apply(_pod_json(t, {"terminated": {"exitCode": 0}}), deleted=False)
    await until(lambda: len(cluster.pods()) == 4)
    for t in third:
        cluster.watch().apply(_pod_json(t, {"terminated": {"exitCode": 0}}), deleted=False)
    assert await loop == 4


async def test_max_pods_stops_claims(orch_ctx, cluster):
    cluster.limits = LIMITS._replace(max_pods=1)
    t1, t2 = await _image_job("maxpods", 2)
    loop = cluster.start()

    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(_pod_json(t1, {"running": {}}), deleted=False)
    await asyncio.sleep(0.6)
    assert len(cluster.pods()) == 1

    cluster.watch().apply(_pod_json(t1, {"terminated": {"exitCode": 0}}), deleted=False)
    await until(lambda: len(cluster.pods()) == 2)
    cluster.watch().apply(_pod_json(t2, {"terminated": {"exitCode": 0}}), deleted=False)
    assert await loop == 2


async def test_quota_backoff_pauses_claims(orch_ctx, cluster, monkeypatch):
    monkeypatch.setattr(loop_mod, "QUOTA_BACKOFF", 0.8)
    (task,) = await _image_job("quota")
    cluster.quota_error = True
    loop = cluster.start(max_empty_polls=20)

    async def epoch_bumped() -> bool:
        return (await _task(task.id)).run_epoch > task.run_epoch

    await until(epoch_bumped)
    after = await _task(task.id)
    assert after.status == TASK_PENDING and after.run_epoch == task.run_epoch + 1 and after.attempt == task.attempt
    cluster.quota_error = False
    await asyncio.sleep(0.3)
    assert cluster.pods() == []  # still backing off

    await until(lambda: len(cluster.pods()) == 1, timeout=3.0)
    cluster.watch().apply(_pod_json(after, {"terminated": {"exitCode": 0}}), deleted=False)
    assert await loop == 1


async def test_cancel_pending_pod(orch_ctx, cluster):
    (task,) = await _image_job("cancel_pending")
    loop = cluster.start()
    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(_pod_json(task, {"waiting": {}}, node=None), deleted=False)

    await cancel_job(task.job_id)

    await until(lambda: cluster.deleted == [kw._pod_name(task.id, task.run_epoch)])
    await loop
    after = await _task(task.id)
    assert after.status == TASK_PENDING_CANCELLED_CLEANUP and after.execution_worker_id is None


async def _age_claim(task_id: int, seconds: float) -> None:
    async with get_sql_session() as session:
        await session.execute(
            update(Task).where(col(Task.id) == task_id).values(started_at=utc_now() - timedelta(seconds=seconds))
        )
        await session.commit()


async def test_pending_timeout_counts_from_claim(orch_ctx, cluster):
    cluster.limits = LIMITS._replace(pending_timeout=5.0)
    (task,) = await _image_job("pending_timeout")
    loop = cluster.start()
    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(
        _pod_json(task, {"waiting": {"reason": "ImagePullBackOff", "message": "back-off"}}), deleted=False
    )
    await _age_claim(task.id, 20)

    await loop

    after = await _task(task.id)
    assert after.status == TASK_PENDING_FAILURE_CLEANUP
    assert "not started after 5s" in (after.error or "") and "ImagePullBackOff: back-off" in (after.error or "")
    assert cluster.deleted == [kw._pod_name(task.id, task.run_epoch)]


async def test_running_pod_without_run_row_hits_pending_timeout(orch_ctx, cluster):
    cluster.limits = LIMITS._replace(pending_timeout=5.0)
    (task,) = await _image_job("never_registered")
    loop = cluster.start()
    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(_pod_json(task, {"running": {}}), deleted=False)
    await _age_claim(task.id, 20)

    await loop

    assert "the task never started" in ((await _task(task.id)).error or "")


async def test_task_timeout_counts_from_run_start(orch_ctx, cluster, monkeypatch):
    monkeypatch.setenv("AAICLICK_TASK_TIMEOUT", "5")
    (task,) = await _image_job("task_timeout")
    loop = cluster.start()
    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(_pod_json(task, {"running": {}}), deleted=False)
    run_id = await register_run(task.id)
    async with get_sql_session() as session:
        await session.execute(
            update(TaskRun).where(col(TaskRun.run_id) == run_id).values(started_at=utc_now() - timedelta(seconds=20))
        )
        await session.commit()

    await loop

    after = await _task(task.id)
    assert after.status == TASK_PENDING_FAILURE_CLEANUP and after.error == "Task timed out after 5.0s"


async def test_lost_ownership_by_adoption_keeps_pod(orch_ctx, cluster):
    """Another worker took the run (same epoch): this one detaches and never deletes."""
    (task,) = await _image_job("adopted_away")
    other = await register_execution_worker()
    loop = cluster.start()
    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(_pod_json(task, {"running": {}}), deleted=False)
    async with get_sql_session() as session:
        await session.execute(update(Task).where(col(Task.id) == task.id).values(execution_worker_id=other.id))
        await session.commit()

    await loop

    assert cluster.deleted == []
    assert (await _task(task.id)).status == TASK_RUNNING


async def test_heartbeat_outage_detaches_all(orch_ctx, outage, cluster, monkeypatch):
    (task,) = await _image_job("hb_outage")
    loop = cluster.start(max_empty_polls=100)
    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(_pod_json(task, {"running": {}}), deleted=False)

    monkeypatch.setattr(loop_mod, "execution_worker_heartbeat", AsyncMock(side_effect=RuntimeError("db down")))
    await asyncio.sleep(2 * ew.HEARTBEAT_INTERVAL)  # > 1.5 intervals of silence

    assert cluster.deleted == []
    assert (await _task(task.id)).status == TASK_RUNNING
    loop.cancel()
    with pytest.raises(asyncio.CancelledError):
        await loop


async def test_shutdown_leaves_pods_running(orch_ctx, cluster):
    worker = await register_execution_worker()
    (task,) = await _image_job("shutdown")
    loop = cluster.start(worker.id, max_empty_polls=100)
    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(_pod_json(task, {"running": {}}), deleted=False)

    await request_execution_worker_stop(worker.id)
    await loop

    assert cluster.deleted == []
    after = await _task(task.id)
    assert after.status == TASK_RUNNING and after.execution_worker_id == worker.id
    async with get_sql_session() as session:
        stopped = await session.get(ExecutionWorker, worker.id)
        assert stopped is not None and stopped.status == "STOPPED"


async def test_one_host_task_at_a_time(orch_ctx, cluster, monkeypatch):
    running = {"now": 0, "peak": 0}

    async def fake_host(task: Task, execution_worker_id: int):
        running["now"] += 1
        running["peak"] = max(running["peak"], running["now"])
        await asyncio.sleep(0.3)
        running["now"] -= 1
        return True, None, None

    monkeypatch.setattr(loop_mod, "dispatch_execute", fake_host)
    for i in range(3):
        await create_job(f"host_{i}", SIMPLE)

    assert await cluster.start() == 3
    assert running["peak"] == 1


# ---------------------------------------------------------------------------
# Restart reclaim and the stale-Pod sweep
# ---------------------------------------------------------------------------


def _shell_pod_json(task: Task, io_in: dict, task_state: dict) -> dict:
    pod = _pod_json(task, task_state)
    pod["status"]["initContainerStatuses"] = [{"name": "io-in", "state": io_in}]
    return pod


async def _claimed_by_dead_worker(name: str, *, shell: bool = False) -> tuple[Task, ExecutionWorker]:
    """A task claimed by a worker that then stopped — what a restarted worker finds."""
    old = await register_execution_worker()
    if shell:
        await create_job(name, create_task(entry_type=ENTRY_SHELL, command=["sh", "-c", "x"], image="img:1"))
    else:
        await create_job(name, create_task(SIMPLE, image="img:1"))
    task = await claim_next_task(old.id)
    assert task is not None
    await deregister_execution_worker(old.id)
    return task, old


async def test_reclaim_adopts_running_module_pod_from_dead_worker(orch_ctx, cluster):
    task, old = await _claimed_by_dead_worker("reclaim_running")
    cluster.preload = [_pod_json(task, {"running": {}})]
    new = await register_execution_worker()
    loop = cluster.start(new.id)

    await until(lambda: _tracking(cluster, task))
    assert cluster.pods() == []
    assert (await _task(task.id)).execution_worker_id == new.id
    cluster.watch().apply(_pod_json(task, {"terminated": {"exitCode": 0}}), deleted=False)

    assert await loop == 1
    assert (await _task(task.id)).status == TASK_COMPLETED
    assert cluster.deleted == [kw._pod_name(task.id, task.run_epoch)]


async def test_reclaim_finishes_done_module_pod(orch_ctx, cluster):
    task, _ = await _claimed_by_dead_worker("reclaim_done")
    cluster.preload = [_pod_json(task, {"terminated": {"exitCode": 0}})]

    assert await cluster.start() == 1
    assert (await _task(task.id)).status == TASK_COMPLETED
    assert cluster.pods() == [] and cluster.deleted == [kw._pod_name(task.id, task.run_epoch)]


async def test_reclaim_deletes_stale_epoch_pod(orch_ctx, cluster):
    task, _ = await _claimed_by_dead_worker("reclaim_stale")
    stale_pod = _pod_json(task, {"running": {}})
    await clear_task(task.id)  # epoch moves on; the old Pod is now stale
    cluster.preload = [stale_pod]
    loop = cluster.start()

    await until(lambda: cluster.deleted == [kw._pod_name(task.id, task.run_epoch)])
    fresh = await _task(task.id)
    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(_pod_json(fresh, {"terminated": {"exitCode": 0}}), deleted=False)
    assert await loop == 1


async def test_reclaim_deletes_pod_of_terminal_task(orch_ctx, cluster):
    task, _ = await _claimed_by_dead_worker("reclaim_terminal")
    await update_task_status(task.id, TASK_COMPLETED, expected_epoch=task.run_epoch)
    cluster.preload = [_pod_json(task, {"running": {}})]

    assert await cluster.start() == 0
    assert cluster.deleted == [kw._pod_name(task.id, task.run_epoch)]


async def test_reclaim_cancelling_task_pod_deleted_and_released(orch_ctx, cluster):
    task, old = await _claimed_by_dead_worker("reclaim_cancelling")
    await cancel_job(task.job_id)
    cluster.preload = [_pod_json(task, {"running": {}})]

    assert await cluster.start() == 0

    assert cluster.deleted == [kw._pod_name(task.id, task.run_epoch)]
    after = await _task(task.id)
    assert after.status == TASK_PENDING_CANCELLED_CLEANUP and after.execution_worker_id is None


async def test_reclaim_ignores_live_owner_pods(orch_ctx, cluster):
    live = await register_execution_worker()
    await create_job("reclaim_live", create_task(SIMPLE, image="img:1"))
    task = await claim_next_task(live.id)
    assert task is not None
    cluster.preload = [_pod_json(task, {"running": {}})]

    assert await cluster.start() == 0

    assert cluster.deleted == [] and cluster.pods() == []
    assert (await _task(task.id)).execution_worker_id == live.id


async def test_reclaim_shell_pod_at_io_in_restages_input(orch_ctx, cluster):  # Review Focus 5
    task, _ = await _claimed_by_dead_worker("reclaim_shell_io_in", shell=True)
    cluster.preload = [_shell_pod_json(task, {"running": {}}, {"waiting": {}})]
    loop = cluster.start()

    await until(lambda: [c[1] for c in cluster.cli if c[1] == "cp"] == ["cp", "cp"])
    assert cluster.pods() == []
    cluster.watch().apply(_shell_pod_json(task, {"terminated": {"exitCode": 0}}, {"running": {}}), deleted=False)
    await until(lambda: kw.follow_vehicle_output.await_count == 1)
    cluster.watch().apply(
        _shell_pod_json(task, {"terminated": {"exitCode": 0}}, {"terminated": {"exitCode": 0}}), deleted=False
    )

    assert await loop == 1
    after = await _task(task.id)
    assert after.status == TASK_COMPLETED and after.result == native_ref({"k": "v"})


async def test_reclaim_shell_pod_at_task_running_resumes_logs(orch_ctx, cluster, monkeypatch):
    task, _ = await _claimed_by_dead_worker("reclaim_shell_running", shell=True)
    run_id = await register_run(task.id)
    since = utc_now() - timedelta(seconds=30)
    monkeypatch.setattr(kw, "last_task_log_time", AsyncMock(return_value=since))
    cluster.preload = [_shell_pod_json(task, {"terminated": {"exitCode": 0}}, {"running": {}})]
    loop = cluster.start()

    await until(lambda: kw.follow_vehicle_output.await_count == 1)
    argv, _, _, followed_run = kw.follow_vehicle_output.await_args.args
    assert followed_run == run_id and any(a.startswith("--since-time=") for a in argv)
    assert [c[1] for c in cluster.cli if c[1] == "cp"] == []
    cluster.watch().apply(
        _shell_pod_json(task, {"terminated": {"exitCode": 0}}, {"terminated": {"exitCode": 0}}), deleted=False
    )

    assert await loop == 1
    assert len(await get_task_runs(task.id)) == 1


async def test_adopted_pod_keeps_original_deadlines(orch_ctx, cluster):
    cluster.limits = LIMITS._replace(pending_timeout=5.0)
    task, _ = await _claimed_by_dead_worker("reclaim_deadline")
    await _age_claim(task.id, 20)
    cluster.preload = [_pod_json(task, {"running": {}})]

    await cluster.start()

    after = await _task(task.id)
    assert after.status == TASK_PENDING_FAILURE_CLEANUP and "the task never started" in (after.error or "")


def _tracking(cluster, task: Task) -> bool:
    """Whether the loop has a coroutine waiting on the task's Pod."""
    return bool(cluster.watches) and kw._pod_name(task.id, task.run_epoch) in cluster.watch()._futures


async def test_heartbeat_recovery_reattaches_own_pods(orch_ctx, outage, cluster, monkeypatch):
    """A bounded worker (``max_tasks``) must not exit while detached: its
    in-flight set is empty, but it still owns the Pod it will take back."""
    (task,) = await _image_job("hb_recovery")
    loop = cluster.start(max_empty_polls=100, max_tasks=1)
    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(_pod_json(task, {"running": {}}), deleted=False)

    real = loop_mod.execution_worker_heartbeat
    monkeypatch.setattr(loop_mod, "execution_worker_heartbeat", AsyncMock(side_effect=RuntimeError("db down")))
    await until(lambda: not _tracking(cluster, task))
    monkeypatch.setattr(loop_mod, "execution_worker_heartbeat", real)
    await until(lambda: _tracking(cluster, task))

    cluster.watch().apply(_pod_json(task, {"terminated": {"exitCode": 0}}), deleted=False)

    assert await loop == 1
    assert not _tracking(cluster, task)
    assert (await _task(task.id)).status == TASK_COMPLETED


async def test_sweep_deletes_reaped_task_pod(orch_ctx, cluster):
    """A Pod whose task was reaped meanwhile (claim expired: new epoch, no
    owner) is nobody's; the sweep removes it on the next heartbeat tick."""
    task, _ = await _claimed_by_dead_worker("sweep_reaped")
    async with get_sql_session() as session:
        await session.execute(
            update(Task)
            .where(col(Task.id) == task.id)
            .values(status=TASK_PENDING_FAILURE_CLEANUP, run_epoch=task.run_epoch + 1, execution_worker_id=None)
        )
        await session.commit()
    loop = cluster.start(max_empty_polls=100)
    await asyncio.sleep(0.1)
    cluster.watch().apply(_pod_json(task, {"running": {}}), deleted=False)

    await until(lambda: cluster.deleted == [kw._pod_name(task.id, task.run_epoch)], timeout=3.0)
    loop.cancel()
    with pytest.raises(asyncio.CancelledError):
        await loop


async def test_sweep_leaves_live_owner_pod(orch_ctx, cluster):
    live = await register_execution_worker()
    await create_job("sweep_live", create_task(SIMPLE, image="img:1"))
    task = await claim_next_task(live.id)
    loop = cluster.start(max_empty_polls=100)
    await asyncio.sleep(0.1)
    cluster.watch().apply(_pod_json(task, {"running": {}}), deleted=False)
    await asyncio.sleep(0.6)  # several heartbeat ticks

    assert cluster.deleted == []
    loop.cancel()
    with pytest.raises(asyncio.CancelledError):
        await loop


# ---------------------------------------------------------------------------
# Review fixes: single cancel, periodic resync, early detach, pre-Pod shutdown
# ---------------------------------------------------------------------------


async def test_deadline_cancel_is_delivered_once(orch_ctx, cluster, monkeypatch):
    """A slow Pod delete must not be interrupted by the next status tick's
    cancel: the deadline result still lands and the Pod is deleted once."""
    cluster.limits = LIMITS._replace(pending_timeout=5.0)

    async def slow_delete(name: str, namespace: str) -> None:
        await asyncio.sleep(1.5)  # > POLL_INTERVAL (0.5)
        cluster.deleted.append(name)

    monkeypatch.setattr(kw, "_kubectl_delete", slow_delete)
    (task,) = await _image_job("cancel_once")
    loop = cluster.start()
    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(_pod_json(task, {"waiting": {"reason": "ImagePullBackOff"}}), deleted=False)
    await _age_claim(task.id, 20)

    await loop

    after = await _task(task.id)
    assert after.status == TASK_PENDING_FAILURE_CLEANUP and "ImagePullBackOff" in (after.error or "")
    assert cluster.deleted == [kw._pod_name(task.id, task.run_epoch)]


async def test_heartbeat_tick_resyncs_watch(orch_ctx, cluster):
    """Events between the list and the watch stream's start would be lost;
    a resync per heartbeat tick is the backstop."""
    resyncs = {"n": 0}
    original = _FakeWatch.resync

    async def counting_resync(self) -> None:
        resyncs["n"] += 1
        await original(self)

    _FakeWatch.resync = counting_resync
    loop = cluster.start(max_empty_polls=100)
    try:
        await until(lambda: resyncs["n"] >= 3, timeout=6.0)
    finally:
        _FakeWatch.resync = original
        loop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop


async def test_heartbeat_outage_detaches_before_adopt_after(orch_ctx, outage, cluster, monkeypatch):
    """Detach must come before another worker may adopt (ADOPT_AFTER =
    2 × HEARTBEAT_INTERVAL), even when the heartbeat call hangs."""

    async def hanging_heartbeat(worker_id: int):
        await asyncio.sleep(30)

    (task,) = await _image_job("hb_hang")
    loop = cluster.start(max_empty_polls=100)
    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(_pod_json(task, {"running": {}}), deleted=False)
    monkeypatch.setattr(loop_mod, "execution_worker_heartbeat", hanging_heartbeat)
    started = time.monotonic()

    await until(lambda: not _tracking(cluster, task))

    assert time.monotonic() - started < 2 * ew.HEARTBEAT_INTERVAL + 0.3  # detach lands before ADOPT_AFTER
    assert cluster.deleted == []
    loop.cancel()
    with pytest.raises(asyncio.CancelledError):
        await loop


async def test_shutdown_before_pod_created_releases_claim(orch_ctx, cluster, monkeypatch):
    """A rollout that lands between the claim and ``kubectl apply`` must cost
    no attempt: the claim goes back to the queue instead of waiting for expiry."""
    gate = asyncio.Event()

    async def blocked_create(manifest: dict, verb: str = "create") -> dict:
        await gate.wait()
        return {**manifest, "metadata": {**manifest["metadata"], "uid": "u"}}

    monkeypatch.setattr(kw, "_kubectl_create_from_file", blocked_create)
    worker = await register_execution_worker()
    (task,) = await _image_job("shutdown_pre_pod")
    loop = cluster.start(worker.id, max_empty_polls=100)

    async def claimed() -> bool:
        return (await _task(task.id)).status == TASK_RUNNING

    await until(claimed)
    await request_execution_worker_stop(worker.id)
    await loop

    after = await _task(task.id)
    assert (after.status, after.run_epoch, after.attempt) == (TASK_PENDING, task.run_epoch + 1, task.attempt)
    # launch's own cleanup removes the half-created Pod; nothing else runs.
    assert cluster.pods() == []


async def test_slow_heartbeat_is_not_cancelled(orch_ctx, outage, cluster, monkeypatch):
    """A heartbeat slower than the retry cadence (a loaded DB) is left to
    finish — cancelling it mid-UPDATE would hold locks and read as an outage."""
    outcomes = {"finished": 0, "cancelled": 0}
    real = loop_mod.execution_worker_heartbeat

    async def slow_heartbeat(worker_id: int):
        try:
            await asyncio.sleep(ew.HEARTBEAT_INTERVAL / 2)  # > retry cadence, < detach threshold
        except asyncio.CancelledError:
            outcomes["cancelled"] += 1
            raise
        outcomes["finished"] += 1
        return await real(worker_id)

    monkeypatch.setattr(loop_mod, "execution_worker_heartbeat", slow_heartbeat)
    (task,) = await _image_job("slow_hb")
    loop = cluster.start(max_empty_polls=100)
    await until(lambda: len(cluster.pods()) == 1)
    cluster.watch().apply(_pod_json(task, {"running": {}}), deleted=False)
    await asyncio.sleep(4 * ew.HEARTBEAT_INTERVAL)

    assert outcomes["cancelled"] == 0 and outcomes["finished"] >= 2
    assert _tracking(cluster, task)  # never detached
    loop.cancel()
    with pytest.raises(asyncio.CancelledError):
        await loop
