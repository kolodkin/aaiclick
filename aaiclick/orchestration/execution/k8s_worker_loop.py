"""The concurrent kubernetes worker: a reconciler over the namespace's Pods.

See docs/designs/kubernetes_runner.md.

One ``PodWatch`` keeps a Pod-state map current; a dispatcher claims against the
capacity gate computed from that map and starts one coroutine per task;
heartbeat and status are one batched timer each, not one per task. Module and
shell Pods run as :func:`run_module_pod` / :func:`run_shell_pod`; host tasks
(``image_source`` NULL — image builds, plain host tasks) keep the mp child via
``dispatch_execute``, one at a time.

Ownership: a Pod coroutine is **detached** (``detach[0] = True`` before its
cancel) whenever this worker lets go of a running Pod without finishing it —
shutdown, a heartbeat outage, or ownership lost to another worker's adoption.
A detached coroutine deletes nothing; the next worker adopts the Pod.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from contextlib import suppress
from datetime import timedelta
from typing import Literal, NamedTuple

from sqlmodel import col, select

from aaiclick.backend import is_chdb
from aaiclick.data.object_context.ch_client import ch_refused

from ...datetime_utils import utc_now
from ..kubernetes_config import K8sLimits, KubernetesConfig, resolve_k8s_limits, resolve_pod_config
from ..models import (
    CANCELLING_TASK_STATUSES,
    EXECUTION_WORKER_STOPPED,
    EXECUTION_WORKER_STOPPING,
    TASK_RUNNING,
    Task,
    TaskRun,
)
from ..orch_context import get_sql_session
from ..runner_config import ENTRY_MODULE
from . import execution_worker as ew
from . import kubernetes_worker as kw
from .claiming import (
    RunKey,
    adopt_run,
    claim_next_task,
    in_flight_status,
    owner_adoptable,
    release_cancelled_run,
    release_claim,
    renew_claims,
)
from .dispatch import dispatch_execute, resolve_dispatch
from .docker_build import resolve_launch_image
from .execution_worker import (
    WorkerStopped,
    _handle_task_result,
    deregister_execution_worker,
    execution_worker_heartbeat,
    parse_task_timeout,
    register_execution_worker,
)
from .kubernetes_worker import QuotaExceeded, _kubectl_bin, _pod_name, run_module_pod, run_shell_pod, shielded
from .pod_gate import gate
from .pod_watch import _NO_CONTAINER, PodState, PodWatch
from .runner_env import validate_worker_runner

logger = logging.getLogger(__name__)

# Pause on claiming after a ResourceQuota rejection.
QUOTA_BACKOFF = 30.0
# Heartbeat silence (as a multiple of HEARTBEAT_INTERVAL) after which the
# worker lets go of its Pods — before ADOPT_AFTER (2 ×), so another worker
# never adopts a run this one is still driving. While heartbeats fail they
# are retried at a quarter of the interval, each bounded by that quarter.
DETACH_AFTER_INTERVALS = 1.5


class InFlight(NamedTuple):
    """One task this worker is driving."""

    task: Task
    run: RunKey
    pod_name: str | None
    """None for a host task (mp child)."""
    aio: asyncio.Task
    detach: list[bool]
    """One-element flag the Pod coroutine reads when cancelled."""


class _Reconciler:
    def __init__(
        self,
        execution_worker_id: int,
        pod_config: KubernetesConfig,
        limits: K8sLimits,
        *,
        max_tasks: int | None,
        max_empty_polls: int | None,
    ) -> None:
        self.worker_id = execution_worker_id
        self.pod_config = pod_config
        self.limits = limits
        self.max_tasks = max_tasks
        self.max_empty_polls = max_empty_polls
        self.watch = PodWatch(pod_config.namespace, _kubectl_bin())
        self.in_flight: dict[int, InFlight] = {}
        self.wake = asyncio.Event()
        # Timers end on this event, never by cancellation: a cancel landing
        # inside a DB write would leave the transaction open (SQLite locks).
        self._stop = asyncio.Event()
        self.shutdown = False
        self.detached = False
        self.quota_pause_until = 0.0
        # A quota release waits out a claim in flight, which would re-claim the released task.
        self._claim_lock = asyncio.Lock()
        self.started = 0
        self.executed = 0
        # Pods whose delete was already requested; forgotten once the watch drops them.
        self._deleting: set[str] = set()
        # Task ids whose coroutine was cancelled: a cancel is delivered once —
        # a second one would land inside the coroutine's cleanup and drop its result.
        self._cancelled: set[int] = set()

    # -- dispatch -----------------------------------------------------------

    def _gate_states(self) -> list[PodState]:
        """Watch states plus a placeholder per Pod task whose Pod the watch has
        not reported yet, so a burst of claims within one tick is gated too."""
        states = list(self.watch.states.values())
        seen = set(self.watch.states)
        for item in self.in_flight.values():
            if item.pod_name and item.pod_name not in seen and not item.aio.done():
                states.append(
                    PodState(
                        item.pod_name,
                        item.task.job_id,
                        item.task.id,
                        item.run.run_epoch,
                        None,
                        "",
                        _NO_CONTAINER,
                        _NO_CONTAINER,
                        False,
                        None,
                    )
                )
        return states

    def _host_busy(self) -> bool:
        return any(item.pod_name is None and not item.aio.done() for item in self.in_flight.values())

    async def _dispatch_tick(self) -> bool:
        if self.shutdown or self.detached:
            return False
        claimed = False
        while self.max_tasks is None or self.started < self.max_tasks:
            # Re-read per claim: a task started earlier in this tick may have set the pause.
            if time.monotonic() < self.quota_pause_until:
                break
            decision = gate(self._gate_states(), self.limits)
            if not decision.open:
                break
            async with self._claim_lock:
                task = await claim_next_task(
                    self.worker_id, exclude_job_ids=decision.blocked_jobs, allow_host_tasks=not self._host_busy()
                )
            if task is None:
                break
            self._start(task)
            claimed = True
        return claimed

    def _start(self, task: Task, *, adopted: bool = False) -> InFlight:
        detach = [False]
        is_pod = task.image_source is not None
        aio = asyncio.create_task(self._execute(task, detach, adopted=adopted))
        item = InFlight(
            task, RunKey(task.id, task.run_epoch), _pod_name(task.id, task.run_epoch) if is_pod else None, aio, detach
        )
        self.in_flight[task.id] = item
        self.started += 1
        logger.info(
            "ExecutionWorker %s %s task %s: %s",
            self.worker_id,
            "adopted" if adopted else "executing",
            task.id,
            task.entrypoint,
        )
        return item

    async def _execute(self, task: Task, detach: list[bool], *, adopted: bool) -> None:
        try:
            try:
                success, result_ref, error = await self._run(task, detach, adopted=adopted)
            except QuotaExceeded as e:
                logger.warning("ExecutionWorker %s task %s refused by quota; releasing: %s", self.worker_id, task.id, e)
                # Pause first: the dispatcher must see it before the task is claimable again.
                self.quota_pause_until = time.monotonic() + QUOTA_BACKOFF
                await shielded(self._release_refused(RunKey(task.id, task.run_epoch)))
                return
            except WorkerStopped:
                self.shutdown = True
                return
            except asyncio.CancelledError:
                if detach[0]:
                    await self._release_if_no_pod(task)
                    return
                raise
            except Exception as e:
                logger.exception("ExecutionWorker %s task %s dispatch raised", self.worker_id, task.id)
                success, result_ref, error = False, None, f"{type(e).__name__}: {e}"
            if detach[0]:
                return  # ownership gone: the result is not ours to write
            if await shielded(_handle_task_result(task, self.worker_id, success, result_ref, error)):
                self.executed += 1
        finally:
            item = self.in_flight.pop(task.id, None)
            self._cancelled.discard(task.id)
            if item is not None and item.pod_name and not detach[0]:
                self._deleting.add(item.pod_name)
            self.wake.set()

    async def _release_refused(self, run: RunKey) -> None:
        async with self._claim_lock:
            await release_claim(run)

    async def _release_if_no_pod(self, task: Task) -> None:
        """A detached Pod task whose Pod never reached the cluster (shutdown
        between claim and ``kubectl apply``) goes back to the queue now, at no
        attempt, instead of waiting for claim expiry. Best-effort: during a DB
        outage the expiry path still covers it."""
        if _pod_name(task.id, task.run_epoch) in self.watch.states:
            return
        try:
            await shielded(release_claim(RunKey(task.id, task.run_epoch)))
        except Exception:
            logger.exception("ExecutionWorker %s could not release task %s", self.worker_id, task.id)

    async def _run(self, task: Task, detach: list[bool], *, adopted: bool) -> tuple[bool, dict | None, str | None]:
        dispatch = await resolve_dispatch(task)
        if dispatch.image_source is None:
            return await dispatch_execute(task, self.worker_id)
        image_tag = None if adopted else await resolve_launch_image(dispatch.image_source, task_id=task.id)
        if dispatch.entry_type == ENTRY_MODULE:
            result = await run_module_pod(
                task,
                self.worker_id,
                self.pod_config,
                self.watch,
                image_tag=image_tag,
                resources=dispatch.resources,
                detach=detach,
            )
        else:
            result = await run_shell_pod(
                task,
                self.worker_id,
                self.pod_config,
                self.watch,
                image_tag=image_tag,
                command=dispatch.command,
                command_env=dispatch.command_env,
                resources=dispatch.resources,
                run_id=await _running_run_id(task.id) if adopted else None,
                detach=detach,
            )
        return result.success, result.result_ref, result.error

    # -- reclaim / sweep ----------------------------------------------------

    async def _reconcile_pods(self, *, adopt: bool) -> None:
        """Settle every managed Pod this worker is not driving: on start (and
        after a heartbeat outage) with ``adopt=True`` — restart reclaim — and
        on each heartbeat tick with ``adopt=False`` — the stale-Pod sweep.

        | Task row                                        | Action                                     |
        |-------------------------------------------------|--------------------------------------------|
        | missing, epoch stale, or not RUNNING/cancelling | delete the Pod                             |
        | cancelling, owner gone                          | delete the Pod, ``release_cancelled_run``  |
        | RUNNING, epoch ok, owner gone (``adopt``)       | ``adopt_run`` → resume at the Pod's stage  |
        | RUNNING, owner alive                            | leave                                      |
        """
        self._deleting &= set(self.watch.states)
        states = [
            state
            for state in self.watch.states.values()
            if state.task_id not in self.in_flight and state.name not in self._deleting
        ]
        if not states:
            return
        tasks = await _load_tasks({state.task_id for state in states})
        for state in states:
            task = tasks.get(state.task_id)
            verdict = _classify(state, task)
            if verdict == "delete":
                await self._delete_pod(state.name)
            elif verdict == "release" and task is not None and await owner_adoptable(task.execution_worker_id):
                await self._delete_pod(state.name)
                await release_cancelled_run(task.id, expected_epoch=task.run_epoch)
            elif verdict == "adopt" and adopt and task is not None:
                mine = task.execution_worker_id == self.worker_id
                if mine or await adopt_run(RunKey(task.id, task.run_epoch), task.execution_worker_id, self.worker_id):
                    self._start(task, adopted=True)

    async def _delete_pod(self, name: str) -> None:
        self._deleting.add(name)
        await kw._kubectl_delete(name, self.pod_config.namespace)

    # -- timers -------------------------------------------------------------

    def _pod_items(self) -> list[InFlight]:
        return [item for item in self.in_flight.values() if item.pod_name and not item.aio.done()]

    def _cancel(self, item: InFlight, reason: str | None = None) -> None:
        if item.task.id in self._cancelled or item.aio.done():
            return
        self._cancelled.add(item.task.id)
        item.aio.cancel(reason)

    def _detach_all(self) -> None:
        for item in self._pod_items():
            item.detach[0] = True
            self._cancel(item)

    async def _pause(self, seconds: float) -> bool:
        """Sleep ``seconds`` unless stopped first; False once stopping."""
        if self._stop.is_set():
            return False
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            return True
        return False

    def _note_silence(self, last_ok: float) -> None:
        """Detach once the DB has been silent for DETACH_AFTER_INTERVALS heartbeats."""
        silent = time.monotonic() - last_ok
        if silent >= DETACH_AFTER_INTERVALS * ew.HEARTBEAT_INTERVAL and not self.detached:
            logger.warning(
                "ExecutionWorker %s lost the DB (%.0fs of silence); detaching from its Pods", self.worker_id, silent
            )
            self.detached = True
            self._detach_all()

    async def _heartbeat_loop(self) -> None:
        last_ok = time.monotonic()
        interval = ew.HEARTBEAT_INTERVAL
        # A heartbeat in flight is never cancelled: a cancel mid-UPDATE holds
        # DB locks and a slow DB would read as an outage. A slow call is left
        # to finish and checked again at the retry cadence.
        pending: asyncio.Future | None = None
        while await self._pause(interval):
            retry = ew.HEARTBEAT_INTERVAL / 4
            if pending is None:
                pending = asyncio.ensure_future(execution_worker_heartbeat(self.worker_id))
            done, _ = await asyncio.wait({pending}, timeout=retry)
            if not done:
                interval = retry
                self._note_silence(last_ok)
                continue
            finished, pending = pending, None
            try:
                status = finished.result()
            except Exception:
                logger.exception("ExecutionWorker %s heartbeat failed", self.worker_id)
                interval = retry
                self._note_silence(last_ok)
                continue
            last_ok = time.monotonic()
            interval = ew.HEARTBEAT_INTERVAL
            if self.detached:
                self.detached = False
                await self._reattach()
            if status in (EXECUTION_WORKER_STOPPING, EXECUTION_WORKER_STOPPED):
                logger.info("ExecutionWorker %s received stop request (%s)", self.worker_id, status)
                self.shutdown = True
                self.wake.set()
            await self._renew()
            try:
                # Backstop for events lost between the list and the watch stream's start.
                await self.watch.resync()
                await self._reconcile_pods(adopt=False)
            except Exception:
                logger.exception("ExecutionWorker %s stale-Pod sweep failed", self.worker_id)
        if pending is not None:
            with suppress(Exception):
                await pending

    async def _reattach(self) -> None:
        """The DB is back after an outage: re-list and take back our own Pods
        unless another worker adopted them meanwhile."""
        await self.watch.resync()
        await self._reconcile_pods(adopt=True)

    async def _renew(self) -> None:
        items = self._pod_items()
        if not items:
            return
        try:
            renewed = await renew_claims(self.worker_id, [item.run for item in items])
        except Exception:
            logger.exception("ExecutionWorker %s claim renewal failed", self.worker_id)
            return
        lost = [item for item in items if item.task.id not in renewed]
        if not lost:
            return
        statuses = {s.task_id: s for s in await in_flight_status([item.run for item in lost])}
        for item in lost:
            if statuses[item.task.id].aborted:
                self._cancel(item)  # cleared / reaped: the Pod goes
            else:
                logger.info(
                    "ExecutionWorker %s task %s adopted by another worker; detaching", self.worker_id, item.task.id
                )
                item.detach[0] = True
                self._cancel(item)

    async def _status_loop(self) -> None:
        while await self._pause(ew.POLL_INTERVAL):
            items = self._pod_items()
            if not items or self.detached:
                continue
            try:
                statuses = await in_flight_status([item.run for item in items])
            except Exception:
                logger.exception("ExecutionWorker %s status poll failed", self.worker_id)
                continue
            now = utc_now()
            task_timeout = parse_task_timeout()
            by_id = {item.task.id: item for item in items}
            for status in statuses:
                item = by_id.get(status.task_id)
                if item is None or item.aio.done():
                    continue
                if status.aborted:
                    self._cancel(item)
                elif status.run_started_at is None:
                    started_at = item.task.started_at
                    if started_at is not None and now > started_at + timedelta(seconds=self.limits.pending_timeout):
                        state = self.watch.states.get(item.pod_name or "")
                        reason = state.not_started_reason() if state else "Pod not created"
                        self._cancel(
                            item, f"Pod {item.pod_name} not started after {self.limits.pending_timeout:g}s: {reason}"
                        )
                elif task_timeout is not None and now > status.run_started_at + timedelta(seconds=task_timeout):
                    self._cancel(item, f"Task timed out after {task_timeout}s")

    # -- main loop ----------------------------------------------------------

    async def _wait_for_event(self) -> None:
        waiters = [asyncio.ensure_future(self.wake.wait()), asyncio.ensure_future(self.watch.changed.wait())]
        try:
            await asyncio.wait(waiters, timeout=ew.POLL_INTERVAL, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                waiter.cancel()
        self.wake.clear()
        self.watch.changed.clear()

    def _done(self, empty_polls: int) -> bool:
        if self.in_flight or (self.detached and not self.shutdown):  # detached Pods are still ours to take back
            return False
        if self.shutdown:
            return True
        if self.max_tasks is not None and self.started >= self.max_tasks:
            return True
        return self.max_empty_polls is not None and empty_polls >= self.max_empty_polls

    async def run(self) -> int:
        await self.watch.resync()
        await self._reconcile_pods(adopt=True)
        watch_task = asyncio.create_task(self.watch.run())
        timers = [asyncio.create_task(self._heartbeat_loop()), asyncio.create_task(self._status_loop())]
        empty_polls = 0
        try:
            while True:
                claimed = await self._dispatch_tick()
                empty_polls = 0 if claimed else empty_polls + 1
                if self._done(empty_polls):
                    break
                if self.shutdown:
                    self._detach_all()
                await self._wait_for_event()
        finally:
            self._stop.set()
            self._detach_all()
            host = [item.aio for item in self.in_flight.values() if item.pod_name is None]
            pods = [item.aio for item in self.in_flight.values() if item.pod_name is not None]
            # Host tasks finish (as the serial loop finishes its current task); Pod coros just let go.
            await asyncio.gather(*host, *pods, *timers, return_exceptions=True)
            watch_task.cancel()
            await asyncio.gather(watch_task, return_exceptions=True)
        return self.executed


async def k8s_worker_main_loop(
    execution_worker_id: int | None = None,
    *,
    install_signal_handlers: bool = True,
    max_tasks: int | None = None,
    max_empty_polls: int | None = None,
) -> int:
    """Main loop of the kubernetes worker (``AAICLICK_RUNNER=kubernetes``).

    Must be called inside an active ``orch_context``. Like the mp worker, the
    parent never queries ClickHouse through the shared client (``ch_refused``);
    shell-Pod log followers open their own.

    Args:
        execution_worker_id: ExecutionWorker ID (registers a new worker if None).
        install_signal_handlers: Install SIGTERM/SIGINT handlers (shutdown:
            stop claiming, let go of the Pods, deregister).
        max_tasks: Stop claiming after this many tasks were started.
        max_empty_polls: Exit after N consecutive empty claim ticks (test helper).

    Returns:
        Number of tasks that completed successfully.
    """
    validate_worker_runner()
    # shortcut: bans chdb with a runner; flush container logs from the child to lift it.
    if is_chdb():
        raise ValueError("the kubernetes worker cannot run on chdb; use a ClickHouse server")
    if execution_worker_id is None:
        worker = await register_execution_worker()
        execution_worker_id = worker.id
        logger.info("ExecutionWorker %s registered (host=%s, pid=%s, mode=k8s)", worker.id, worker.hostname, worker.pid)
    reconciler = _Reconciler(
        execution_worker_id,
        resolve_pod_config(),
        resolve_k8s_limits(),
        max_tasks=max_tasks,
        max_empty_polls=max_empty_polls,
    )

    def on_signal(signum, frame):
        reconciler.shutdown = True
        reconciler.wake.set()

    if install_signal_handlers:
        signal.signal(signal.SIGTERM, on_signal)
        signal.signal(signal.SIGINT, on_signal)
    try:
        with ch_refused("the kubernetes worker parent must not query ClickHouse through the shared client"):
            return await reconciler.run()
    finally:
        await deregister_execution_worker(execution_worker_id)
        logger.info("ExecutionWorker %s stopped (executed %s tasks)", execution_worker_id, reconciler.executed)


Verdict = Literal["delete", "release", "adopt", "leave"]


def _classify(state: PodState, task: Task | None) -> Verdict:
    if task is None or state.run_epoch != task.run_epoch:
        return "delete"
    if task.status == TASK_RUNNING:
        return "adopt"
    if task.status in CANCELLING_TASK_STATUSES:
        return "release"
    return "delete"


async def _load_tasks(task_ids: set[int]) -> dict[int, Task]:
    async with get_sql_session() as session:
        rows = (await session.execute(select(Task).where(col(Task.id).in_(list(task_ids))))).scalars().all()
    return {task.id: task for task in rows}


async def _running_run_id(task_id: int) -> int | None:
    """The attempt's open ``task_runs`` row, for an adopted shell Pod whose
    previous owner already registered the run."""
    async with get_sql_session() as session:
        return (
            await session.execute(
                select(TaskRun.run_id)
                .where(col(TaskRun.task_id) == task_id, col(TaskRun.status) == TASK_RUNNING)
                .order_by(col(TaskRun.run_id).desc())
                .limit(1)
            )
        ).scalar_one_or_none()
