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
from datetime import timedelta
from typing import NamedTuple

from aaiclick.backend import is_chdb
from aaiclick.data.object_context.ch_client import ch_refused

from ...datetime_utils import utc_now
from ..kubernetes_config import K8sLimits, KubernetesConfig, resolve_k8s_limits, resolve_pod_config
from ..models import (
    EXECUTION_WORKER_STOPPED,
    EXECUTION_WORKER_STOPPING,
    Task,
)
from ..runner_config import ENTRY_MODULE
from . import execution_worker as ew
from .claiming import RunKey, claim_next_task, in_flight_status, release_claim, renew_claims
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
from .kubernetes_worker import QuotaExceeded, _kubectl_bin, _pod_name, run_module_pod, run_shell_pod
from .pod_gate import gate
from .pod_watch import _NO_CONTAINER, PodState, PodWatch
from .runner_env import validate_worker_runner

logger = logging.getLogger(__name__)

# Pause on claiming after a ResourceQuota rejection.
QUOTA_BACKOFF = 30.0
# Heartbeats that may fail in a row before the worker lets go of its Pods;
# mirrors ADOPT_AFTER (2 × HEARTBEAT_INTERVAL) on the adopting side.
DETACH_AFTER_MISSED_HEARTBEATS = 2


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
        self.shutdown = False
        self.detached = False
        self.quota_pause_until = 0.0
        self.started = 0
        self.executed = 0

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
        if self.shutdown or self.detached or time.monotonic() < self.quota_pause_until:
            return False
        claimed = False
        while self.max_tasks is None or self.started < self.max_tasks:
            decision = gate(self._gate_states(), self.limits)
            if not decision.open:
                break
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
                await release_claim(RunKey(task.id, task.run_epoch))
                self.quota_pause_until = time.monotonic() + QUOTA_BACKOFF
                return
            except WorkerStopped:
                self.shutdown = True
                return
            except asyncio.CancelledError:
                if detach[0]:
                    return
                raise
            except Exception as e:
                logger.exception("ExecutionWorker %s task %s dispatch raised", self.worker_id, task.id)
                success, result_ref, error = False, None, f"{type(e).__name__}: {e}"
            if detach[0]:
                return  # ownership gone: the result is not ours to write
            if await _handle_task_result(task, self.worker_id, success, result_ref, error):
                self.executed += 1
        finally:
            self.in_flight.pop(task.id, None)
            self.wake.set()

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
                run_id=None,
                detach=detach,
            )
        return result.success, result.result_ref, result.error

    # -- timers -------------------------------------------------------------

    def _pod_items(self) -> list[InFlight]:
        return [item for item in self.in_flight.values() if item.pod_name and not item.aio.done()]

    def _detach_all(self) -> None:
        for item in self._pod_items():
            item.detach[0] = True
            item.aio.cancel()

    async def _heartbeat_loop(self) -> None:
        missed = 0
        while True:
            await asyncio.sleep(ew.HEARTBEAT_INTERVAL)
            try:
                status = await execution_worker_heartbeat(self.worker_id)
            except Exception:
                missed += 1
                logger.exception("ExecutionWorker %s heartbeat failed (%s in a row)", self.worker_id, missed)
                if missed >= DETACH_AFTER_MISSED_HEARTBEATS and not self.detached:
                    logger.warning("ExecutionWorker %s lost the DB; detaching from its Pods", self.worker_id)
                    self.detached = True
                    self._detach_all()
                continue
            missed = 0
            if self.detached:
                self.detached = False
                await self._reattach()
            if status in (EXECUTION_WORKER_STOPPING, EXECUTION_WORKER_STOPPED):
                logger.info("ExecutionWorker %s received stop request (%s)", self.worker_id, status)
                self.shutdown = True
                self.wake.set()
            await self._renew()

    async def _reattach(self) -> None:
        """The DB is back after an outage: re-list and take back our own Pods
        (reclaim, see :func:`reclaim`) unless another worker adopted them."""
        await self.watch.resync()

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
                item.aio.cancel()  # cleared / reaped: the Pod goes
            else:
                logger.info(
                    "ExecutionWorker %s task %s adopted by another worker; detaching", self.worker_id, item.task.id
                )
                item.detach[0] = True
                item.aio.cancel()

    async def _status_loop(self) -> None:
        while True:
            await asyncio.sleep(ew.POLL_INTERVAL)
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
                    item.aio.cancel()
                elif status.run_started_at is None:
                    started_at = item.task.started_at
                    if started_at is not None and now > started_at + timedelta(seconds=self.limits.pending_timeout):
                        state = self.watch.states.get(item.pod_name or "")
                        reason = state.not_started_reason() if state else "Pod not created"
                        item.aio.cancel(
                            f"Pod {item.pod_name} not started after {self.limits.pending_timeout:g}s: {reason}"
                        )
                elif task_timeout is not None and now > status.run_started_at + timedelta(seconds=task_timeout):
                    item.aio.cancel(f"Task timed out after {task_timeout}s")

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
        if self.in_flight:
            return False
        if self.shutdown:
            return True
        if self.max_tasks is not None and self.started >= self.max_tasks:
            return True
        return self.max_empty_polls is not None and empty_polls >= self.max_empty_polls

    async def run(self) -> int:
        await self.watch.resync()
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
            for timer in timers:
                timer.cancel()
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
