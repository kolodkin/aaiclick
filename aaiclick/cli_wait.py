"""Blocking wait loop shared by ``run-job --progress`` and ``job wait``.

Watches :func:`internal_api.job_stats` until the job reaches a terminal status,
invoking ``on_change`` only when the per-status task counts change, so a piped
CI log gets one report per transition instead of one per tick.

Where the transport is ``cross_process`` the loop blocks on a change signal
instead of sleeping, keeping a slow poll as the safety net; otherwise it polls
exactly as before.

Holds no presentation and opens no context: callers wrap it in
``_run_internal_api`` so every poll shares one orch context, and supply the
render callback themselves.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from time import monotonic

from aaiclick import internal_api
from aaiclick.orchestration.env import job_wait_timeout
from aaiclick.orchestration.events import (
    STATE_LISTENING,
    EventBus,
    SignalTransport,
    event_bus,
    get_transport,
    signal_transport,
)
from aaiclick.orchestration.models import TERMINAL_JOB_STATUSES
from aaiclick.orchestration.view_models import JobStatsView
from aaiclick.view_models import RefId

DEFAULT_POLL_INTERVAL = 1.0
DEFAULT_SIGNAL_POLL_INTERVAL = 10.0


class JobWaitTimeout(RuntimeError):
    """Raised when a job stays non-terminal past the wait timeout.

    Carries the last stats so the caller can report which task is stuck, on
    whichever stream it owns.
    """

    def __init__(self, message: str, stats: JobStatsView) -> None:
        super().__init__(message)
        self.stats = stats


def _wake_interval(transport: SignalTransport, poll_interval: float, signal_poll_interval: float) -> float:
    """How long to wait before refetching unprompted.

    Both conditions are load-bearing: ``state`` alone would pick the slow
    interval for ``LocalTransport``, which always reports ``LISTENING``.
    """
    if transport.cross_process and transport.state == STATE_LISTENING:
        return signal_poll_interval
    return poll_interval


@asynccontextmanager
async def _wake_ups(transport: SignalTransport) -> AsyncIterator[Callable[[float], Awaitable[None]]]:
    """Yield ``await wake(seconds)``, which returns early on a change signal.

    A transport that is not ``cross_process`` yields plain ``asyncio.sleep``;
    nothing would ever wake the subscription.

    The subscription opens before the caller's first fetch and stays open for
    the whole loop. The mailbox is depth-1, so a commit landing during a fetch
    is queued and the next wait returns at once instead of being lost.
    """
    if not transport.cross_process:
        yield asyncio.sleep
        return

    bus = EventBus()
    stop = asyncio.Event()
    with event_bus(bus), signal_transport(transport):
        feed = asyncio.create_task(transport.feed(bus, stop=stop))
        try:
            with bus.subscription() as sub:

                async def wake(seconds: float) -> None:
                    # A signal, a timeout and a closed bus all mean the same
                    # thing — refetch now — so the reason is never read.
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(sub.wait(), seconds)

                yield wake
        finally:
            stop.set()
            feed.cancel()
            with suppress(asyncio.CancelledError):
                await feed


async def wait_for_job(
    ref: RefId,
    *,
    timeout: float | None = None,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    signal_poll_interval: float = DEFAULT_SIGNAL_POLL_INTERVAL,
    on_change: Callable[[JobStatsView], None] | None = None,
) -> JobStatsView:
    """Poll ``ref`` until its job reaches a terminal status.

    Args:
        ref: Job id or name.
        timeout: Seconds to wait before raising ``JobWaitTimeout``. The first
            poll always happens, so ``0`` means "check once". ``None`` reads
            ``AAICLICK_JOB_WAIT_TIMEOUT``, defaulting to an hour.
        poll_interval: Seconds between polls when no change signal can reach
            this process.
        signal_poll_interval: Seconds between polls while change signals are
            arriving, where the poll is only a safety net. Ignored when the
            backend cannot deliver signals to this process.
        on_change: Called with the stats whenever the per-status task counts
            change, and once more on the terminal poll.

    Returns:
        The terminal ``JobStatsView``.

    Raises:
        JobWaitTimeout: If the job is still non-terminal at the deadline.
    """
    if timeout is None:
        timeout = job_wait_timeout()
    deadline = monotonic() + timeout
    # Resolve once: a name resolves to the *most recent* job of that name, so
    # re-resolving each tick would silently retarget a run started mid-wait.
    job_id = (await internal_api.get_job(ref)).id
    last_counts: dict[str, int] | None = None

    transport = get_transport()
    async with _wake_ups(transport) as wake:
        while True:
            stats = await internal_api.job_stats(job_id)
            terminal = stats.job_status in TERMINAL_JOB_STATUSES

            # ``terminal or`` guarantees a final report even when the last tick's
            # counts are unchanged — tasks finish, then the job row flips.
            if on_change is not None and (terminal or stats.status_counts != last_counts):
                on_change(stats)
                last_counts = stats.status_counts

            if terminal:
                return stats

            if monotonic() >= deadline:
                raise JobWaitTimeout(
                    f"Job {stats.job_name!r} (id={stats.job_id}) did not reach a terminal "
                    f"status within {timeout}s; last status was {stats.job_status}",
                    stats,
                )

            await wake(_wake_interval(transport, poll_interval, signal_poll_interval))
