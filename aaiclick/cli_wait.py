"""Blocking wait loop shared by ``run-job --progress`` and ``job wait``.

Polls :func:`internal_api.job_stats` until the job reaches a terminal status,
invoking ``on_change`` only when the per-status task counts change, so a piped
CI log gets one report per transition instead of one per tick.

Holds no presentation and opens no context: callers wrap it in
``_run_internal_api`` so every poll shares one orch context, and supply the
render callback themselves.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from time import monotonic

from aaiclick import internal_api
from aaiclick.orchestration.models import TERMINAL_JOB_STATUSES
from aaiclick.orchestration.view_models import JobStatsView
from aaiclick.view_models import RefId

DEFAULT_WAIT_TIMEOUT = 600.0
DEFAULT_POLL_INTERVAL = 1.0


class JobWaitTimeout(RuntimeError):
    """Raised when a job stays non-terminal past the wait timeout.

    Carries the last stats so the caller can report which task is stuck, on
    whichever stream it owns.
    """

    def __init__(self, message: str, stats: JobStatsView) -> None:
        super().__init__(message)
        self.stats = stats


async def wait_for_job(
    ref: RefId,
    *,
    timeout: float = DEFAULT_WAIT_TIMEOUT,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    on_change: Callable[[JobStatsView], None] | None = None,
) -> JobStatsView:
    """Poll ``ref`` until its job reaches a terminal status.

    Args:
        ref: Job id or name.
        timeout: Seconds to wait before raising ``JobWaitTimeout``. The first
            poll always happens, so ``0`` means "check once".
        poll_interval: Seconds between polls.
        on_change: Called with the stats whenever the per-status task counts
            change, and once more on the terminal poll.

    Returns:
        The terminal ``JobStatsView``.

    Raises:
        JobWaitTimeout: If the job is still non-terminal at the deadline.
    """
    deadline = monotonic() + timeout
    # Resolve once: a name resolves to the *most recent* job of that name, so
    # re-resolving each tick would silently retarget a run started mid-wait.
    job_id = (await internal_api.get_job(ref)).id
    last_counts: dict[str, int] | None = None

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

        await asyncio.sleep(poll_interval)
