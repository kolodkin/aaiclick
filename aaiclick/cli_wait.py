"""Blocking wait loop shared by ``run-job --progress`` and ``job wait``.

Polls :func:`internal_api.job_stats` until the job reaches a terminal status.
Progress is rendered only when the per-status task counts change, so a piped
CI log gets one table per transition instead of one per tick.

Deliberately does not open its own context — callers wrap it in
``_run_internal_api`` so every poll shares one orch context.
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import redirect_stdout
from time import monotonic

from aaiclick import cli_renderers, internal_api
from aaiclick.orchestration.models import JOB_CANCELLED, JOB_COMPLETED, JOB_FAILED, JobStatus
from aaiclick.orchestration.view_models import JobStatsView
from aaiclick.view_models import RefId

TERMINAL_JOB_STATUSES: tuple[JobStatus, ...] = (JOB_COMPLETED, JOB_FAILED, JOB_CANCELLED)
DEFAULT_WAIT_TIMEOUT = 600.0
DEFAULT_POLL_INTERVAL = 1.0


class JobWaitTimeout(RuntimeError):
    """Raised when a job stays non-terminal past the wait timeout."""


def exit_code_for(status: JobStatus) -> int:
    """Process exit code for a terminal job status.

    Only ``COMPLETED`` is a success: ``set -e`` scripts and CI steps depend on
    a failed or cancelled job exiting non-zero.
    """
    return 0 if status == JOB_COMPLETED else 1


async def wait_for_job(
    ref: RefId,
    *,
    timeout: float = DEFAULT_WAIT_TIMEOUT,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    progress: bool = False,
) -> JobStatsView:
    """Poll ``ref`` until its job reaches a terminal status.

    Args:
        ref: Job id or name.
        timeout: Seconds to wait before raising ``JobWaitTimeout``. The first
            poll always happens, so ``0`` means "check once".
        poll_interval: Seconds between polls.
        progress: Render the stats table on every status-count change.

    Returns:
        The terminal ``JobStatsView``.

    Raises:
        JobWaitTimeout: If the job is still non-terminal at the deadline.
    """
    deadline = monotonic() + timeout
    last_counts: dict[str, int] | None = None

    while True:
        stats = await internal_api.job_stats(ref)
        terminal = stats.job_status in TERMINAL_JOB_STATUSES

        if progress and (terminal or stats.status_counts != last_counts):
            cli_renderers.render_job_stats(stats)
            last_counts = stats.status_counts

        if terminal:
            return stats

        if monotonic() >= deadline:
            # Always dump task states: a timeout is only actionable if it
            # names the stuck task, whether or not progress was requested.
            # It is a diagnostic, so it goes to stderr — stdout stays a single
            # parseable document for --json callers.
            if not progress:
                with redirect_stdout(sys.stderr):
                    cli_renderers.render_job_stats(stats)
            raise JobWaitTimeout(
                f"Job {stats.job_name!r} (id={stats.job_id}) did not reach a terminal "
                f"status within {timeout}s; last status was {stats.job_status}"
            )

        await asyncio.sleep(poll_interval)
