"""Tests for the blocking wait/progress loop behind ``run-job --progress``
and ``job wait``."""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from aaiclick.cli_renderers import render_job_failure
from aaiclick.cli_wait import JobWaitTimeout, wait_for_job
from aaiclick.orchestration.models import JOB_COMPLETED, Job, JobStatus
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.registered_jobs import run_job
from aaiclick.orchestration.view_models import JobStatsView, TaskStatsView


def _stats(job_status: JobStatus, counts: dict[str, int], tasks: list[TaskStatsView] | None = None) -> JobStatsView:
    return JobStatsView(
        job_id=1,
        job_name="j",
        job_status=job_status,
        total_tasks=sum(counts.values()),
        status_counts=counts,
        tasks=tasks or [],
    )


@contextmanager
def _patch_stats(*returns: JobStatsView):
    """Patch ``job_stats`` to yield ``returns`` in order, repeating the last.

    ``get_job`` is stubbed too: the loop resolves the ref once up front.
    """
    calls = list(returns)

    async def _next(_ref):
        return calls.pop(0) if len(calls) > 1 else calls[0]

    job_stats = AsyncMock(side_effect=_next)
    with patch.multiple(
        "aaiclick.cli_wait.internal_api",
        job_stats=job_stats,
        get_job=AsyncMock(return_value=SimpleNamespace(id=1)),
    ):
        yield job_stats


async def test_wait_returns_immediately_when_job_already_terminal():
    with _patch_stats(_stats("COMPLETED", {"COMPLETED": 2})) as stats:
        result = await wait_for_job(1, timeout=5.0, poll_interval=0)

    assert result.job_status == "COMPLETED"
    assert stats.await_count == 1


async def test_wait_polls_until_job_reaches_terminal_status():
    with _patch_stats(
        _stats("RUNNING", {"RUNNING": 1}),
        _stats("RUNNING", {"COMPLETED": 1, "RUNNING": 1}),
        _stats("COMPLETED", {"COMPLETED": 2}),
    ) as stats:
        result = await wait_for_job(1, timeout=5.0, poll_interval=0)

    assert result.job_status == "COMPLETED"
    assert stats.await_count == 3


async def test_wait_treats_cancelled_as_terminal():
    """A cancelled job must end the loop — otherwise the CLI hangs until timeout."""
    with _patch_stats(_stats("CANCELLED", {"CANCELLED": 1})):
        result = await wait_for_job(1, timeout=5.0, poll_interval=0)

    assert result.job_status == "CANCELLED"


async def test_wait_raises_timeout_when_job_never_finishes():
    with _patch_stats(_stats("RUNNING", {"RUNNING": 1})):
        with pytest.raises(JobWaitTimeout, match="did not reach a terminal"):
            await wait_for_job(1, timeout=0.0, poll_interval=0)


async def test_on_change_fires_only_when_status_counts_change():
    """A loop that reported every tick would bury a piped CI log in hundreds of
    identical tables."""
    seen: list[str] = []
    with _patch_stats(
        _stats("RUNNING", {"RUNNING": 1}),
        _stats("RUNNING", {"RUNNING": 1}),
        _stats("RUNNING", {"RUNNING": 1}),
        _stats("COMPLETED", {"COMPLETED": 1}),
    ):
        await wait_for_job(1, timeout=5.0, poll_interval=0, on_change=lambda s: seen.append(s.job_status))

    assert seen == ["RUNNING", "COMPLETED"]


async def test_no_output_without_a_callback(capsys):
    with _patch_stats(_stats("COMPLETED", {"COMPLETED": 1})):
        await wait_for_job(1, timeout=5.0, poll_interval=0)

    assert capsys.readouterr().out == ""


async def test_timeout_carries_the_stats_for_diagnosis():
    """A timeout is only actionable if it names the stuck task, so the caller
    gets the last stats rather than a bare message."""
    tasks = [TaskStatsView(id=7, entrypoint="mod.stuck", status="RUNNING")]
    with _patch_stats(_stats("RUNNING", {"RUNNING": 1}, tasks)):
        with pytest.raises(JobWaitTimeout) as exc:
            await wait_for_job(1, timeout=0.0, poll_interval=0)

    assert [t.entrypoint for t in exc.value.stats.tasks] == ["mod.stuck"]


def test_render_job_failure_shows_full_error_not_the_table_truncation(capsys):
    """``render_job_stats`` clips errors to 60 chars for its table; the final
    failure message must not, or the traceback is unusable."""
    long_error = "ValueError: " + "x" * 200
    stats = _stats(
        "FAILED",
        {"FAILED": 1, "COMPLETED": 1},
        [
            TaskStatsView(id=7, entrypoint="mod.boom", status="FAILED", error=long_error),
            TaskStatsView(id=8, entrypoint="mod.ok", status="COMPLETED"),
        ],
    )
    render_job_failure(stats)
    out = capsys.readouterr().out

    assert long_error in out
    assert "mod.ok" not in out
    assert "task get 7" in out


async def test_wait_resolves_a_real_job_through_the_internal_api(orch_ctx):
    """Guards the seam the mocked tests cannot: ref resolution and the real
    ``JobStatsView`` shape coming back from a DB round-trip."""
    job = await run_job("waitable", "myapp.waitable")
    async with get_sql_session() as session:
        row = await session.get(Job, job.id)
        assert row is not None
        row.status = JOB_COMPLETED
        session.add(row)
        await session.commit()

    stats = await wait_for_job(job.id, timeout=0.0, poll_interval=0)

    assert stats.job_id == job.id
    assert stats.job_status == JOB_COMPLETED
    assert stats.total_tasks == 1


def test_render_job_failure_is_not_silent_on_a_cascade_only_failure(capsys):
    """A cancelled origin leaves no directly-FAILED task, but the rollup still
    fails the job — the block must still name what went wrong."""
    stats = _stats(
        "FAILED",
        {"UPSTREAM_FAILED": 1, "CANCELLED": 1},
        [
            TaskStatsView(id=9, entrypoint="mod.downstream", status="UPSTREAM_FAILED", error="Upstream task failed"),
            TaskStatsView(id=8, entrypoint="mod.origin", status="CANCELLED"),
        ],
    )
    render_job_failure(stats)
    out = capsys.readouterr().out

    assert "mod.downstream" in out
    assert "mod.origin" in out


def test_render_job_failure_hides_cascade_victims_when_a_real_failure_exists(capsys):
    """A wide fan-out can cascade to hundreds of UPSTREAM_FAILED tasks whose
    error is one constant string — noise next to the task that actually broke."""
    stats = _stats(
        "FAILED",
        {"FAILED": 1, "UPSTREAM_FAILED": 1},
        [
            TaskStatsView(id=7, entrypoint="mod.boom", status="FAILED", error="ValueError: real"),
            TaskStatsView(id=9, entrypoint="mod.downstream", status="UPSTREAM_FAILED", error="Upstream task failed"),
        ],
    )
    render_job_failure(stats)
    out = capsys.readouterr().out

    assert "mod.boom" in out
    assert "mod.downstream" not in out
