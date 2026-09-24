"""Tests for the CLI output renderers in ``aaiclick/cli_renderers.py``."""

from aaiclick.cli_renderers import render_job_failure
from aaiclick.orchestration.models import JobStatus
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
