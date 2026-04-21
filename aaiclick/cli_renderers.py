"""Text renderers for the CLI.

Each CLI verb has two output modes: the default human-readable table (one
function per verb here) and ``--json`` (``view.model_dump_json()`` invoked
directly in ``__main__``). Renderers read fields off the view model — never
from SQLModel rows — so the JSON schema and text columns cannot drift.
"""

from __future__ import annotations

from aaiclick.orchestration.view_models import (
    JobDetail,
    JobStatsView,
    JobView,
    RegisteredJobView,
    TaskDetail,
    WorkerView,
)
from aaiclick.view_models import Page


def _fmt_ms(ms: int | None) -> str:
    """Format a millisecond duration as ``123ms`` / ``1.2s`` / ``1m 30.5s``."""
    if ms is None:
        return "-"
    if ms < 1000:
        return f"{ms}ms"
    seconds = ms / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, rem = divmod(seconds, 60)
    return f"{int(minutes)}m {rem:.1f}s"


def render_jobs_page(page: Page[JobView], offset: int) -> None:
    """Print a paged list of jobs as an aligned text table."""
    if not page.items:
        print("No jobs found")
        return

    print(f"{'ID':<20} {'Name':<25} {'Status':<12} {'Type':<10} {'Created':<20}")
    print("-" * 87)
    for j in page.items:
        created = j.created_at.strftime("%Y-%m-%d %H:%M:%S")
        print(f"{j.id:<20} {j.name:<25} {j.status.value:<12} {j.run_type.value:<10} {created:<20}")
    total = page.total if page.total is not None else len(page.items)
    print(f"\nShowing {offset + 1}-{offset + len(page.items)} of {total}")


def render_job_detail(detail: JobDetail) -> None:
    """Print full job details — ID, name, status, timestamps, registration."""
    print(f"ID:           {detail.id}")
    print(f"Name:         {detail.name}")
    print(f"Status:       {detail.status.value}")
    print(f"Run type:     {detail.run_type.value}")
    print(f"Registered:   {detail.registered_job_id or '-'}")
    print(f"Created at:   {detail.created_at}")
    print(f"Started at:   {detail.started_at or '-'}")
    print(f"Completed at: {detail.completed_at or '-'}")
    if detail.error:
        print(f"Error:        {detail.error}")


def render_job_stats(stats: JobStatsView) -> None:
    """Print formatted job stats as a markdown table to stdout."""
    print(f"\n## Job: {stats.job_name} (ID: {stats.job_id})")
    print()
    breakdown = [f"{status}: {count}" for status, count in sorted(stats.status_counts.items())]
    print("| Status | Tasks | Wall Time | Exec Time | Breakdown |")
    print("|--------|-------|-----------|-----------|-----------|")
    print(
        f"| {stats.job_status.value} "
        f"| {stats.total_tasks} "
        f"| {_fmt_ms(stats.wall_time_ms)} "
        f"| {_fmt_ms(stats.exec_time_ms)} "
        f"| {', '.join(breakdown)} |"
    )
    print()
    print("| Task | Status | Queue | Exec |")
    print("|------|--------|-------|------|")
    for t in stats.tasks:
        error_suffix = f" `{t.error[:60]}`" if t.error else ""
        print(
            f"| {t.entrypoint} "
            f"| {t.status.value} "
            f"| {_fmt_ms(t.queue_time_ms)} "
            f"| {_fmt_ms(t.exec_time_ms)}{error_suffix} |"
        )
    print()


def render_job_cancelled(view: JobView) -> None:
    """Single-line confirmation that ``internal_api.cancel_job`` succeeded."""
    print(f"Job {view.id} cancelled")


def render_job_created(view: JobView) -> None:
    """Single-line confirmation that ``internal_api.run_job`` created a job."""
    print(f"Job '{view.name}' created (id={view.id}, run_type={view.run_type.value})")


def render_registered_jobs_page(page: Page[RegisteredJobView]) -> None:
    """Print a paged list of registered jobs as an aligned text table."""
    if not page.items:
        print("No registered jobs found")
        return

    print(f"{'ID':<20} {'Name':<25} {'Enabled':<9} {'Schedule':<15} {'Next Run':<20}")
    print("-" * 89)
    for j in page.items:
        next_run = j.next_run_at.strftime("%Y-%m-%d %H:%M:%S") if j.next_run_at else "-"
        print(f"{j.id:<20} {j.name:<25} {str(j.enabled):<9} {j.schedule or '-':<15} {next_run:<20}")


def render_registered_job(view: RegisteredJobView) -> None:
    """Print registered-job details — mirrors the old ``register-job`` output."""
    print(f"Registered job '{view.name}' (id={view.id})")
    if view.schedule:
        print(f"  Schedule:         {view.schedule}")
    if view.preservation_mode:
        print(f"  Preservation:     {view.preservation_mode.value}")
    if view.next_run_at:
        print(f"  Next run at:      {view.next_run_at}")


def render_registered_job_enabled(view: RegisteredJobView) -> None:
    """Single-line confirmation that ``internal_api.enable_job`` succeeded."""
    print(f"Job '{view.name}' enabled (id={view.id})")


def render_registered_job_disabled(view: RegisteredJobView) -> None:
    """Single-line confirmation that ``internal_api.disable_job`` succeeded."""
    print(f"Job '{view.name}' disabled (id={view.id})")


def render_task_detail(detail: TaskDetail) -> None:
    """Print full task details — ID, job, entrypoint, status, timings, worker."""
    print(f"ID:           {detail.id}")
    print(f"Job:          {detail.job_id}")
    print(f"Name:         {detail.name}")
    print(f"Entrypoint:   {detail.entrypoint}")
    print(f"Status:       {detail.status.value}")
    print(f"Attempt:      {detail.attempt}")
    print(f"Max retries:  {detail.max_retries}")
    print(f"Created at:   {detail.created_at}")
    print(f"Started at:   {detail.started_at or '-'}")
    print(f"Completed at: {detail.completed_at or '-'}")
    print(f"Worker:       {detail.worker_id or '-'}")
    print(f"Log path:     {detail.log_path or '-'}")
    if detail.kwargs:
        print(f"Kwargs:       {detail.kwargs}")
    if detail.result is not None:
        print(f"Result:       {detail.result}")
    if detail.error:
        print(f"Error:        {detail.error}")


def render_workers_page(page: Page[WorkerView], offset: int) -> None:
    """Print a paged list of workers as an aligned text table."""
    if not page.items:
        print("No workers found")
        return

    print(f"{'ID':<20} {'Status':<10} {'Host':<20} {'PID':<8} {'Completed':<10} {'Failed':<8}")
    print("-" * 80)
    for w in page.items:
        print(
            f"{w.id:<20} {w.status.value:<10} {w.hostname:<20} {w.pid:<8} {w.tasks_completed:<10} {w.tasks_failed:<8}"
        )
    total = page.total if page.total is not None else len(page.items)
    print(f"\nShowing {offset + 1}-{offset + len(page.items)} of {total}")


def render_worker_stopped(view: WorkerView) -> None:
    """Single-line confirmation that ``internal_api.stop_worker`` succeeded."""
    print(f"Stop requested for worker {view.id}")
