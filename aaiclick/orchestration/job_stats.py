"""Job statistics computation and display."""

from __future__ import annotations

from datetime import timedelta
from typing import Dict, List, Optional

from pydantic import BaseModel

from .models import Job, Task


class TaskStats(BaseModel):
    """Execution statistics for a single task."""

    id: int
    entrypoint: str
    status: str
    queue_time: Optional[timedelta] = None
    exec_time: Optional[timedelta] = None
    error: Optional[str] = None


class JobStats(BaseModel):
    """Execution statistics for a job and all its tasks."""

    job_id: int
    job_name: str
    job_status: str
    total_tasks: int
    status_counts: Dict[str, int]
    wall_time: Optional[timedelta] = None
    exec_time: Optional[timedelta] = None
    tasks: List[TaskStats]


def _fmt_duration(td: Optional[timedelta]) -> str:
    """Format a timedelta as a human-readable duration string."""
    if td is None:
        return "-"
    total_seconds = td.total_seconds()
    if total_seconds < 1:
        return f"{total_seconds * 1000:.0f}ms"
    if total_seconds < 60:
        return f"{total_seconds:.1f}s"
    minutes, seconds = divmod(total_seconds, 60)
    return f"{int(minutes)}m {seconds:.1f}s"


def _short_entrypoint(entrypoint: str) -> str:
    """Extract the short function name from a fully-qualified entrypoint."""
    if ":" in entrypoint:
        return entrypoint.rsplit(":", 1)[-1]
    return entrypoint


def compute_job_stats(job: Job, tasks: list[Task]) -> JobStats:
    """Compute execution statistics for a job and its tasks.

    Args:
        job: Job instance
        tasks: List of Task instances belonging to the job

    Returns:
        JobStats with computed durations and status counts
    """
    status_counts: Dict[str, int] = {}
    for t in tasks:
        status_counts[t.status.value] = status_counts.get(t.status.value, 0) + 1

    task_stats = []
    for t in tasks:
        queue_time = None
        if t.created_at and t.started_at:
            queue_time = t.started_at - t.created_at
        exec_time = None
        if t.started_at and t.completed_at:
            exec_time = t.completed_at - t.started_at

        task_stats.append(TaskStats(
            id=t.id,
            entrypoint=_short_entrypoint(t.entrypoint),
            status=t.status.value,
            queue_time=queue_time,
            exec_time=exec_time,
            error=t.error,
        ))

    wall_time = None
    if job.created_at and job.completed_at:
        wall_time = job.completed_at - job.created_at
    exec_time = None
    if job.started_at and job.completed_at:
        exec_time = job.completed_at - job.started_at

    return JobStats(
        job_id=job.id,
        job_name=job.name,
        job_status=job.status.value,
        total_tasks=len(tasks),
        status_counts=status_counts,
        wall_time=wall_time,
        exec_time=exec_time,
        tasks=task_stats,
    )


def print_job_stats(stats: JobStats) -> None:
    """Print formatted job stats to stdout."""
    print(f"\n{'=' * 70}")
    print(f"JOB STATS: {stats.job_name} (ID: {stats.job_id})")
    print(f"{'=' * 70}")
    print(f"  Status:     {stats.job_status}")
    print(f"  Tasks:      {stats.total_tasks}")
    print(f"  Wall time:  {_fmt_duration(stats.wall_time)}")
    print(f"  Exec time:  {_fmt_duration(stats.exec_time)}")

    parts = [f"{status}: {count}" for status, count in sorted(stats.status_counts.items())]
    print(f"  Breakdown:  {', '.join(parts)}")

    print(f"\n  {'Task':<35} {'Status':<12} {'Queue':<10} {'Exec':<10}")
    print(f"  {'-' * 67}")
    for t in stats.tasks:
        print(
            f"  {t.entrypoint:<35} {t.status:<12} "
            f"{_fmt_duration(t.queue_time):<10} {_fmt_duration(t.exec_time):<10}"
        )
        if t.error:
            print(f"    ERROR: {t.error[:80]}")
    print()
