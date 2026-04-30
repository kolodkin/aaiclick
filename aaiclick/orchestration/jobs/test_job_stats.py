"""Tests for job stats computation."""

from datetime import datetime, timedelta

from ..models import (
    JOB_COMPLETED,
    JOB_FAILED,
    JOB_PENDING,
    RUN_MANUAL,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PENDING,
    Job,
    JobStatus,
    Task,
    TaskStatus,
)
from .stats import JobStats, _fmt_duration, compute_job_stats


def _make_job(
    *,
    status: JobStatus = JOB_COMPLETED,
    created_at: datetime = datetime(2025, 1, 1, 12, 0, 0),
    started_at: datetime = datetime(2025, 1, 1, 12, 0, 1),
    completed_at: datetime = datetime(2025, 1, 1, 12, 0, 10),
) -> Job:
    return Job(
        id=1,
        name="test_job",
        status=status,
        run_type=RUN_MANUAL,
        created_at=created_at,
        started_at=started_at,
        completed_at=completed_at,
    )


def _make_task(
    *,
    task_id: int = 100,
    entrypoint: str = "mod.sub:my_func",
    status: TaskStatus = TASK_COMPLETED,
    created_at: datetime = datetime(2025, 1, 1, 12, 0, 0),
    started_at: datetime = datetime(2025, 1, 1, 12, 0, 2),
    completed_at: datetime = datetime(2025, 1, 1, 12, 0, 5),
    error: str = None,
) -> Task:
    return Task(
        id=task_id,
        job_id=1,
        entrypoint=entrypoint,
        name=entrypoint.rsplit(".", 1)[-1].rsplit(":", 1)[-1],
        status=status,
        created_at=created_at,
        started_at=started_at,
        completed_at=completed_at,
        error=error,
    )


def test_compute_job_stats_basic():
    job = _make_job()
    tasks = [
        _make_task(task_id=100, entrypoint="pkg.mod:extract"),
        _make_task(task_id=101, entrypoint="pkg.mod:transform"),
    ]

    stats = compute_job_stats(job, tasks)

    assert isinstance(stats, JobStats)
    assert stats.job_id == 1
    assert stats.job_name == "test_job"
    assert stats.job_status == "COMPLETED"
    assert stats.total_tasks == 2
    assert stats.status_counts == {"COMPLETED": 2}
    assert stats.wall_time == timedelta(seconds=10)
    assert stats.exec_time == timedelta(seconds=9)


def test_compute_job_stats_mixed_statuses():
    job = _make_job(status=JOB_FAILED)
    tasks = [
        _make_task(task_id=100, status=TASK_COMPLETED),
        _make_task(
            task_id=101,
            status=TASK_FAILED,
            completed_at=datetime(2025, 1, 1, 12, 0, 4),
            error="something broke",
        ),
        _make_task(
            task_id=102,
            status=TASK_PENDING,
            started_at=None,
            completed_at=None,
        ),
    ]

    stats = compute_job_stats(job, tasks)

    assert stats.total_tasks == 3
    assert stats.status_counts == {"COMPLETED": 1, "FAILED": 1, "PENDING": 1}
    assert stats.tasks[1].error == "something broke"
    assert stats.tasks[2].queue_time is None
    assert stats.tasks[2].exec_time is None


def test_compute_job_stats_task_durations():
    job = _make_job()
    task = _make_task(
        created_at=datetime(2025, 1, 1, 12, 0, 0),
        started_at=datetime(2025, 1, 1, 12, 0, 3),
        completed_at=datetime(2025, 1, 1, 12, 0, 8),
    )

    stats = compute_job_stats(job, [task])

    assert stats.tasks[0].queue_time == timedelta(seconds=3)
    assert stats.tasks[0].exec_time == timedelta(seconds=5)


def test_compute_job_stats_short_entrypoint_colon():
    job = _make_job()
    task = _make_task(entrypoint="aaiclick.example_projects.nyc_taxi_pipeline:load_data")

    stats = compute_job_stats(job, [task])

    assert stats.tasks[0].entrypoint == "load_data"


def test_compute_job_stats_short_entrypoint_dot():
    job = _make_job()
    task = _make_task(entrypoint="aaiclick.example_projects.nyc_taxi_pipeline.load_data")

    stats = compute_job_stats(job, [task])

    assert stats.tasks[0].entrypoint == "load_data"


def test_compute_job_stats_no_tasks():
    job = _make_job()

    stats = compute_job_stats(job, [])

    assert stats.total_tasks == 0
    assert stats.status_counts == {}
    assert stats.tasks == []


def test_compute_job_stats_pending_job():
    job = _make_job(
        status=JOB_PENDING,
        started_at=None,
        completed_at=None,
    )

    stats = compute_job_stats(job, [])

    assert stats.wall_time is None
    assert stats.exec_time is None


def test_fmt_duration_milliseconds():
    assert _fmt_duration(timedelta(milliseconds=500)) == "500ms"


def test_fmt_duration_seconds():
    assert _fmt_duration(timedelta(seconds=5.3)) == "5.3s"


def test_fmt_duration_minutes():
    assert _fmt_duration(timedelta(seconds=125)) == "2m 5.0s"


def test_fmt_duration_none():
    assert _fmt_duration(None) == "-"
