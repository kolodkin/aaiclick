"""Tests for orchestration view models and SQLModel adapters."""

from datetime import datetime

from .models import (
    Job,
    JobStatus,
    RunType,
    Task,
    TaskStatus,
)
from .view_models import (
    JobDetail,
    JobStatsView,
    TaskStatsView,
    TaskView,
    _ms_between,
    compute_job_stats_view,
    job_to_detail,
    job_to_view,
    task_to_stats_view,
    task_to_view,
)


def _make_job(
    *,
    job_id: int = 1,
    name: str = "test_job",
    status: JobStatus = JobStatus.COMPLETED,
    registered_job_id: int | None = None,
    created_at: datetime = datetime(2025, 1, 1, 12, 0, 0),
    started_at: datetime | None = datetime(2025, 1, 1, 12, 0, 1),
    completed_at: datetime | None = datetime(2025, 1, 1, 12, 0, 10),
    error: str | None = None,
) -> Job:
    return Job(
        id=job_id,
        name=name,
        status=status,
        run_type=RunType.MANUAL,
        preserve=None,
        registered_job_id=registered_job_id,
        created_at=created_at,
        started_at=started_at,
        completed_at=completed_at,
        error=error,
    )


def _make_task(
    *,
    task_id: int = 100,
    job_id: int = 1,
    entrypoint: str = "pkg.mod:extract",
    status: TaskStatus = TaskStatus.COMPLETED,
    created_at: datetime = datetime(2025, 1, 1, 12, 0, 0),
    started_at: datetime | None = datetime(2025, 1, 1, 12, 0, 2),
    completed_at: datetime | None = datetime(2025, 1, 1, 12, 0, 5),
    error: str | None = None,
    kwargs: dict | None = None,
    worker_id: int | None = None,
    log_path: str | None = None,
    result: dict | None = None,
) -> Task:
    return Task(
        id=task_id,
        job_id=job_id,
        entrypoint=entrypoint,
        name=entrypoint.rsplit(":", 1)[-1].rsplit(".", 1)[-1],
        status=status,
        created_at=created_at,
        started_at=started_at,
        completed_at=completed_at,
        error=error,
        kwargs=kwargs or {},
        worker_id=worker_id,
        log_path=log_path,
        result=result,
        attempt=1,
    )


def test_job_to_view_propagates_registered_job_id():
    job = _make_job(registered_job_id=42)
    view = job_to_view(job)
    assert view.registered_job_id == 42


def test_job_to_view_json_serializes_enums():
    view = job_to_view(_make_job(status=JobStatus.FAILED, error="boom"))
    payload = view.model_dump(mode="json")
    assert payload["status"] == "FAILED"
    assert payload["run_type"] == "MANUAL"
    assert payload["preserve"] is None
    assert payload["error"] == "boom"


def test_task_to_view_omits_detail_fields():
    task = _make_task(kwargs={"x": 1}, worker_id=42)
    view = task_to_view(task)
    dumped = view.model_dump()
    assert dumped["id"] == 100
    assert dumped["job_id"] == 1
    assert dumped["status"] == TaskStatus.COMPLETED
    assert "kwargs" not in dumped
    assert "worker_id" not in dumped


def test_job_to_detail_embeds_task_views_and_duration():
    job = _make_job(
        started_at=datetime(2025, 1, 1, 12, 0, 1),
        completed_at=datetime(2025, 1, 1, 12, 0, 10, 500_000),
    )
    tasks = [
        _make_task(task_id=100, entrypoint="pkg:a"),
        _make_task(task_id=101, entrypoint="pkg:b"),
    ]
    detail = job_to_detail(job, tasks)
    assert isinstance(detail, JobDetail)
    assert len(detail.tasks) == 2
    assert all(isinstance(t, TaskView) for t in detail.tasks)
    assert detail.duration_ms == 9500


def test_job_to_detail_duration_none_when_not_completed():
    job = _make_job(started_at=None, completed_at=None, status=JobStatus.PENDING)
    detail = job_to_detail(job, [])
    assert detail.duration_ms is None


def test_compute_job_stats_view_basic():
    job = _make_job()
    tasks = [
        _make_task(task_id=100, entrypoint="pkg.mod:extract"),
        _make_task(task_id=101, entrypoint="pkg.mod:transform"),
    ]
    stats = compute_job_stats_view(job, tasks)
    assert isinstance(stats, JobStatsView)
    assert stats.job_id == 1
    assert stats.job_status == JobStatus.COMPLETED
    assert stats.total_tasks == 2
    assert stats.status_counts == {"COMPLETED": 2}
    assert stats.wall_time_ms == 10_000
    assert stats.exec_time_ms == 9_000
    assert [t.entrypoint for t in stats.tasks] == ["extract", "transform"]


def test_compute_job_stats_view_mixed_statuses():
    job = _make_job(status=JobStatus.FAILED)
    tasks = [
        _make_task(task_id=100, status=TaskStatus.COMPLETED),
        _make_task(
            task_id=101,
            status=TaskStatus.FAILED,
            completed_at=datetime(2025, 1, 1, 12, 0, 4),
            error="boom",
        ),
        _make_task(
            task_id=102,
            status=TaskStatus.PENDING,
            started_at=None,
            completed_at=None,
        ),
    ]
    stats = compute_job_stats_view(job, tasks)
    assert stats.status_counts == {"COMPLETED": 1, "FAILED": 1, "PENDING": 1}
    assert stats.tasks[1].error == "boom"
    assert stats.tasks[2].queue_time_ms is None
    assert stats.tasks[2].exec_time_ms is None


def test_task_to_stats_view_queue_and_exec_ms():
    task = _make_task(
        created_at=datetime(2025, 1, 1, 12, 0, 0),
        started_at=datetime(2025, 1, 1, 12, 0, 3),
        completed_at=datetime(2025, 1, 1, 12, 0, 8),
    )
    sv = task_to_stats_view(task)
    assert isinstance(sv, TaskStatsView)
    assert sv.queue_time_ms == 3_000
    assert sv.exec_time_ms == 5_000


def test_ms_between_handles_nones():
    assert _ms_between(None, None) is None
    assert _ms_between(datetime(2025, 1, 1), None) is None
    assert _ms_between(None, datetime(2025, 1, 1)) is None
    assert _ms_between(datetime(2025, 1, 1, 12, 0, 0), datetime(2025, 1, 1, 12, 0, 1, 500_000)) == 1500
