"""Tests for orchestration view models and SQLModel adapters."""

from datetime import datetime

from .models import (
    Job,
    JobStatus,
    PreservationMode,
    RegisteredJob,
    RunType,
    Task,
    TaskStatus,
    Worker,
    WorkerStatus,
)
from .view_models import (
    JobDetail,
    JobStatsView,
    JobView,
    RegisteredJobView,
    TaskDetail,
    TaskStatsView,
    TaskView,
    WorkerView,
    _ms_between,
    _short_entrypoint,
    compute_job_stats_view,
    job_to_detail,
    job_to_view,
    registered_job_to_view,
    task_to_detail,
    task_to_stats_view,
    task_to_view,
    worker_to_view,
)


def _make_job(
    *,
    job_id: int = 1,
    name: str = "test_job",
    status: JobStatus = JobStatus.COMPLETED,
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
        preservation_mode=PreservationMode.NONE,
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


def test_job_to_view_round_trip():
    job = _make_job()
    view = job_to_view(job)
    assert isinstance(view, JobView)
    dumped = view.model_dump()
    assert dumped == {
        "id": 1,
        "name": "test_job",
        "status": JobStatus.COMPLETED,
        "run_type": RunType.MANUAL,
        "preservation_mode": PreservationMode.NONE,
        "created_at": datetime(2025, 1, 1, 12, 0, 0),
        "started_at": datetime(2025, 1, 1, 12, 0, 1),
        "completed_at": datetime(2025, 1, 1, 12, 0, 10),
        "error": None,
    }


def test_job_to_view_json_serializes_enums():
    view = job_to_view(_make_job(status=JobStatus.FAILED, error="boom"))
    payload = view.model_dump(mode="json")
    assert payload["status"] == "FAILED"
    assert payload["run_type"] == "MANUAL"
    assert payload["preservation_mode"] == "NONE"
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


def test_task_to_detail_includes_detail_fields():
    task = _make_task(
        kwargs={"x": 1},
        worker_id=42,
        log_path="/tmp/task.log",
        result={"value": 7},
    )
    detail = task_to_detail(task)
    assert isinstance(detail, TaskDetail)
    assert detail.kwargs == {"x": 1}
    assert detail.worker_id == 42
    assert detail.log_path == "/tmp/task.log"
    assert detail.result == {"value": 7}


def test_task_to_detail_empty_kwargs_default():
    # Task.kwargs can be None in the DB — adapter must coerce to {}
    task = _make_task()
    task.kwargs = None
    detail = task_to_detail(task)
    assert detail.kwargs == {}


def test_job_to_detail_embeds_task_views_and_duration():
    job = _make_job(
        started_at=datetime(2025, 1, 1, 12, 0, 1),
        completed_at=datetime(2025, 1, 1, 12, 0, 10, 500_000),  # +9.5s
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


def test_worker_to_view_round_trip():
    worker = Worker(
        id=555,
        hostname="worker-1",
        pid=1234,
        status=WorkerStatus.ACTIVE,
        started_at=datetime(2025, 1, 1, 10, 0, 0),
        last_heartbeat=datetime(2025, 1, 1, 10, 5, 0),
        tasks_completed=7,
        tasks_failed=1,
    )
    view = worker_to_view(worker)
    assert isinstance(view, WorkerView)
    assert view.model_dump() == {
        "id": 555,
        "hostname": "worker-1",
        "pid": 1234,
        "status": WorkerStatus.ACTIVE,
        "started_at": datetime(2025, 1, 1, 10, 0, 0),
        "last_heartbeat": datetime(2025, 1, 1, 10, 5, 0),
        "tasks_completed": 7,
        "tasks_failed": 1,
    }


def test_registered_job_to_view_round_trip():
    rj = RegisteredJob(
        id=999,
        name="etl",
        entrypoint="pkg.mod:etl",
        enabled=True,
        schedule="0 8 * * *",
        default_kwargs={"batch": 100},
        preservation_mode=PreservationMode.FULL,
        next_run_at=datetime(2025, 1, 2, 8, 0, 0),
        created_at=datetime(2025, 1, 1, 0, 0, 0),
        updated_at=datetime(2025, 1, 1, 0, 0, 0),
    )
    view = registered_job_to_view(rj)
    assert isinstance(view, RegisteredJobView)
    assert view.schedule == "0 8 * * *"
    assert view.default_kwargs == {"batch": 100}
    assert view.preservation_mode == PreservationMode.FULL


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
    # created → completed spans 10 seconds
    assert stats.wall_time_ms == 10_000
    # started → completed spans 9 seconds
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


def test_short_entrypoint():
    assert _short_entrypoint("pkg.mod:my_func") == "my_func"
    assert _short_entrypoint("pkg.mod.my_func") == "my_func"
    assert _short_entrypoint("my_func") == "my_func"


def test_ms_between_handles_nones():
    assert _ms_between(None, None) is None
    assert _ms_between(datetime(2025, 1, 1), None) is None
    assert _ms_between(None, datetime(2025, 1, 1)) is None
    assert _ms_between(datetime(2025, 1, 1, 12, 0, 0), datetime(2025, 1, 1, 12, 0, 1, 500_000)) == 1500
