"""Orchestration domain view models plus SQLModel → view adapters.

View models are plain pydantic — no SQLModel imports. The ``to_view`` /
``to_detail`` adapters pull SQLModel in and are the only path that knows about
the DB schema, keeping imports one-directional (SQLModel → pydantic).
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from .jobs.stats import _short_entrypoint
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


class JobView(BaseModel):
    """Compact job representation used by list endpoints."""

    id: int
    name: str
    status: JobStatus
    run_type: RunType
    preservation_mode: PreservationMode
    registered_job_id: int | None = None
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


class TaskView(BaseModel):
    """Compact task representation used by list endpoints and ``JobDetail``."""

    id: int
    job_id: int
    entrypoint: str
    name: str
    status: TaskStatus
    attempt: int
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None


class TaskDetail(TaskView):
    """Full task representation used by ``GET /tasks/{id}``."""

    kwargs: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] | None = None
    log_path: str | None = None
    worker_id: int | None = None
    error: str | None = None
    max_retries: int = 0


class JobDetail(JobView):
    """Full job representation used by ``GET /jobs/{ref}``."""

    tasks: list[TaskView] = Field(default_factory=list)
    duration_ms: int | None = None


class TaskStatsView(BaseModel):
    """Per-task execution stats exposed inside ``JobStatsView``."""

    id: int
    entrypoint: str
    status: TaskStatus
    queue_time_ms: int | None = None
    exec_time_ms: int | None = None
    error: str | None = None


class JobStatsView(BaseModel):
    """Execution stats for a job and all its tasks.

    Replacement for ``aaiclick.orchestration.jobs.stats.JobStats`` — Phase 2
    migrates callers, after which the old dataclass is removed.
    """

    job_id: int
    job_name: str
    job_status: JobStatus
    total_tasks: int
    status_counts: dict[str, int]
    wall_time_ms: int | None = None
    exec_time_ms: int | None = None
    tasks: list[TaskStatsView] = Field(default_factory=list)


class WorkerView(BaseModel):
    """Worker representation used by ``GET /workers``."""

    id: int
    hostname: str
    pid: int
    status: WorkerStatus
    started_at: datetime
    last_heartbeat: datetime
    tasks_completed: int
    tasks_failed: int


class RegisteredJobView(BaseModel):
    """Registered job representation used by ``GET /registered-jobs``."""

    id: int
    name: str
    entrypoint: str
    enabled: bool
    schedule: str | None = None
    default_kwargs: dict[str, Any] | None = None
    preservation_mode: PreservationMode | None = None
    next_run_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


def _ms_between(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return int((end - start).total_seconds() * 1000)


def job_to_view(job: Job) -> JobView:
    return JobView(
        id=job.id,
        name=job.name,
        status=job.status,
        run_type=job.run_type,
        preservation_mode=job.preservation_mode,
        registered_job_id=job.registered_job_id,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error=job.error,
    )


def task_to_view(task: Task) -> TaskView:
    return TaskView(
        id=task.id,
        job_id=task.job_id,
        entrypoint=task.entrypoint,
        name=task.name,
        status=task.status,
        attempt=task.attempt,
        created_at=task.created_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
    )


def task_to_detail(task: Task) -> TaskDetail:
    return TaskDetail(
        id=task.id,
        job_id=task.job_id,
        entrypoint=task.entrypoint,
        name=task.name,
        status=task.status,
        attempt=task.attempt,
        created_at=task.created_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
        kwargs=task.kwargs,
        result=task.result,
        log_path=task.log_path,
        worker_id=task.worker_id,
        error=task.error,
        max_retries=task.max_retries,
    )


def job_to_detail(job: Job, tasks: list[Task]) -> JobDetail:
    return JobDetail(
        id=job.id,
        name=job.name,
        status=job.status,
        run_type=job.run_type,
        preservation_mode=job.preservation_mode,
        registered_job_id=job.registered_job_id,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error=job.error,
        tasks=[task_to_view(t) for t in tasks],
        duration_ms=_ms_between(job.started_at, job.completed_at),
    )


def worker_to_view(worker: Worker) -> WorkerView:
    return WorkerView(
        id=worker.id,
        hostname=worker.hostname,
        pid=worker.pid,
        status=worker.status,
        started_at=worker.started_at,
        last_heartbeat=worker.last_heartbeat,
        tasks_completed=worker.tasks_completed,
        tasks_failed=worker.tasks_failed,
    )


def registered_job_to_view(rj: RegisteredJob) -> RegisteredJobView:
    return RegisteredJobView(
        id=rj.id,
        name=rj.name,
        entrypoint=rj.entrypoint,
        enabled=rj.enabled,
        schedule=rj.schedule,
        default_kwargs=rj.default_kwargs,
        preservation_mode=rj.preservation_mode,
        next_run_at=rj.next_run_at,
        created_at=rj.created_at,
        updated_at=rj.updated_at,
    )


def task_to_stats_view(task: Task) -> TaskStatsView:
    return TaskStatsView(
        id=task.id,
        entrypoint=_short_entrypoint(task.entrypoint),
        status=task.status,
        queue_time_ms=_ms_between(task.created_at, task.started_at),
        exec_time_ms=_ms_between(task.started_at, task.completed_at),
        error=task.error,
    )


def compute_job_stats_view(job: Job, tasks: list[Task]) -> JobStatsView:
    """Compute the execution-stats view model for a job and its tasks."""
    return JobStatsView(
        job_id=job.id,
        job_name=job.name,
        job_status=job.status,
        total_tasks=len(tasks),
        status_counts=dict(Counter(t.status for t in tasks)),
        wall_time_ms=_ms_between(job.created_at, job.completed_at),
        exec_time_ms=_ms_between(job.started_at, job.completed_at),
        tasks=[task_to_stats_view(t) for t in tasks],
    )
