"""
aaiclick.orchestration.models - Data models for orchestration backend.

This module defines SQLModel models for jobs, tasks, workers, groups, and dependencies.
All IDs are snowflake IDs (64-bit integers) generated using aaiclick.snowflake.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, Optional

from sqlmodel import JSON, Column, Field, SQLModel


class JobStatus(StrEnum):
    """Job execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskStatus(StrEnum):
    """Task execution status."""

    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class WorkerStatus(StrEnum):
    """Worker status."""

    ACTIVE = "active"
    IDLE = "idle"
    STOPPED = "stopped"


class Job(SQLModel, table=True):
    """
    Job model - represents a workflow execution.

    Each job has a unique snowflake ID and tracks overall workflow status.
    """

    __tablename__ = "jobs"

    id: int = Field(primary_key=True)
    name: str = Field(index=True)
    status: JobStatus = Field(default=JobStatus.PENDING, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)
    error: Optional[str] = Field(default=None)


class Group(SQLModel, table=True):
    """
    Group model - represents a logical grouping of tasks.

    Groups support nesting via parent_group_id.
    """

    __tablename__ = "groups"

    id: int = Field(primary_key=True)
    job_id: int = Field(foreign_key="jobs.id", index=True)
    parent_group_id: Optional[int] = Field(default=None, foreign_key="groups.id", index=True)
    name: str = Field()
    created_at: datetime = Field(default_factory=datetime.utcnow)


class Task(SQLModel, table=True):
    """
    Task model - represents a unit of work to be executed.

    Tasks can be part of a group and have dependencies on other tasks/groups.
    """

    __tablename__ = "tasks"

    id: int = Field(primary_key=True)
    job_id: int = Field(foreign_key="jobs.id", index=True)
    group_id: Optional[int] = Field(default=None, foreign_key="groups.id", index=True)
    entrypoint: str = Field()
    kwargs: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    status: TaskStatus = Field(default=TaskStatus.PENDING, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    claimed_at: Optional[datetime] = Field(default=None)
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)
    worker_id: Optional[int] = Field(default=None, foreign_key="workers.id", index=True)
    result_table_id: Optional[int] = Field(default=None)
    log_path: Optional[str] = Field(default=None)
    error: Optional[str] = Field(default=None)


class Worker(SQLModel, table=True):
    """
    Worker model - represents a worker process that executes tasks.

    Workers claim tasks from the queue and execute them.
    """

    __tablename__ = "workers"

    id: int = Field(primary_key=True)
    hostname: str = Field(index=True)
    pid: int = Field()
    status: WorkerStatus = Field(default=WorkerStatus.ACTIVE, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow, index=True)


class Dependency(SQLModel, table=True):
    """
    Dependency model - represents dependencies between tasks and groups.

    Supports all combinations:
    - Task → Task
    - Task → Group
    - Group → Task
    - Group → Group
    """

    __tablename__ = "dependencies"

    # Entity that must complete first
    previous_id: int = Field(primary_key=True, index=True)
    previous_type: str = Field(primary_key=True)  # 'task' or 'group'

    # Entity that waits (executes after previous completes)
    next_id: int = Field(primary_key=True, index=True)
    next_type: str = Field(primary_key=True)  # 'task' or 'group'

    created_at: datetime = Field(default_factory=datetime.utcnow)
