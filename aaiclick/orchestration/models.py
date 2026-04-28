"""
aaiclick.orchestration.models - Data models for orchestration backend.

This module defines SQLModel models for jobs, tasks, workers, groups, and dependencies.
All IDs are snowflake IDs (64-bit integers) generated using aaiclick.snowflake.
"""

import sys
from collections.abc import Sequence
from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, Literal, Union

from sqlalchemy import BigInteger, Boolean, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped
from sqlmodel import JSON, Column, Field, Relationship, SQLModel

from .task_registry import register_task

# Dependency type constants
DEPENDENCY_TASK = "task"
DEPENDENCY_GROUP = "group"
DEPENDENCY_TYPES = [DEPENDENCY_TASK, DEPENDENCY_GROUP]

# Type alias for dependency type annotations (Literal requires hardcoded values)
DependencyType = Literal["task", "group"]

Preserve = list[str] | Literal["*"] | None
"""Job-level table preservation declaration.

- ``None`` — nothing preserved (default; pure task-local semantics).
- ``["foo", "bar"]`` — these named tables survive the run; dropped at job completion.
- ``"*"`` — every ``j_<id>_<name>`` created during the job survives the run.
- ``[]`` — explicit ``no preservation``; does NOT fall through to RegisteredJob default.
"""


# Python 3.10 compatibility: StrEnum was added in 3.11
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:

    class StrEnum(str, Enum):
        """String Enum for Python 3.10 compatibility."""

        pass


class RunType(StrEnum):
    """How a job run was triggered."""

    SCHEDULED = "SCHEDULED"
    MANUAL = "MANUAL"


class JobStatus(StrEnum):
    """Job execution status."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskStatus(StrEnum):
    """Task execution status."""

    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    PENDING_CLEANUP = "PENDING_CLEANUP"


class WorkerStatus(StrEnum):
    """Worker status."""

    ACTIVE = "ACTIVE"
    IDLE = "IDLE"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


class PreservationMode(StrEnum):
    """Which tables survive after a job completes.

    - ``NONE``: persistent tables only (default) — intermediate tables are
      dropped as soon as their refs fall to zero.
    - ``FULL``: every table the job produced stays until the job TTL expires,
      useful for development and debugging.
    """

    NONE = "NONE"
    FULL = "FULL"


class RegisteredJob(SQLModel, table=True):
    """
    RegisteredJob model - catalog of known jobs.

    Stores the job definition (entrypoint, schedule, defaults) separately
    from individual job runs. The background worker uses cron schedules
    to create Job rows automatically.
    """

    __tablename__: ClassVar[str] = "registered_jobs"
    __table_args__ = (UniqueConstraint("name"),)

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    name: str = Field(index=True)
    entrypoint: str = Field()
    enabled: bool = Field(sa_column=Column(Boolean, nullable=False, server_default="1"), default=True)
    schedule: str | None = Field(default=None)
    default_kwargs: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON, nullable=True))
    preservation_mode: PreservationMode | None = Field(default=None)
    next_run_at: datetime | None = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Job(SQLModel, table=True):
    """
    Job model - represents a workflow execution.

    Each job has a unique snowflake ID and tracks overall workflow status.
    """

    __tablename__: ClassVar[str] = "jobs"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    name: str = Field(index=True)
    status: JobStatus = Field(default=JobStatus.PENDING, index=True)
    run_type: RunType = Field()
    registered_job_id: int | None = Field(
        default=None,
        sa_column=Column(BigInteger, ForeignKey("registered_jobs.id"), nullable=True, index=True),
    )
    preservation_mode: PreservationMode = Field(
        default=PreservationMode.NONE,
        sa_column_kwargs={"server_default": PreservationMode.NONE.value, "nullable": False},
    )
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    started_at: datetime | None = Field(default=None)
    completed_at: datetime | None = Field(default=None)
    error: str | None = Field(default=None)


class Worker(SQLModel, table=True):
    """
    Worker model - represents a worker process that executes tasks.

    Workers claim tasks from the queue and execute them.
    """

    __tablename__: ClassVar[str] = "workers"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    hostname: str = Field(index=True)
    pid: int = Field()
    status: WorkerStatus = Field(default=WorkerStatus.ACTIVE, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime = Field(default_factory=datetime.utcnow)
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow, index=True)
    tasks_completed: int = Field(default=0)
    tasks_failed: int = Field(default=0)


class Dependency(SQLModel, table=True):
    """
    Dependency model - represents dependencies between tasks and groups.

    Supports all combinations:
    - Task → Task
    - Task → Group
    - Group → Task
    - Group → Group
    """

    __tablename__: ClassVar[str] = "dependencies"

    # Entity that must complete first
    previous_id: int = Field(sa_column=Column(BigInteger, primary_key=True, index=True))
    previous_type: str = Field(sa_column=Column(String, primary_key=True))

    # Entity that waits (executes after previous completes)
    next_id: int = Field(sa_column=Column(BigInteger, primary_key=True, index=True))
    next_type: str = Field(sa_column=Column(String, primary_key=True))

    created_at: datetime = Field(default_factory=datetime.utcnow)


class Group(SQLModel, table=True):
    """
    Group model - represents a logical grouping of tasks.

    Groups support nesting via parent_group_id.
    Non-DB attribute ``_tasks`` carries associated Task objects so that
    returning a Group from a @task/@job function also registers its tasks.
    """

    __tablename__: ClassVar[str] = "groups"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    job_id: int = Field(default=0, sa_column=Column(BigInteger, ForeignKey("jobs.id"), index=True))
    parent_group_id: int | None = Field(
        default=None, sa_column=Column(BigInteger, ForeignKey("groups.id"), index=True, nullable=True)
    )
    name: str = Field()
    created_at: datetime = Field(default_factory=datetime.utcnow)

    _tasks: list = []
    _result_task: Any = None

    def model_post_init(self, __context: Any) -> None:
        self._tasks = []
        self._result_task = None
        register_task(self.id, self)

    def add_task(self, task: "Task") -> None:
        """Attach a Task to this group for co-registration."""
        self._tasks.append(task)

    def get_tasks(self) -> list["Task"]:
        """Return tasks attached to this group (non-DB)."""
        return self._tasks

    # Dependencies where this group is the "next" (i.e., this group depends on previous)
    # Note: overlaps="previous_dependencies" tells SQLAlchemy that both Task and Group
    # intentionally write to Dependency.next_id (polymorphic design via next_type)
    previous_dependencies: Mapped[list[Dependency]] = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "and_(Group.id == foreign(Dependency.next_id), Dependency.next_type == 'group')",
            "cascade": "all, delete-orphan",
            "overlaps": "previous_dependencies",
        }
    )

    def depends_on(self, other: Union["Task", "Group"]) -> "Group":
        """
        Declare that this group depends on a task or another group.

        Creates a Dependency record that will be committed when commit_tasks() is called.

        Args:
            other: Task or Group that must complete before tasks in this group

        Returns:
            self (for chaining)
        """
        dependency = Dependency(
            previous_id=other.id,
            previous_type=DEPENDENCY_TASK if isinstance(other, Task) else DEPENDENCY_GROUP,
            next_id=self.id,
            next_type=DEPENDENCY_GROUP,
        )
        self.previous_dependencies.append(dependency)
        return self

    def __rshift__(
        self, other: Union["Task", "Group", Sequence[Union["Task", "Group"]]]
    ) -> Union["Task", "Group", Sequence[Union["Task", "Group"]]]:
        """A >> B: B depends on A (A executes before B)."""
        if isinstance(other, (Task, Group)):
            other.depends_on(self)
            return other
        for item in other:
            item.depends_on(self)
        return other

    def __lshift__(self, other: Union["Task", "Group", list[Union["Task", "Group"]]]) -> "Group":
        """A << B: A depends on B (B executes before A)."""
        if isinstance(other, list):
            for item in other:
                self.depends_on(item)
        else:
            self.depends_on(other)
        return self

    def __rrshift__(self, other: Union["Task", "Group", Sequence[Union["Task", "Group"]]]) -> "Group":
        """Reverse: [A, B] >> C means C depends on A and B (fan-in)."""
        if isinstance(other, list):
            for item in other:
                self.depends_on(item)
        else:
            self.depends_on(other)
        return self

    def __rlshift__(
        self, other: Union["Task", "Group", list[Union["Task", "Group"]]]
    ) -> Union["Task", "Group", list[Union["Task", "Group"]]]:
        """Reverse: [A, B] << C means A and B depend on C (fan-out)."""
        if isinstance(other, list):
            for item in other:
                item.depends_on(self)
            return other
        else:
            other.depends_on(self)
            return other


class Task(SQLModel, table=True):
    """
    Task model - represents a unit of work to be executed.

    Tasks can be part of a group and have dependencies on other tasks/groups.
    """

    __tablename__: ClassVar[str] = "tasks"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    job_id: int = Field(default=0, sa_column=Column(BigInteger, ForeignKey("jobs.id"), index=True))
    group_id: int | None = Field(
        default=None, sa_column=Column(BigInteger, ForeignKey("groups.id"), index=True, nullable=True)
    )
    entrypoint: str = Field()
    name: str = Field()
    kwargs: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    status: TaskStatus = Field(default=TaskStatus.PENDING, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    claimed_at: datetime | None = Field(default=None)
    started_at: datetime | None = Field(default=None)
    completed_at: datetime | None = Field(default=None)
    worker_id: int | None = Field(
        default=None, sa_column=Column(BigInteger, ForeignKey("workers.id"), index=True, nullable=True)
    )
    result: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON, nullable=True))
    log_path: str | None = Field(default=None)
    error: str | None = Field(default=None)
    max_retries: int = Field(default=0)
    attempt: int = Field(default=0)
    retry_after: datetime | None = Field(default=None)
    run_ids: list[int] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, server_default="[]"))
    run_statuses: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, server_default="[]"))

    def model_post_init(self, __context: Any) -> None:
        register_task(self.id, self)

    # Dependencies where this task is the "next" (i.e., this task depends on previous)
    # Note: overlaps="previous_dependencies" tells SQLAlchemy that both Task and Group
    # intentionally write to Dependency.next_id (polymorphic design via next_type)
    previous_dependencies: Mapped[list[Dependency]] = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "and_(Task.id == foreign(Dependency.next_id), Dependency.next_type == 'task')",
            "cascade": "all, delete-orphan",
            "overlaps": "previous_dependencies",
        }
    )

    def depends_on(self, other: Union["Task", "Group"]) -> "Task":
        """
        Declare that this task depends on another task or group.

        Creates a Dependency record that will be committed when commit_tasks() is called.

        Args:
            other: Task or Group that must complete before this task

        Returns:
            self (for chaining)
        """
        dependency = Dependency(
            previous_id=other.id,
            previous_type=DEPENDENCY_TASK if isinstance(other, Task) else DEPENDENCY_GROUP,
            next_id=self.id,
            next_type=DEPENDENCY_TASK,
        )
        self.previous_dependencies.append(dependency)
        return self

    def __rshift__(
        self, other: Union["Task", "Group", Sequence[Union["Task", "Group"]]]
    ) -> Union["Task", "Group", Sequence[Union["Task", "Group"]]]:
        """A >> B: B depends on A (A executes before B)."""
        if isinstance(other, (Task, Group)):
            other.depends_on(self)
            return other
        for item in other:
            item.depends_on(self)
        return other

    def __lshift__(self, other: Union["Task", "Group", list[Union["Task", "Group"]]]) -> "Task":
        """A << B: A depends on B (B executes before A)."""
        if isinstance(other, list):
            for item in other:
                self.depends_on(item)
        else:
            self.depends_on(other)
        return self

    def __rrshift__(self, other: Union["Task", "Group", Sequence[Union["Task", "Group"]]]) -> "Task":
        """Reverse: [A, B] >> C means C depends on A and B (fan-in)."""
        if isinstance(other, list):
            for item in other:
                self.depends_on(item)
        else:
            self.depends_on(other)
        return self

    def __rlshift__(
        self, other: Union["Task", "Group", list[Union["Task", "Group"]]]
    ) -> Union["Task", "Group", list[Union["Task", "Group"]]]:
        """Reverse: [A, B] << C means A and B depend on C (fan-out)."""
        if isinstance(other, list):
            for item in other:
                item.depends_on(self)
            return other
        else:
            other.depends_on(self)
            return other


# Type alias for tasks/groups that can be applied
TasksType = Task | Group | list[Task | Group]
