"""
aaiclick.orchestration.models - Data models for orchestration backend.

This module defines SQLModel models for jobs, tasks, workers, groups, and dependencies.
All IDs are snowflake IDs (64-bit integers) generated using aaiclick.snowflake.
Worker IDs are UUID strings (no chdb dependency for worker registration).
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

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


# Python 3.10 compatibility: StrEnum was added in 3.11
try:
    from enum import StrEnum
except ImportError:

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


class WorkerStatus(StrEnum):
    """Worker status."""

    ACTIVE = "ACTIVE"
    IDLE = "IDLE"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


class RegisteredJob(SQLModel, table=True):
    """
    RegisteredJob model - catalog of known jobs.

    Stores the job definition (entrypoint, schedule, defaults) separately
    from individual job runs. The background worker uses cron schedules
    to create Job rows automatically.
    """

    __tablename__ = "registered_jobs"
    __table_args__ = (UniqueConstraint("name"),)

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    name: str = Field(index=True)
    entrypoint: str = Field()
    enabled: bool = Field(sa_column=Column(Boolean, nullable=False, server_default="1"), default=True)
    schedule: Optional[str] = Field(default=None)
    default_kwargs: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON, nullable=True))
    next_run_at: Optional[datetime] = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Job(SQLModel, table=True):
    """
    Job model - represents a workflow execution.

    Each job has a unique snowflake ID and tracks overall workflow status.
    """

    __tablename__ = "jobs"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    name: str = Field(index=True)
    status: JobStatus = Field(default=JobStatus.PENDING, index=True)
    run_type: RunType = Field()
    registered_job_id: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger, ForeignKey("registered_jobs.id"), nullable=True, index=True),
    )
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)
    error: Optional[str] = Field(default=None)


class Worker(SQLModel, table=True):
    """
    Worker model - represents a worker process that executes tasks.

    Workers claim tasks from the queue and execute them.
    """

    __tablename__ = "workers"

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

    __tablename__ = "dependencies"

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

    __tablename__ = "groups"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    job_id: int = Field(sa_column=Column(BigInteger, ForeignKey("jobs.id"), index=True))
    parent_group_id: Optional[int] = Field(default=None, sa_column=Column(BigInteger, ForeignKey("groups.id"), index=True, nullable=True))
    name: str = Field()
    created_at: datetime = Field(default_factory=datetime.utcnow)

    _tasks: list = []

    def model_post_init(self, __context: Any) -> None:
        self._tasks = []
        register_task(self.id, self)

    def add_task(self, task: "Task") -> None:
        """Attach a Task to this group for co-registration."""
        self._tasks.append(task)

    def get_tasks(self) -> List["Task"]:
        """Return tasks attached to this group (non-DB)."""
        return self._tasks

    # Dependencies where this group is the "next" (i.e., this group depends on previous)
    # Note: overlaps="previous_dependencies" tells SQLAlchemy that both Task and Group
    # intentionally write to Dependency.next_id (polymorphic design via next_type)
    previous_dependencies: Mapped[List[Dependency]] = Relationship(
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

    def __rshift__(self, other: Union["Task", "Group", List[Union["Task", "Group"]]]) -> Union["Task", "Group", List[Union["Task", "Group"]]]:
        """A >> B: B depends on A (A executes before B)."""
        if isinstance(other, list):
            for item in other:
                item.depends_on(self)
            return other
        else:
            other.depends_on(self)
            return other

    def __lshift__(self, other: Union["Task", "Group", List[Union["Task", "Group"]]]) -> "Group":
        """A << B: A depends on B (B executes before A)."""
        if isinstance(other, list):
            for item in other:
                self.depends_on(item)
        else:
            self.depends_on(other)
        return self

    def __rrshift__(self, other: Union["Task", "Group", List[Union["Task", "Group"]]]) -> "Group":
        """Reverse: [A, B] >> C means C depends on A and B (fan-in)."""
        if isinstance(other, list):
            for item in other:
                self.depends_on(item)
        else:
            self.depends_on(other)
        return self

    def __rlshift__(self, other: Union["Task", "Group", List[Union["Task", "Group"]]]) -> Union["Task", "Group", List[Union["Task", "Group"]]]:
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

    __tablename__ = "tasks"

    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    job_id: int = Field(sa_column=Column(BigInteger, ForeignKey("jobs.id"), index=True))
    group_id: Optional[int] = Field(default=None, sa_column=Column(BigInteger, ForeignKey("groups.id"), index=True, nullable=True))
    entrypoint: str = Field()
    name: str = Field()
    kwargs: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    status: TaskStatus = Field(default=TaskStatus.PENDING, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    claimed_at: Optional[datetime] = Field(default=None)
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)
    worker_id: Optional[int] = Field(default=None, sa_column=Column(BigInteger, ForeignKey("workers.id"), index=True, nullable=True))
    result: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON, nullable=True))
    log_path: Optional[str] = Field(default=None)
    error: Optional[str] = Field(default=None)
    max_retries: int = Field(default=0)
    attempt: int = Field(default=0)
    retry_after: Optional[datetime] = Field(default=None)
    run_ids: List[int] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, server_default="[]"))
    run_statuses: List[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, server_default="[]"))

    def model_post_init(self, __context: Any) -> None:
        register_task(self.id, self)

    # Dependencies where this task is the "next" (i.e., this task depends on previous)
    # Note: overlaps="previous_dependencies" tells SQLAlchemy that both Task and Group
    # intentionally write to Dependency.next_id (polymorphic design via next_type)
    previous_dependencies: Mapped[List[Dependency]] = Relationship(
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

    def __rshift__(self, other: Union["Task", "Group", List[Union["Task", "Group"]]]) -> Union["Task", "Group", List[Union["Task", "Group"]]]:
        """A >> B: B depends on A (A executes before B)."""
        if isinstance(other, list):
            for item in other:
                item.depends_on(self)
            return other
        else:
            other.depends_on(self)
            return other

    def __lshift__(self, other: Union["Task", "Group", List[Union["Task", "Group"]]]) -> "Task":
        """A << B: A depends on B (B executes before A)."""
        if isinstance(other, list):
            for item in other:
                self.depends_on(item)
        else:
            self.depends_on(other)
        return self

    def __rrshift__(self, other: Union["Task", "Group", List[Union["Task", "Group"]]]) -> "Task":
        """Reverse: [A, B] >> C means C depends on A and B (fan-in)."""
        if isinstance(other, list):
            for item in other:
                self.depends_on(item)
        else:
            self.depends_on(other)
        return self

    def __rlshift__(self, other: Union["Task", "Group", List[Union["Task", "Group"]]]) -> Union["Task", "Group", List[Union["Task", "Group"]]]:
        """Reverse: [A, B] << C means A and B depend on C (fan-out)."""
        if isinstance(other, list):
            for item in other:
                item.depends_on(self)
            return other
        else:
            other.depends_on(self)
            return other


# Type alias for tasks/groups that can be applied
TasksType = Union[Task, Group, List[Union[Task, Group]]]
