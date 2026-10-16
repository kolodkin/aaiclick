"""ExecutionWorker execution context for passing task metadata to running tasks.

Provides a ContextVar-based mechanism for the execution_worker loop to make the
current task's metadata (job_id, task_id) available to the executing
function. This is used by expander tasks (dynamic.py) to know which
job they belong to when creating child tasks.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass

from ..runner_config import ENTRY_MODULE, EntryType


@dataclass
class TaskInfo:
    """Metadata about the currently executing task."""

    task_id: int
    job_id: int
    # The executing task's own image_source, carried so commit_tasks can stamp
    # inheritance onto dynamic children without re-fetching the task row.
    image_source: dict | None = None
    # The executing task's group: map()/reduce() expanders nest their parts in it.
    group_id: int | None = None
    # The executing task's entry type: image inheritance depends on it (a JVM
    # image has no Python, a Python image no JVM).
    entry_type: EntryType = ENTRY_MODULE


_current_task_info: ContextVar[TaskInfo] = ContextVar("current_task_info")


def set_current_task_info(
    task_id: int,
    job_id: int,
    image_source: dict | None = None,
    group_id: int | None = None,
    entry_type: EntryType = ENTRY_MODULE,
) -> None:
    """Set the current task info for the executing context."""
    _current_task_info.set(
        TaskInfo(task_id=task_id, job_id=job_id, image_source=image_source, group_id=group_id, entry_type=entry_type)
    )


def get_current_task_info() -> TaskInfo:
    """Get the current task info.

    Raises:
        RuntimeError: If called outside of task execution context.
    """
    try:
        return _current_task_info.get()
    except LookupError as err:
        raise RuntimeError(
            "No task is currently executing. get_current_task_info() can only be called during task execution."
        ) from err
