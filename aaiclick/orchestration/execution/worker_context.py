"""Worker execution context for passing task metadata to running tasks.

Provides a ContextVar-based mechanism for the worker loop to make the
current task's metadata (job_id, task_id) available to the executing
function. This is used by expander tasks (dynamic.py) to know which
job they belong to when creating child tasks.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass


@dataclass
class TaskInfo:
    """Metadata about the currently executing task."""

    task_id: int
    job_id: int


_current_task_info: ContextVar[TaskInfo] = ContextVar('current_task_info')


def set_current_task_info(task_id: int, job_id: int) -> None:
    """Set the current task info for the executing context."""
    _current_task_info.set(TaskInfo(task_id=task_id, job_id=job_id))


def get_current_task_info() -> TaskInfo:
    """Get the current task info.

    Raises:
        RuntimeError: If called outside of task execution context.
    """
    try:
        return _current_task_info.get()
    except LookupError as err:
        raise RuntimeError(
            "No task is currently executing. "
            "get_current_task_info() can only be called during task execution."
        ) from err
