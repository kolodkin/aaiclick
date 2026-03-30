"""TaskResult dataclass and factory helpers for task return values."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TaskResult:
    """Explicit return type for tasks that yield both data and dynamic child tasks.

    Both fields default to None:
    - TaskResult(tasks=[t1, t2])        — tasks only, no data
    - TaskResult(data=value)            — data only, no tasks
    - TaskResult(data=value, tasks=[t]) — both
    """

    data: Any = None
    tasks: list = field(default_factory=list)


def task_result(*, data=None, tasks=None) -> TaskResult:
    """Create a TaskResult with both data and tasks.

    Args:
        data: Return value / output data
        tasks: List of child Task objects

    Returns:
        TaskResult(data=data, tasks=tasks or [])

    Example:
        return task_result(data=my_object, tasks=[t1, t2])
    """
    return TaskResult(data=data, tasks=tasks or [])


def data_list(*data) -> TaskResult:
    """Create a TaskResult carrying only data items.

    Args:
        *data: One or more data values to return

    Returns:
        TaskResult(data=list(data)) when multiple items given,
        TaskResult(data=data[0]) when a single item is given.

    Example:
        return data_list(obj_a, obj_b)
        return data_list(single_obj)
    """
    if len(data) == 1:
        return TaskResult(data=data[0])
    return TaskResult(data=list(data))


def tasks_list(*tasks) -> TaskResult:
    """Create a TaskResult carrying only child tasks.

    Args:
        *tasks: Task objects to schedule as children

    Returns:
        TaskResult(tasks=list(tasks))

    Example:
        return tasks_list(t1, t2, t3)
    """
    return TaskResult(tasks=list(tasks))
