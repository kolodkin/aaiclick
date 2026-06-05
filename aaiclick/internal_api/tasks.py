"""Internal API for task commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

import os

from aaiclick.orchestration.execution import claiming
from aaiclick.orchestration.jobs.queries import get_task as _get_task_impl
from aaiclick.orchestration.view_models import (
    ClearTaskView,
    TaskDetail,
    TaskLogsView,
    clear_to_view,
    task_to_detail,
)

from .errors import NotFound


async def get_task(task_id: int) -> TaskDetail:
    """Return full task detail by numeric ID.

    Raises ``NotFound`` if no task matches ``task_id``.
    """
    task = await _get_task_impl(task_id)
    if task is None:
        raise NotFound(f"Task not found: {task_id}")
    return task_to_detail(task)


async def get_task_logs(task_id: int) -> TaskLogsView:
    """Return captured log lines for a task.

    Reads the file at ``task.log_path``. Returns ``available=False`` when the
    task has no log path or the file is not present on this process's
    filesystem (distributed / docker runs).

    Raises ``NotFound`` if no task matches ``task_id``.
    """
    task = await _get_task_impl(task_id)
    if task is None:
        raise NotFound(f"Task not found: {task_id}")

    log_path = task.log_path
    if not log_path or not os.path.isfile(log_path):
        return TaskLogsView(available=False, log_path=log_path)

    with open(log_path, encoding="utf-8", errors="replace") as f:
        lines = f.read().splitlines()
    return TaskLogsView(available=True, log_path=log_path, lines=lines)


async def clear_task(task_id: int) -> ClearTaskView:
    """Reset a task and all its downstream tasks to PENDING for re-run.

    Upstream tasks and their output tables are left untouched; a terminal job
    is reactivated so the cleared tasks run again. Raises ``NotFound`` if no
    task matches ``task_id``.
    """
    try:
        cleared_ids, job = await claiming.clear_task(task_id)
    except claiming.TaskNotFound as exc:
        raise NotFound(str(exc)) from exc
    return clear_to_view(job, cleared_ids)
