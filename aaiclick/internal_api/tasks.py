"""Internal API for task commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

from aaiclick.orchestration.jobs.queries import get_task as _get_task_impl
from aaiclick.orchestration.view_models import TaskDetail, task_to_detail

from .errors import NotFound


async def get_task(task_id: int) -> TaskDetail:
    """Return full task detail by numeric ID.

    Raises ``NotFound`` if no task matches ``task_id``.
    """
    task = await _get_task_impl(task_id)
    if task is None:
        raise NotFound(f"Task not found: {task_id}")
    return task_to_detail(task)
