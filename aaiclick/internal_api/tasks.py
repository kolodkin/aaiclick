"""Internal API for task commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

from sqlmodel import select

from aaiclick.log_models import MAX_TASK_LOG_LINES
from aaiclick.orchestration.execution import claiming
from aaiclick.orchestration.logging import read_task_logs
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.view_models import (
    ClearTaskView,
    TaskAttemptView,
    TaskDetail,
    TaskLogsView,
    clear_to_view,
    task_to_detail,
)

from .errors import NotFound


async def _require_visible_task(task_id: int) -> Task:
    async with get_sql_session() as session:
        task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
    if task is None:
        raise NotFound(f"Task not found: {task_id}")
    return task


async def get_task(task_id: int) -> TaskDetail:
    """Return full task detail by numeric ID.

    Raises ``NotFound`` if no task matches ``task_id``.
    """
    return task_to_detail(await _require_visible_task(task_id))


async def get_task_logs(task_id: int, tail: int = MAX_TASK_LOG_LINES, attempt: int | None = None) -> TaskLogsView:
    """Return the last ``tail`` captured log lines of one run of a task.

    Reads the ClickHouse ``task_logs`` stream, so logs are available whichever
    host ran the task. ``attempt`` is 1-based over ``Task.run_ids`` and
    defaults to the latest run. Returns ``available=False`` when the task has
    not run yet or the chosen run produced no output.

    Raises ``NotFound`` if no task matches ``task_id``, or ``attempt`` is
    outside the task's recorded runs.
    """
    task = await _require_visible_task(task_id)

    if attempt is None and not task.run_ids:
        return TaskLogsView(available=False)

    selected = attempt or len(task.run_ids)
    if not 1 <= selected <= len(task.run_ids):
        raise NotFound(f"Task {task_id} has no attempt {attempt}")

    lines = await read_task_logs(task_id, task.run_ids[selected - 1], tail=tail)
    attempts = [TaskAttemptView(attempt=i, status=status) for i, status in enumerate(task.run_statuses, start=1)]
    return TaskLogsView(available=bool(lines), lines=lines, attempt=selected, attempts=attempts)


async def clear_task(task_id: int) -> ClearTaskView:
    """Reset a task and all its downstream tasks to PENDING for re-run.

    Upstream tasks and their output tables are left untouched; a terminal job
    is reactivated so the cleared tasks run again. Raises ``NotFound`` if no
    task matches ``task_id``.
    """
    await _require_visible_task(task_id)
    try:
        cleared_ids, job = await claiming.clear_task(task_id)
    except claiming.TaskNotFound as exc:
        raise NotFound(str(exc)) from exc
    return clear_to_view(job, cleared_ids)
