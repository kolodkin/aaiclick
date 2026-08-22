"""Internal API for task commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

from sqlmodel import col, select

from aaiclick.orchestration.execution import claiming
from aaiclick.orchestration.logging import read_task_logs
from aaiclick.orchestration.models import Job, Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.view_models import (
    ClearTaskView,
    TaskDetail,
    TaskLogsView,
    clear_to_view,
    task_to_detail,
)
from aaiclick.tenancy import get_active_tenant_id

from .errors import NotFound


async def _require_visible_task(task_id: int) -> Task:
    """Load a task, joined to its job so tenancy is inherited in one query.

    A task outside the active tenant reads as missing — no existence leak.
    """
    async with get_sql_session() as session:
        task = (
            await session.execute(
                select(Task)
                .join(Job, col(Job.id) == col(Task.job_id))
                .where(Task.id == task_id, Job.tenant_id == get_active_tenant_id())
            )
        ).scalar_one_or_none()
    if task is None:
        raise NotFound(f"Task not found: {task_id}")
    return task


async def get_task(task_id: int) -> TaskDetail:
    """Return full task detail by numeric ID.

    Raises ``NotFound`` if no task matches ``task_id``.
    """
    return task_to_detail(await _require_visible_task(task_id))


async def get_task_logs(task_id: int, tail: int | None = None) -> TaskLogsView:
    """Return captured log lines for a task's latest run.

    Reads the ClickHouse ``task_logs`` stream written by the task process, so
    logs are available regardless of which host ran the task (local, docker, or
    kubernetes). When ``tail`` is given, returns only the last ``tail`` lines.
    Returns ``available=False`` when the task has not run yet or its latest run
    produced no captured output.

    Raises ``NotFound`` if no task matches ``task_id``.
    """
    task = await _require_visible_task(task_id)

    if not task.run_ids:
        return TaskLogsView(available=False)

    lines = await read_task_logs(task_id, task.run_ids[-1], tail=tail)
    return TaskLogsView(available=bool(lines), lines=lines)


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
