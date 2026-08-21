"""Internal API for task commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

from sqlmodel import select

from aaiclick.orchestration.execution import claiming
from aaiclick.orchestration.jobs.queries import get_task as _get_task_impl
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


async def _visible_task(task_id: int) -> Task | None:
    """Load a task only if its job belongs to the active tenant.

    A cross-tenant task reads as missing (``None``) — no existence leak.
    """
    task = await _get_task_impl(task_id)
    if task is None:
        return None
    async with get_sql_session() as session:
        job = (await session.execute(select(Job).where(Job.id == task.job_id))).scalar_one_or_none()
    if job is None or job.tenant_id != get_active_tenant_id():
        return None
    return task


async def get_task(task_id: int) -> TaskDetail:
    """Return full task detail by numeric ID.

    Raises ``NotFound`` if no task matches ``task_id``.
    """
    task = await _visible_task(task_id)
    if task is None:
        raise NotFound(f"Task not found: {task_id}")
    return task_to_detail(task)


async def get_task_logs(task_id: int, tail: int | None = None) -> TaskLogsView:
    """Return captured log lines for a task's latest run.

    Reads the ClickHouse ``task_logs`` stream written by the task process, so
    logs are available regardless of which host ran the task (local, docker, or
    kubernetes). When ``tail`` is given, returns only the last ``tail`` lines.
    Returns ``available=False`` when the task has not run yet or its latest run
    produced no captured output.

    Raises ``NotFound`` if no task matches ``task_id``.
    """
    task = await _visible_task(task_id)
    if task is None:
        raise NotFound(f"Task not found: {task_id}")

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
    if await _visible_task(task_id) is None:
        raise NotFound(f"Task not found: {task_id}")
    try:
        cleared_ids, job = await claiming.clear_task(task_id)
    except claiming.TaskNotFound as exc:
        raise NotFound(str(exc)) from exc
    return clear_to_view(job, cleared_ids)
