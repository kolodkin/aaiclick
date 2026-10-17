from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from aaiclick.internal_api import tasks as tasks_api
from aaiclick.log_models import MAX_TASK_LOG_LINES
from aaiclick.orchestration.view_models import ClearTaskView, TaskDetail, TaskLogsView

from ..auth import require_admin
from ..deps import orch_scope, orch_scope_with_ch
from ..errors import problem_responses

# Mixed scopes (like jobs.py): the logs endpoint reads the ClickHouse
# ``task_logs`` stream so it needs the CH-enabled scope; the others are SQL-only.
router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get(
    "/{task_id}",
    response_model=TaskDetail,
    responses=problem_responses(404),
    dependencies=[Depends(orch_scope)],
)
async def get_task(task_id: int) -> TaskDetail:
    return await tasks_api.get_task(task_id)


@router.get(
    "/{task_id}/logs",
    response_model=TaskLogsView,
    responses=problem_responses(404),
    dependencies=[Depends(orch_scope_with_ch)],
)
async def get_task_logs(
    task_id: int,
    tail: int = Query(
        default=MAX_TASK_LOG_LINES, ge=1, le=MAX_TASK_LOG_LINES, description="Return only the last N log lines."
    ),
    attempt: int | None = Query(default=None, ge=1, description="1-based run to read; defaults to the latest."),
) -> TaskLogsView:
    return await tasks_api.get_task_logs(task_id, tail=tail, attempt=attempt)


@router.post(
    "/{task_id}/clear",
    response_model=ClearTaskView,
    responses=problem_responses(403, 404),
    dependencies=[Depends(orch_scope), Depends(require_admin)],
)
async def clear_task(task_id: int) -> ClearTaskView:
    return await tasks_api.clear_task(task_id)
