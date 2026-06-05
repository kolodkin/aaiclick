from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.internal_api import tasks as tasks_api
from aaiclick.orchestration.view_models import ClearTaskView, TaskDetail, TaskLogsView

from ..deps import orch_scope
from ..errors import problem_responses

router = APIRouter(prefix="/tasks", tags=["tasks"], dependencies=[Depends(orch_scope)])


@router.get("/{task_id}", response_model=TaskDetail, responses=problem_responses(404))
async def get_task(task_id: int) -> TaskDetail:
    return await tasks_api.get_task(task_id)


@router.get("/{task_id}/logs", response_model=TaskLogsView, responses=problem_responses(404))
async def get_task_logs(task_id: int) -> TaskLogsView:
    return await tasks_api.get_task_logs(task_id)


@router.post("/{task_id}/clear", response_model=ClearTaskView, responses=problem_responses(404))
async def clear_task(task_id: int) -> ClearTaskView:
    return await tasks_api.clear_task(task_id)
