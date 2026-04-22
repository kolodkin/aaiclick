from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.internal_api import tasks as tasks_api
from aaiclick.orchestration.view_models import TaskDetail

from ..deps import orch_scope

router = APIRouter(prefix="/tasks", tags=["tasks"], dependencies=[Depends(orch_scope)])


@router.get("/{task_id}", response_model=TaskDetail)
async def get_task(task_id: int) -> TaskDetail:
    return await tasks_api.get_task(task_id)
