"""REST router for task commands — paths relative to ``/api/v0``."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.internal_api import tasks as tasks_api
from aaiclick.orchestration.view_models import TaskDetail

from ..deps import orch_scope

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("/{task_id}", response_model=TaskDetail)
async def get_task(
    task_id: int,
    _scope: None = Depends(orch_scope),
) -> TaskDetail:
    return await tasks_api.get_task(task_id)
