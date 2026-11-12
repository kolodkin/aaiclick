from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.internal_api import execution_workers as execution_workers_api
from aaiclick.orchestration.view_models import ExecutionWorkerView
from aaiclick.view_models import ExecutionWorkerFilter, Page

from ..deps import orch_scope

router = APIRouter(prefix="/execution-workers", tags=["execution-workers"], dependencies=[Depends(orch_scope)])


@router.get("", response_model=Page[ExecutionWorkerView])
async def list_execution_workers(filter: ExecutionWorkerFilter = Depends()) -> Page[ExecutionWorkerView]:
    return await execution_workers_api.list_execution_workers(filter)
