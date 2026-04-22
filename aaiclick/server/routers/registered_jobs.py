from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.internal_api import registered_jobs as rj_api
from aaiclick.orchestration.view_models import RegisteredJobView
from aaiclick.view_models import Page, RegisteredJobFilter, RegisterJobRequest

from ..deps import orch_scope

router = APIRouter(prefix="/registered-jobs", tags=["registered-jobs"], dependencies=[Depends(orch_scope)])


@router.get("", response_model=Page[RegisteredJobView])
async def list_registered_jobs(
    filter: RegisteredJobFilter = Depends(),
) -> Page[RegisteredJobView]:
    return await rj_api.list_registered_jobs(filter)


@router.post("", response_model=RegisteredJobView, status_code=201)
async def register_job(request: RegisterJobRequest) -> RegisteredJobView:
    return await rj_api.register_job(request)


@router.post("/{name}/enable", response_model=RegisteredJobView)
async def enable_job(name: str) -> RegisteredJobView:
    return await rj_api.enable_job(name)


@router.post("/{name}/disable", response_model=RegisteredJobView)
async def disable_job(name: str) -> RegisteredJobView:
    return await rj_api.disable_job(name)
