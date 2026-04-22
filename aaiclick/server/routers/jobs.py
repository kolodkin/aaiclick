from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.internal_api import jobs as jobs_api
from aaiclick.orchestration.view_models import JobDetail, JobStatsView, JobView
from aaiclick.view_models import JobListFilter, Page, RefId, RunJobRequest

from ..deps import orch_scope, orch_scope_with_ch

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("", response_model=Page[JobView], dependencies=[Depends(orch_scope)])
async def list_jobs(filter: JobListFilter = Depends()) -> Page[JobView]:
    return await jobs_api.list_jobs(filter)


@router.post(":run", response_model=JobView, status_code=201, dependencies=[Depends(orch_scope_with_ch)])
async def run_job(request: RunJobRequest) -> JobView:
    return await jobs_api.run_job(request)


@router.get("/{ref}", response_model=JobDetail, dependencies=[Depends(orch_scope)])
async def get_job(ref: RefId) -> JobDetail:
    return await jobs_api.get_job(ref)


@router.get("/{ref}/stats", response_model=JobStatsView, dependencies=[Depends(orch_scope)])
async def job_stats(ref: RefId) -> JobStatsView:
    return await jobs_api.job_stats(ref)


@router.post("/{ref}/cancel", response_model=JobView, dependencies=[Depends(orch_scope)])
async def cancel_job(ref: RefId) -> JobView:
    return await jobs_api.cancel_job(ref)
