"""REST router for job commands — paths relative to ``/api/v0``."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.internal_api import jobs as jobs_api
from aaiclick.orchestration.view_models import JobDetail, JobStatsView, JobView
from aaiclick.view_models import JobListFilter, Page, RefId, RunJobRequest

from ..deps import orch_scope, orch_scope_with_ch

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("", response_model=Page[JobView])
async def list_jobs(
    filter: JobListFilter = Depends(),
    _scope: None = Depends(orch_scope),
) -> Page[JobView]:
    return await jobs_api.list_jobs(filter)


@router.post(":run", response_model=JobView, status_code=201)
async def run_job(
    request: RunJobRequest,
    _scope: None = Depends(orch_scope_with_ch),
) -> JobView:
    return await jobs_api.run_job(request)


@router.get("/{ref}", response_model=JobDetail)
async def get_job(
    ref: RefId,
    _scope: None = Depends(orch_scope),
) -> JobDetail:
    return await jobs_api.get_job(ref)


@router.get("/{ref}/stats", response_model=JobStatsView)
async def job_stats(
    ref: RefId,
    _scope: None = Depends(orch_scope),
) -> JobStatsView:
    return await jobs_api.job_stats(ref)


@router.post("/{ref}/cancel", response_model=JobView)
async def cancel_job(
    ref: RefId,
    _scope: None = Depends(orch_scope),
) -> JobView:
    return await jobs_api.cancel_job(ref)
