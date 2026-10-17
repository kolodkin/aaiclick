from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.internal_api import jobs as jobs_api
from aaiclick.orchestration.view_models import JobDetail, JobGraphView, JobStatsView, JobView
from aaiclick.view_models import JobListFilter, Page, RefId, RunJobRequest

from ..auth import require_admin
from ..deps import orch_scope, orch_scope_with_ch
from ..errors import problem_responses

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("", response_model=Page[JobView], dependencies=[Depends(orch_scope)])
async def list_jobs(filter: JobListFilter = Depends()) -> Page[JobView]:
    return await jobs_api.list_jobs(filter)


@router.post(
    ":run",
    response_model=JobView,
    status_code=201,
    dependencies=[Depends(orch_scope_with_ch), Depends(require_admin)],
    responses=problem_responses(403),
)
async def run_job(request: RunJobRequest) -> JobView:
    return await jobs_api.run_job(request)


@router.get(
    "/{ref}",
    response_model=JobDetail,
    responses=problem_responses(404),
    dependencies=[Depends(orch_scope)],
)
async def get_job(ref: RefId) -> JobDetail:
    return await jobs_api.get_job(ref)


@router.get(
    "/{ref}/stats",
    response_model=JobStatsView,
    responses=problem_responses(404),
    dependencies=[Depends(orch_scope)],
)
async def job_stats(ref: RefId) -> JobStatsView:
    return await jobs_api.job_stats(ref)


@router.get(
    "/{ref}/graph",
    response_model=JobGraphView,
    responses=problem_responses(404),
    dependencies=[Depends(orch_scope)],
)
async def job_graph(ref: RefId) -> JobGraphView:
    return await jobs_api.get_job_graph(ref)


@router.post(
    "/{ref}/cancel",
    response_model=JobView,
    responses=problem_responses(403, 404, 409),
    dependencies=[Depends(orch_scope), Depends(require_admin)],
)
async def cancel_job(ref: RefId) -> JobView:
    return await jobs_api.cancel_job(ref)
