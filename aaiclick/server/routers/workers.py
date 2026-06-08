from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response

from aaiclick.internal_api import workers as workers_api
from aaiclick.orchestration.view_models import WorkerView
from aaiclick.view_models import Page, StartWorkerRequest, WorkerFilter

from ..deps import orch_scope
from ..errors import problem_responses

router = APIRouter(prefix="/workers", tags=["workers"], dependencies=[Depends(orch_scope)])


@router.get("", response_model=Page[WorkerView])
async def list_workers(filter: WorkerFilter = Depends()) -> Page[WorkerView]:
    return await workers_api.list_workers(filter)


@router.post("", status_code=202, responses=problem_responses(422, 503))
async def start_worker(request: StartWorkerRequest, http_request: Request) -> Response:
    """Spawn a detached worker subprocess (distributed mode only).

    Returns ``202 Accepted`` with an empty body once the fork/exec succeeds;
    the caller polls ``GET /workers`` to observe the new worker row.
    """
    await workers_api.start_worker(request)
    return Response(status_code=202, headers={"Location": http_request.url_for("start_worker").path})


@router.post("/{worker_id}/stop", response_model=WorkerView, responses=problem_responses(404, 409))
async def stop_worker(worker_id: int) -> WorkerView:
    return await workers_api.stop_worker(worker_id)
