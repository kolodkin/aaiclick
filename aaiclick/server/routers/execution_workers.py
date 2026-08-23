from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response

from aaiclick.internal_api import execution_workers as execution_workers_api
from aaiclick.orchestration.view_models import ExecutionWorkerView
from aaiclick.view_models import ExecutionWorkerFilter, Page, StartExecutionWorkerRequest

from ..auth import require_superadmin
from ..deps import orch_scope
from ..errors import problem_responses

router = APIRouter(prefix="/execution-workers", tags=["execution-workers"], dependencies=[Depends(orch_scope)])


@router.get("", response_model=Page[ExecutionWorkerView])
async def list_execution_workers(filter: ExecutionWorkerFilter = Depends()) -> Page[ExecutionWorkerView]:
    return await execution_workers_api.list_execution_workers(filter)


@router.post(
    "",
    status_code=202,
    dependencies=[Depends(require_superadmin)],
    responses=problem_responses(403, 422, 503),
)
async def start_execution_worker(request: StartExecutionWorkerRequest, http_request: Request) -> Response:
    """Spawn a detached execution worker subprocess (distributed mode only).

    Returns ``202 Accepted`` with an empty body once the fork/exec succeeds;
    the caller polls ``GET /execution-workers`` to observe the new execution worker row.
    """
    await execution_workers_api.start_execution_worker(request)
    return Response(status_code=202, headers={"Location": http_request.url_for("start_execution_worker").path})


@router.post(
    "/{execution_worker_id}/stop",
    response_model=ExecutionWorkerView,
    dependencies=[Depends(require_superadmin)],
    responses=problem_responses(403, 404, 409),
)
async def stop_execution_worker(execution_worker_id: int) -> ExecutionWorkerView:
    return await execution_workers_api.stop_execution_worker(execution_worker_id)
