"""REST router for worker commands — paths relative to ``/api/v0``.

``start_worker`` is deliberately omitted in v0 — spawning a worker from an
HTTP request needs auth + lifecycle design that is not yet in scope.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.internal_api import workers as workers_api
from aaiclick.orchestration.view_models import WorkerView
from aaiclick.view_models import Page, WorkerFilter

from ..deps import orch_scope

router = APIRouter(prefix="/workers", tags=["workers"])


@router.get("", response_model=Page[WorkerView])
async def list_workers(
    filter: WorkerFilter = Depends(),
    _scope: None = Depends(orch_scope),
) -> Page[WorkerView]:
    return await workers_api.list_workers(filter)


@router.post("/{worker_id}/stop", response_model=WorkerView)
async def stop_worker(
    worker_id: int,
    _scope: None = Depends(orch_scope),
) -> WorkerView:
    return await workers_api.stop_worker(worker_id)
