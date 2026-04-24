"""Internal API for worker commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

from sqlmodel import col

from aaiclick.orchestration.execution.worker import (
    get_worker,
)
from aaiclick.orchestration.execution.worker import (
    request_worker_stop as _request_worker_stop_impl,
)
from aaiclick.orchestration.models import Worker
from aaiclick.orchestration.view_models import WorkerView, worker_to_view
from aaiclick.view_models import Page, WorkerFilter

from .errors import Conflict, NotFound
from .pagination import paginate


async def list_workers(filter: WorkerFilter | None = None) -> Page[WorkerView]:
    """Return a page of workers ordered by ``started_at`` descending.

    ``filter.status`` restricts to a single status when set. Pagination uses
    ``filter.limit`` / ``filter.offset``; ``filter.cursor`` is reserved for a
    future cursor-based REST/MCP surface and is currently ignored.
    """
    filter = filter or WorkerFilter()

    predicates = []
    if filter.status is not None:
        predicates.append(Worker.status == filter.status)

    page = await paginate(
        Worker,
        where=predicates,
        order_by=col(Worker.started_at).desc(),
        limit=filter.limit,
        offset=filter.offset,
    )
    return Page[WorkerView](items=[worker_to_view(w) for w in page.rows], total=page.total)


async def stop_worker(worker_id: int) -> WorkerView:
    """Request a worker to stop gracefully after its current task.

    Raises ``NotFound`` if no worker matches ``worker_id``, or ``Conflict`` if
    the worker is already in ``STOPPING`` / ``STOPPED``. The worker is resolved
    first so the error distinguishes "not found" from "already terminal"; the
    atomic transition lives in
    ``aaiclick.orchestration.execution.worker.request_worker_stop`` and is
    authoritative about the final state.
    """
    worker = await get_worker(worker_id)
    if worker is None:
        raise NotFound(f"Worker not found: {worker_id}")

    if not await _request_worker_stop_impl(worker_id):
        raise Conflict(f"Worker {worker_id} already in terminal state: {worker.status.value}")

    refreshed = await get_worker(worker_id)
    if refreshed is None:
        raise RuntimeError(f"Worker {worker_id} disappeared after stop")
    return worker_to_view(refreshed)
