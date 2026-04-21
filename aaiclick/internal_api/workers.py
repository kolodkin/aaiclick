"""Internal API for worker commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

from sqlmodel import col, func, select

from aaiclick.orchestration.execution.worker import get_worker
from aaiclick.orchestration.execution.worker import request_worker_stop as _request_worker_stop_impl
from aaiclick.orchestration.models import Worker
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.view_models import WorkerView, worker_to_view
from aaiclick.view_models import Page, WorkerFilter

from .errors import Conflict, NotFound


async def list_workers(filter: WorkerFilter | None = None) -> Page[WorkerView]:
    """Return a page of workers ordered by ``started_at`` descending.

    ``filter.status`` restricts to a single status when set. Pagination uses
    ``filter.limit`` / ``filter.offset``; ``filter.cursor`` is reserved for a
    future cursor-based REST/MCP surface and is currently ignored.
    """
    filter = filter or WorkerFilter()

    count_query = select(func.count()).select_from(Worker)
    list_query = select(Worker)
    if filter.status is not None:
        count_query = count_query.where(Worker.status == filter.status)
        list_query = list_query.where(Worker.status == filter.status)

    list_query = list_query.order_by(col(Worker.started_at).desc()).limit(filter.limit).offset(filter.offset)
    async with get_sql_session() as session:
        total = (await session.execute(count_query)).scalar_one()
        rows = (await session.execute(list_query)).scalars().all()

    return Page[WorkerView](items=[worker_to_view(w) for w in rows], total=total)


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
