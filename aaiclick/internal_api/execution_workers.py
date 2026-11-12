"""Internal API for execution_worker commands.

See docs/designs/api_server.md — "Worker lifecycle — CLI only".

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

from sqlmodel import col

from aaiclick.orchestration.execution.execution_worker import (
    get_execution_worker,
)
from aaiclick.orchestration.execution.execution_worker import (
    request_execution_worker_stop as _request_worker_stop_impl,
)
from aaiclick.orchestration.models import ExecutionWorker
from aaiclick.orchestration.view_models import ExecutionWorkerView, execution_worker_to_view
from aaiclick.view_models import ExecutionWorkerFilter, Page

from .errors import Conflict, NotFound
from .pagination import paginate


async def list_execution_workers(filter: ExecutionWorkerFilter | None = None) -> Page[ExecutionWorkerView]:
    """Return a page of execution workers ordered by ``started_at`` descending.

    ``filter.status`` restricts to a single status when set. Pagination uses
    ``filter.limit`` / ``filter.offset``; ``filter.cursor`` is reserved for a
    future cursor-based REST/MCP surface and is currently ignored.
    """
    filter = filter or ExecutionWorkerFilter()

    predicates = []
    if filter.status is not None:
        predicates.append(ExecutionWorker.status == filter.status)

    page = await paginate(
        ExecutionWorker,
        where=predicates,
        order_by=col(ExecutionWorker.started_at).desc(),
        limit=filter.limit,
        offset=filter.offset,
    )
    return Page[ExecutionWorkerView](items=[execution_worker_to_view(w) for w in page.rows], total=page.total)


async def stop_execution_worker(execution_worker_id: int) -> ExecutionWorkerView:
    """Request a execution_worker to stop gracefully after its current task.

    Raises ``NotFound`` if no execution_worker matches ``execution_worker_id``, or ``Conflict`` if
    the execution_worker is already in ``STOPPING`` / ``STOPPED``. The execution_worker is resolved
    first so the error distinguishes "not found" from "already terminal"; the
    atomic transition lives in
    ``aaiclick.orchestration.execution.execution_worker.request_execution_worker_stop`` and is
    authoritative about the final state.
    """
    execution_worker = await get_execution_worker(execution_worker_id)
    if execution_worker is None:
        raise NotFound(f"ExecutionWorker not found: {execution_worker_id}")

    if not await _request_worker_stop_impl(execution_worker_id):
        raise Conflict(f"ExecutionWorker {execution_worker_id} already in terminal state: {execution_worker.status}")

    refreshed = await get_execution_worker(execution_worker_id)
    if refreshed is None:
        raise RuntimeError(f"ExecutionWorker {execution_worker_id} disappeared after stop")
    return execution_worker_to_view(refreshed)
