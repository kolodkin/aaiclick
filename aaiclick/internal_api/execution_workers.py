"""Internal API for execution_worker commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

import asyncio
import sys

from sqlmodel import col

from aaiclick.backend import is_local
from aaiclick.orchestration.execution.execution_worker import (
    get_execution_worker,
)
from aaiclick.orchestration.execution.execution_worker import (
    request_execution_worker_stop as _request_worker_stop_impl,
)
from aaiclick.orchestration.models import ExecutionWorker
from aaiclick.orchestration.view_models import ExecutionWorkerView, execution_worker_to_view
from aaiclick.view_models import ExecutionWorkerFilter, Page, StartExecutionWorkerRequest

from .errors import Conflict, ExecutionWorkerSpawnFailed, Invalid, NotFound
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


async def start_execution_worker(request: StartExecutionWorkerRequest | None = None) -> None:
    """Spawn a detached execution_worker process and return once the fork/exec succeeds.

    Distributed-mode only: raises ``Invalid`` in local mode (chdb + SQLite),
    where every process shares one chdb data path and a spawned child would
    deadlock on the file lock — use the CLI's ``local start`` there instead.

    The child runs ``python -m aaiclick execution-worker start [--max-tasks N]`` in its
    own session (``start_new_session=True``) so it outlives the caller. The
    server does not track its PID; shutdown goes through the cooperative
    ``stop_execution_worker`` path. Exec failures (missing binary, no permission) raise
    ``ExecutionWorkerSpawnFailed``.
    """
    request = request or StartExecutionWorkerRequest()

    if is_local():
        raise Invalid("start_execution_worker requires distributed backends; use `local start` in local mode")

    cmd = [sys.executable, "-m", "aaiclick", "execution-worker", "start"]
    if request.max_tasks is not None:
        cmd += ["--max-tasks", str(request.max_tasks)]

    try:
        await asyncio.create_subprocess_exec(*cmd, start_new_session=True)
    except (OSError, ValueError) as exc:
        raise ExecutionWorkerSpawnFailed(f"failed to spawn execution worker process: {exc}") from exc


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
