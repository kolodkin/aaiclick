"""Internal API for worker commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

import asyncio
import sys

from sqlmodel import col

from aaiclick.backend import is_local
from aaiclick.orchestration.execution.worker import (
    get_worker,
)
from aaiclick.orchestration.execution.worker import (
    request_worker_stop as _request_worker_stop_impl,
)
from aaiclick.orchestration.models import Worker
from aaiclick.orchestration.view_models import WorkerView, worker_to_view
from aaiclick.view_models import Page, StartWorkerRequest, WorkerFilter

from .errors import Conflict, Invalid, NotFound, WorkerSpawnFailed
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


async def start_worker(request: StartWorkerRequest | None = None) -> None:
    """Spawn a detached worker process and return once the fork/exec succeeds.

    Distributed-mode only: raises ``Invalid`` in local mode (chdb + SQLite),
    where every process shares one chdb data path and a spawned child would
    deadlock on the file lock — use the CLI's ``local start`` there instead.

    The child runs ``python -m aaiclick worker start [--max-tasks N]`` in its
    own session (``start_new_session=True``) so it outlives the caller. The
    server does not track its PID; shutdown goes through the cooperative
    ``stop_worker`` path. Exec failures (missing binary, no permission) raise
    ``WorkerSpawnFailed``.
    """
    request = request or StartWorkerRequest()

    if is_local():
        raise Invalid("start_worker requires distributed backends; use `local start` in local mode")

    cmd = [sys.executable, "-m", "aaiclick", "worker", "start"]
    if request.max_tasks is not None:
        cmd += ["--max-tasks", str(request.max_tasks)]

    try:
        await asyncio.create_subprocess_exec(*cmd, start_new_session=True)
    except (OSError, ValueError) as exc:
        raise WorkerSpawnFailed(f"failed to spawn worker process: {exc}") from exc


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
        raise Conflict(f"Worker {worker_id} already in terminal state: {worker.status}")

    refreshed = await get_worker(worker_id)
    if refreshed is None:
        raise RuntimeError(f"Worker {worker_id} disappeared after stop")
    return worker_to_view(refreshed)
