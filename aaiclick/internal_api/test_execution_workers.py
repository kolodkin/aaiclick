"""Tests for ``aaiclick.internal_api.execution_workers``."""

from __future__ import annotations

import pytest

from aaiclick.orchestration.execution.execution_worker import (
    deregister_execution_worker,
    register_execution_worker,
)
from aaiclick.orchestration.models import EXECUTION_WORKER_ACTIVE, EXECUTION_WORKER_STOPPED, EXECUTION_WORKER_STOPPING
from aaiclick.orchestration.view_models import ExecutionWorkerView
from aaiclick.view_models import ExecutionWorkerFilter, Page

from . import errors, execution_workers


async def test_list_workers_returns_page_with_total(orch_ctx):
    await register_execution_worker(hostname="host_a", pid=1001)
    await register_execution_worker(hostname="host_b", pid=1002)

    page = await execution_workers.list_execution_workers()

    assert isinstance(page, Page)
    assert page.total is not None and page.total >= 2
    assert all(isinstance(w, ExecutionWorkerView) for w in page.items)
    hosts = [w.hostname for w in page.items]
    assert "host_a" in hosts and "host_b" in hosts


async def test_list_workers_filter_by_status(orch_ctx):
    active = await register_execution_worker(hostname="active", pid=2001)
    stopped = await register_execution_worker(hostname="stopped", pid=2002)
    await deregister_execution_worker(stopped.id)

    active_page = await execution_workers.list_execution_workers(ExecutionWorkerFilter(status=EXECUTION_WORKER_ACTIVE))
    stopped_page = await execution_workers.list_execution_workers(
        ExecutionWorkerFilter(status=EXECUTION_WORKER_STOPPED)
    )

    active_ids = [w.id for w in active_page.items]
    stopped_ids = [w.id for w in stopped_page.items]
    assert active.id in active_ids and stopped.id not in active_ids
    assert stopped.id in stopped_ids and active.id not in stopped_ids


async def test_list_workers_pagination(orch_ctx):
    for i in range(5):
        await register_execution_worker(hostname=f"page_host_{i}", pid=3000 + i)

    first = await execution_workers.list_execution_workers(ExecutionWorkerFilter(limit=2, offset=0))
    second = await execution_workers.list_execution_workers(ExecutionWorkerFilter(limit=2, offset=2))

    assert first.total is not None and first.total >= 5
    assert len(first.items) == 2 and len(second.items) == 2
    assert {w.id for w in first.items}.isdisjoint({w.id for w in second.items})


async def test_stop_worker_transitions_to_stopping(orch_ctx):
    execution_worker = await register_execution_worker()

    view = await execution_workers.stop_execution_worker(execution_worker.id)

    assert isinstance(view, ExecutionWorkerView)
    assert view.id == execution_worker.id
    assert view.status == EXECUTION_WORKER_STOPPING


async def test_stop_worker_not_found_raises(orch_ctx):
    with pytest.raises(errors.NotFound):
        await execution_workers.stop_execution_worker(999_999_999)


async def test_stop_worker_already_stopping_raises_conflict(orch_ctx):
    execution_worker = await register_execution_worker()

    await execution_workers.stop_execution_worker(execution_worker.id)

    with pytest.raises(errors.Conflict):
        await execution_workers.stop_execution_worker(execution_worker.id)


async def test_stop_worker_already_stopped_raises_conflict(orch_ctx):
    execution_worker = await register_execution_worker()
    await deregister_execution_worker(execution_worker.id)

    with pytest.raises(errors.Conflict):
        await execution_workers.stop_execution_worker(execution_worker.id)
