"""Tests for ``aaiclick.internal_api.workers``."""

from __future__ import annotations

import pytest

from aaiclick.orchestration.execution.worker import (
    deregister_worker,
    register_worker,
)
from aaiclick.orchestration.models import WORKER_ACTIVE, WORKER_STOPPED, WORKER_STOPPING
from aaiclick.orchestration.view_models import WorkerView
from aaiclick.view_models import Page, WorkerFilter

from . import errors, workers


async def test_list_workers_returns_page_with_total(orch_ctx):
    await register_worker(hostname="host_a", pid=1001)
    await register_worker(hostname="host_b", pid=1002)

    page = await workers.list_workers()

    assert isinstance(page, Page)
    assert page.total is not None and page.total >= 2
    assert all(isinstance(w, WorkerView) for w in page.items)
    hosts = [w.hostname for w in page.items]
    assert "host_a" in hosts and "host_b" in hosts


async def test_list_workers_filter_by_status(orch_ctx):
    active = await register_worker(hostname="active", pid=2001)
    stopped = await register_worker(hostname="stopped", pid=2002)
    await deregister_worker(stopped.id)

    active_page = await workers.list_workers(WorkerFilter(status=WORKER_ACTIVE))
    stopped_page = await workers.list_workers(WorkerFilter(status=WORKER_STOPPED))

    active_ids = [w.id for w in active_page.items]
    stopped_ids = [w.id for w in stopped_page.items]
    assert active.id in active_ids and stopped.id not in active_ids
    assert stopped.id in stopped_ids and active.id not in stopped_ids


async def test_list_workers_pagination(orch_ctx):
    for i in range(5):
        await register_worker(hostname=f"page_host_{i}", pid=3000 + i)

    first = await workers.list_workers(WorkerFilter(limit=2, offset=0))
    second = await workers.list_workers(WorkerFilter(limit=2, offset=2))

    assert first.total is not None and first.total >= 5
    assert len(first.items) == 2 and len(second.items) == 2
    assert {w.id for w in first.items}.isdisjoint({w.id for w in second.items})


async def test_stop_worker_transitions_to_stopping(orch_ctx):
    worker = await register_worker()

    view = await workers.stop_worker(worker.id)

    assert isinstance(view, WorkerView)
    assert view.id == worker.id
    assert view.status == WORKER_STOPPING


async def test_stop_worker_not_found_raises(orch_ctx):
    with pytest.raises(errors.NotFound):
        await workers.stop_worker(999_999_999)


async def test_stop_worker_already_stopping_raises_conflict(orch_ctx):
    worker = await register_worker()

    await workers.stop_worker(worker.id)

    with pytest.raises(errors.Conflict):
        await workers.stop_worker(worker.id)


async def test_stop_worker_already_stopped_raises_conflict(orch_ctx):
    worker = await register_worker()
    await deregister_worker(worker.id)

    with pytest.raises(errors.Conflict):
        await workers.stop_worker(worker.id)
