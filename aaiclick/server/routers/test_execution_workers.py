from __future__ import annotations

from aaiclick.orchestration.execution.execution_worker import register_execution_worker
from aaiclick.orchestration.view_models import ExecutionWorkerView
from aaiclick.view_models import Page

from ..app import API_PREFIX


async def test_list_workers(orch_ctx, app_client):
    await register_execution_worker(hostname="http_worker")

    response = await app_client.get(f"{API_PREFIX}/execution-workers")

    assert response.status_code == 200
    page = Page[ExecutionWorkerView].model_validate(response.json())
    assert page.total is not None and page.total >= 1
    assert any(w.hostname == "http_worker" for w in page.items)
