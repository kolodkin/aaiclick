"""Test helpers for task-log capture tests.

``read_logs_via_child``: for mp-module tests (``orch_ctx_no_ch``) the parent
holds no chdb session, so reading back what a worker child wrote requires
another child that opens its own ``orch_context``.

``dispatch_with_fake_cli``: run a persisted task through ``dispatch_execute``
against a fake ``docker`` / ``kubectl`` script.
"""

from __future__ import annotations

import asyncio
import multiprocessing
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from ..factories import create_job
from ..jobs import get_task
from ..jobs.queries import get_tasks_for_job
from ..logging import read_task_logs
from ..models import Task
from . import dispatch, docker_worker, kubernetes_worker
from .execution_worker import JobDispatch, RunnerResult

_mp_ctx = multiprocessing.get_context("spawn")


def _read_logs_child_target(task_id: int, run_id: int, queue: multiprocessing.Queue) -> None:
    from ..orch_context import orch_context  # Circular dep: orch_context imports the execution package at top level.

    async def _run() -> None:
        async with orch_context():
            lines = await read_task_logs(task_id, run_id)
            queue.put([line.text for line in lines])

    asyncio.run(_run())


def read_logs_via_child(task_id: int, run_id: int) -> list[str]:
    queue = _mp_ctx.Queue()
    proc = _mp_ctx.Process(target=_read_logs_child_target, args=(task_id, run_id, queue), daemon=True)
    proc.start()
    texts = queue.get(timeout=60)
    proc.join()
    return texts


async def dispatch_with_fake_cli(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, cli_env: str, script: str, spec: JobDispatch
) -> Task:
    """Install ``script`` as the CLI named by ``cli_env``, dispatch a fresh
    task with ``spec`` and return the task as stored afterwards."""
    cli_bin = tmp_path / "cli"
    cli_bin.write_text(script)
    cli_bin.chmod(0o755)
    monkeypatch.setenv(cli_env, str(cli_bin))
    for runner in (docker_worker, kubernetes_worker):
        monkeypatch.setattr(runner, "read_task_run_result", AsyncMock(return_value=RunnerResult(True, None, None)))
        monkeypatch.setattr(runner, "execution_worker_heartbeat", AsyncMock())
    monkeypatch.setattr(dispatch, "_resolve_dispatch", AsyncMock(return_value=spec))

    job = await create_job("fake_cli_job", "aaiclick.orchestration.fixtures.sample_tasks.simple_task")
    task = (await get_tasks_for_job(job.id))[0]
    task.entry_type = spec.entry_type
    success, _, error = await dispatch.dispatch_execute(task, execution_worker_id=1)
    assert success, error
    stored = await get_task(task.id)
    assert stored is not None
    return stored
