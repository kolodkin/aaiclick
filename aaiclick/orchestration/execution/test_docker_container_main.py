"""Tests for the container-side _container_main entrypoint.

Container-side tests live in their own module because they exercise the
same code path the container would run — including ``orch_context()``
booting chdb. ``orch_ctx_no_ch`` is the wrong fixture here; we want the
default ``orch_ctx`` so chdb is available, exactly like a real container.
Mixing these tests into ``test_docker_worker.py`` (which uses
``orch_ctx_no_ch``) would force two chdb-mode contexts in one module."""

from __future__ import annotations

import json
from unittest.mock import patch

from sqlmodel import select

from ..factories import create_job
from ..models import TASK_RUNNING, Task
from ..orch_context import get_sql_session
from . import docker_worker


async def test_container_main_writes_success_payload(orch_ctx, tmp_path):
    """Round-trip: container-side entrypoint runs a real task and writes
    a success-shaped result.json the host parent can consume."""
    job = await create_job(
        "test_container_main_success",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )

    async with get_sql_session() as session:
        task = (
            await session.execute(select(Task).where(Task.job_id == job.id))
        ).scalar_one()

    with patch.object(docker_worker, "CONTAINER_IPC_DIR", str(tmp_path)):
        exit_code = await docker_worker._container_main(task.id)

    assert exit_code == 0
    payload = json.loads((tmp_path / "result.json").read_text())
    assert payload["success"] is True
    assert payload["error"] is None
    assert payload["log_path"]  # path to the captured log file
    # `simple_task` returns None; result_ref should reflect that
    assert payload["result_ref"] is None

    # Reaper invariant: only run_ids/run_statuses appended (TASK_RUNNING).
    # Terminal status is set by the host worker, not the container.
    async with get_sql_session() as session:
        task = (
            await session.execute(select(Task).where(Task.id == task.id))
        ).scalar_one()
    assert len(task.run_ids) == 1
    assert task.run_statuses == [TASK_RUNNING]


async def test_container_main_writes_failure_payload_on_exception(
    orch_ctx, tmp_path
):
    """Failed user code must produce a failure-shaped result.json and
    exit non-zero — the host parent depends on the exit code as a fast
    failure signal in addition to the JSON contents."""
    job = await create_job(
        "test_container_main_failure",
        "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
    )

    async with get_sql_session() as session:
        task = (
            await session.execute(select(Task).where(Task.job_id == job.id))
        ).scalar_one()

    with patch.object(docker_worker, "CONTAINER_IPC_DIR", str(tmp_path)):
        exit_code = await docker_worker._container_main(task.id)

    assert exit_code == 1
    payload = json.loads((tmp_path / "result.json").read_text())
    assert payload["success"] is False
    assert payload["result_ref"] is None
    assert payload["log_path"] is None
    assert "intentionally" in (payload["error"] or "")


async def test_container_main_does_not_write_terminal_task_status(
    orch_ctx, tmp_path
):
    """Reaper invariant: the container never flips the Task to a
    terminal status. Only run_ids / run_statuses are appended (always
    TASK_RUNNING for the new attempt)."""
    job = await create_job(
        "test_container_main_invariant",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )

    async with get_sql_session() as session:
        task = (
            await session.execute(select(Task).where(Task.job_id == job.id))
        ).scalar_one()
        original_status = task.status

    with patch.object(docker_worker, "CONTAINER_IPC_DIR", str(tmp_path)):
        await docker_worker._container_main(task.id)

    async with get_sql_session() as session:
        task_after = (
            await session.execute(select(Task).where(Task.id == task.id))
        ).scalar_one()

    assert task_after.status == original_status
    assert task_after.error is None
    # The host's _handle_task_result is what would set TASK_COMPLETED.
