"""Tests for the docker host-side runner — dispatch, IPC, cancellation, timeout."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

from ..models import RUNNER_DOCKER, Task
from ..runner_config import ENTRY_MODULE, ImagePrebuilt
from . import docker_worker
from .docker_worker import _build_docker_run_cmd, build_shell_run_spec
from .execution_worker import JobDispatch


def _cmdtask(**kw):
    return Task(
        id=1,
        job_id=1,
        name="t",
        entrypoint=kw.get("entrypoint", ""),
        entry_type=kw["entry_type"],
        command=kw.get("command"),
        command_env=kw.get("command_env"),
    )


def test_module_cmd_uses_bootstrap_shim():
    cmd = _build_docker_run_cmd(
        _cmdtask(entry_type=ENTRY_MODULE, entrypoint="m.f"),
        "python:3.12",
        "/ipc",
        {"A": "1"},
    )
    joined = " ".join(cmd)
    assert "aaiclick.orchestration.execution.docker_worker" in joined
    assert "--task-id" in joined
    assert "/aaiclick-ipc" in joined  # IPC mount present for module


def test_build_shell_run_spec_wraps_argv():
    task = Task(
        id=7,
        job_id=1,
        name="t",
        entrypoint="",
        entry_type="shell",
        command=["echo", "hi"],
        command_env={"K": "v"},
        run_epoch=2,
    )
    spec = build_shell_run_spec(task, "img:tag")
    assert spec.argv[:2] == ["docker", "run"]
    assert "--rm" in spec.argv
    assert "--name" in spec.argv and "aaiclick-task-7-2" in spec.argv
    assert ["-e", "K=v"] == spec.argv[spec.argv.index("-e") : spec.argv.index("-e") + 2]
    assert spec.argv[-3:] == ["img:tag", "echo", "hi"]
    assert spec.env is None
    assert spec.cleanup_argv == ["docker", "kill", "aaiclick-task-7-2"]


def _task(entrypoint="user.module.entry", task_id=42, job_id=1) -> Task:
    return Task(
        id=task_id,
        job_id=job_id,
        entrypoint=entrypoint,
        name="test",
    )


def test_build_docker_run_cmd_shape():
    cmd = docker_worker._build_docker_run_cmd(
        _cmdtask(entry_type=ENTRY_MODULE, entrypoint="user.module.entry"),
        "aaiclick-job:abc",
        "/tmp/ipc",
        {"AAICLICK_SQL_URL": "u"},
    )
    joined = " ".join(cmd)
    assert "docker run --detach" in joined
    # --rm is intentionally absent — the host parent calls docker rm itself
    # so docker wait can race-freely report the exit code.
    assert "--rm" not in cmd
    assert "-v /tmp/ipc:/aaiclick-ipc" in joined
    assert "-e AAICLICK_SQL_URL=u" in joined
    assert joined.endswith("aaiclick-job:abc python -m aaiclick.orchestration.execution.docker_worker --task-id 1")


def test_read_result_succeeds_on_success_payload(tmp_path):
    payload = {
        "success": True,
        "result_ref": {"foo": 1},
        "error": None,
    }
    (tmp_path / "result.json").write_text(json.dumps(payload))

    result = docker_worker._read_result_or_synthesize_failure(
        str(tmp_path), exit_code=0, error=None, was_cancelled=False
    )
    assert result.success is True
    assert result.result_ref == {"foo": 1}
    assert result.error is None


def test_read_result_synthesizes_failure_when_file_missing(tmp_path):
    result = docker_worker._read_result_or_synthesize_failure(
        str(tmp_path), exit_code=137, error=None, was_cancelled=False
    )
    assert result.success is False
    assert result.error and "exited with code 137" in result.error


def test_read_result_synthesizes_failure_on_malformed_json(tmp_path):
    (tmp_path / "result.json").write_text("not-valid-json")
    result = docker_worker._read_result_or_synthesize_failure(
        str(tmp_path), exit_code=0, error=None, was_cancelled=False
    )
    assert result.success is False
    assert result.error and "malformed" in result.error


def test_read_result_propagates_cancellation(tmp_path):
    """Even if the container managed to write a success payload before
    being killed, a cancellation flag must override it — the host's
    explicit kill is the source of truth."""
    payload = {"success": True, "result_ref": {}, "error": None}
    (tmp_path / "result.json").write_text(json.dumps(payload))
    result = docker_worker._read_result_or_synthesize_failure(
        str(tmp_path), exit_code=137, error=None, was_cancelled=True
    )
    assert result.success is False
    assert result.error == "cancelled"


def test_read_result_propagates_timeout_error(tmp_path):
    """Timeout error from _wait_for_container takes precedence over
    file contents — same reasoning as cancellation."""
    result = docker_worker._read_result_or_synthesize_failure(
        str(tmp_path),
        exit_code=-1,
        error="Task timed out after 60.0s",
        was_cancelled=False,
    )
    assert result.success is False
    assert "timed out" in (result.error or "")


async def test_run_task_in_container_cancellation_flag_overrides_result(monkeypatch, tmp_path):
    """When the cancel watcher fires while the container is running, the
    ``cancelled`` Event must reach the host parent and override whatever
    the container managed to write to result.json before being killed.

    Regression guard for the original race where the host inspected
    ``cancel_watcher`` task state directly: the watcher could still be
    sleeping in ``asyncio.wait_for`` when ``done`` got set externally,
    causing the host to miss the cancellation."""
    monkeypatch.setattr(docker_worker, "_docker_pull_if_registered", AsyncMock(return_value=None))

    cancelled_seen = []

    async def fake_run_detached(cmd):
        # Container writes a stale success payload before the host kills it.
        ipc_dir = next(arg.split(":", 1)[0] for arg in cmd if arg.endswith(":/aaiclick-ipc"))
        Path(ipc_dir, "result.json").write_text(json.dumps({"success": True, "result_ref": {}, "error": None}))
        return "fake-cid"

    async def fake_wait(cid, timeout):
        # Sleep long enough for the watcher to fire, then pretend the
        # container exited 137 (SIGKILL'd by the watcher).
        await asyncio.sleep(0.2)
        return 137, None

    async def fake_check_cancelled(task_id):
        # First poll: not cancelled. Subsequent polls: cancelled.
        cancelled_seen.append(task_id)
        return len(cancelled_seen) > 1

    monkeypatch.setattr(docker_worker, "_docker_run_detached", fake_run_detached)
    monkeypatch.setattr(docker_worker, "_wait_for_container", fake_wait)
    monkeypatch.setattr(docker_worker, "_docker_rm", AsyncMock())
    monkeypatch.setattr(docker_worker, "_docker_kill", AsyncMock())
    monkeypatch.setattr(docker_worker, "execution_worker_heartbeat", AsyncMock())
    monkeypatch.setattr(docker_worker, "check_task_cancelled", fake_check_cancelled)
    # Speed up the poll interval so the watcher actually fires within the test.
    monkeypatch.setattr(docker_worker, "POLL_INTERVAL", 0.05)

    dispatch = JobDispatch(RUNNER_DOCKER, "aaiclick-job:abc", None, image_source=ImagePrebuilt(image_tag="aaiclick-job:abc"))
    success, _, error = await docker_worker._run_task_in_container(_task(), execution_worker_id=1, dispatch=dispatch)
    assert success is False
    assert error == "cancelled"
