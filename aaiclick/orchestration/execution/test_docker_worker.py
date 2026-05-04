"""Tests for the docker host-side runner — dispatch, IPC, cancellation, timeout."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

from ..docker_config import BUILD_TASK_ENTRYPOINT
from ..models import RUNNER_DOCKER, RUNNER_SUBPROCESS, Task
from . import docker_worker


def _task(entrypoint="user.module.entry", task_id=42, job_id=1) -> Task:
    return Task(
        id=task_id,
        job_id=job_id,
        entrypoint=entrypoint,
        name="test",
    )


def test_build_container_env_includes_always_passed(monkeypatch):
    monkeypatch.setenv("AAICLICK_SQL_URL", "postgresql+asyncpg://pg/x")
    monkeypatch.setenv("AAICLICK_CH_URL", "clickhouse://ch/x")
    monkeypatch.setenv("AAICLICK_TASK_TIMEOUT", "60")
    monkeypatch.delenv("AAICLICK_DEFAULT_PRESERVATION_MODE", raising=False)
    monkeypatch.delenv("AAICLICK_DOCKER_PASSTHROUGH_ENV", raising=False)

    env = docker_worker._build_container_env()
    assert env["AAICLICK_SQL_URL"] == "postgresql+asyncpg://pg/x"
    assert env["AAICLICK_CH_URL"] == "clickhouse://ch/x"
    assert env["AAICLICK_TASK_TIMEOUT"] == "60"
    assert "AAICLICK_DEFAULT_PRESERVATION_MODE" not in env


def test_build_container_env_passthrough(monkeypatch):
    monkeypatch.setenv("AAICLICK_SQL_URL", "u")
    monkeypatch.setenv("AAICLICK_CH_URL", "u")
    monkeypatch.setenv("AAICLICK_DOCKER_PASSTHROUGH_ENV", "FOO,BAR,UNSET")
    monkeypatch.setenv("FOO", "1")
    monkeypatch.setenv("BAR", "2")
    monkeypatch.delenv("UNSET", raising=False)

    env = docker_worker._build_container_env()
    assert env["FOO"] == "1"
    assert env["BAR"] == "2"
    assert "UNSET" not in env


def test_build_docker_run_cmd_shape():
    cmd = docker_worker._build_docker_run_cmd(
        image_tag="aaiclick-job:abc",
        task_id=42,
        ipc_dir="/tmp/ipc",
        log_base="/var/log/aaiclick",
        env={"AAICLICK_SQL_URL": "u"},
    )
    joined = " ".join(cmd)
    assert "docker run --rm --detach" in joined
    assert "-v /tmp/ipc:/aaiclick-ipc" in joined
    assert "-v /var/log/aaiclick:/var/log/aaiclick" in joined
    assert "-e AAICLICK_LOG_DIR=/var/log/aaiclick" in joined
    assert "-e AAICLICK_SQL_URL=u" in joined
    assert joined.endswith(
        "aaiclick-job:abc python -m "
        "aaiclick.orchestration.execution.docker_worker --task-id 42"
    )


def test_read_result_succeeds_on_success_payload(tmp_path):
    payload = {
        "success": True,
        "result_ref": {"foo": 1},
        "log_path": "/some/log.log",
        "error": None,
    }
    (tmp_path / "result.json").write_text(json.dumps(payload))

    result = docker_worker._read_result_or_synthesize_failure(
        str(tmp_path), exit_code=0, error=None, was_cancelled=False
    )
    assert result.success is True
    assert result.result_ref == {"foo": 1}
    assert result.log_path == "/some/log.log"
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
    payload = {"success": True, "result_ref": {}, "log_path": None, "error": None}
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


async def test_resolve_runner_build_task_always_subprocess():
    """The auto-injected build task entrypoint is hardcoded to subprocess
    even when its job is in docker mode."""
    build_task = _task(entrypoint=BUILD_TASK_ENTRYPOINT, task_id=99)
    assert await docker_worker._resolve_runner(build_task) == RUNNER_SUBPROCESS


async def test_resolve_runner_reads_job_runner_mode(monkeypatch):
    """User tasks inherit the job's runner_mode."""
    user_task = _task(task_id=100, job_id=200)

    class _FakeJob:
        runner_mode = RUNNER_DOCKER

    class _FakeResult:
        def scalar_one_or_none(self):
            return _FakeJob()

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def execute(self, *a, **k):
            return _FakeResult()

    monkeypatch.setattr(docker_worker, "get_sql_session", lambda: _FakeSession())
    assert await docker_worker._resolve_runner(user_task) == RUNNER_DOCKER


async def test_dispatch_execute_routes_docker_to_container_runner(monkeypatch):
    user_task = _task()

    monkeypatch.setattr(
        docker_worker,
        "_resolve_runner",
        AsyncMock(return_value=RUNNER_DOCKER),
    )
    in_container = AsyncMock(return_value=(True, None, None, None))
    monkeypatch.setattr(docker_worker, "_run_task_in_container", in_container)

    await docker_worker.dispatch_execute(user_task, worker_id=1)
    in_container.assert_awaited_once_with(user_task, 1)


async def test_dispatch_execute_routes_subprocess_to_mp_child(monkeypatch):
    user_task = _task()

    monkeypatch.setattr(
        docker_worker,
        "_resolve_runner",
        AsyncMock(return_value=RUNNER_SUBPROCESS),
    )

    in_child = AsyncMock(return_value=(True, None, None, None))
    from . import mp_worker

    monkeypatch.setattr(mp_worker, "_run_task_in_child", in_child)

    await docker_worker.dispatch_execute(user_task, worker_id=2)
    in_child.assert_awaited_once_with(user_task, 2)


async def test_run_task_in_container_writes_result_via_ipc(monkeypatch, tmp_path):
    """End-to-end host runner: pull, run, container exits clean, host
    reads result.json and returns its contents."""
    monkeypatch.setattr(
        docker_worker, "_fetch_image_tag", AsyncMock(return_value="aaiclick-job:abc")
    )
    monkeypatch.setattr(
        docker_worker, "_docker_pull_if_registered", AsyncMock(return_value=None)
    )

    captured = {}

    async def fake_run_detached(cmd):
        # Simulate the container writing a success payload, then return a fake id.
        ipc_dir = next(
            arg.split(":", 1)[0] for arg in cmd if arg.endswith(":/aaiclick-ipc")
        )
        Path(ipc_dir, "result.json").write_text(
            json.dumps(
                {
                    "success": True,
                    "result_ref": {"x": 1},
                    "log_path": "/log/path",
                    "error": None,
                }
            )
        )
        captured["cmd"] = cmd
        return "fake-cid"

    async def fake_wait(cid, timeout):
        return 0, None

    async def no_op(*args, **kwargs):
        return None

    monkeypatch.setattr(docker_worker, "_docker_run_detached", fake_run_detached)
    monkeypatch.setattr(docker_worker, "_wait_for_container", fake_wait)
    monkeypatch.setattr(docker_worker, "worker_heartbeat", AsyncMock())
    monkeypatch.setattr(
        docker_worker, "check_task_cancelled", AsyncMock(return_value=False)
    )

    success, ref, log_path, error = await docker_worker._run_task_in_container(
        _task(), worker_id=1
    )

    assert success is True
    assert ref == {"x": 1}
    assert log_path == "/log/path"
    assert error is None
    assert "aaiclick-job:abc" in captured["cmd"]


async def test_run_task_in_container_timeout_kills_and_returns_error(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        docker_worker, "_fetch_image_tag", AsyncMock(return_value="aaiclick-job:abc")
    )
    monkeypatch.setattr(
        docker_worker, "_docker_pull_if_registered", AsyncMock(return_value=None)
    )

    async def fake_run_detached(cmd):
        return "fake-cid"

    async def fake_wait(cid, timeout):
        return -1, "Task timed out after 60.0s"

    monkeypatch.setattr(docker_worker, "_docker_run_detached", fake_run_detached)
    monkeypatch.setattr(docker_worker, "_wait_for_container", fake_wait)
    monkeypatch.setattr(docker_worker, "worker_heartbeat", AsyncMock())
    monkeypatch.setattr(
        docker_worker, "check_task_cancelled", AsyncMock(return_value=False)
    )

    success, ref, _, error = await docker_worker._run_task_in_container(
        _task(), worker_id=1
    )
    assert success is False
    assert error and "timed out" in error
