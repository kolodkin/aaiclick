"""Tests for the docker host-side runner — dispatch, cancellation, timeout."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from ..models import RUNNER_DOCKER, Task
from ..runner_config import ENTRY_JVM, ENTRY_MODULE, ImagePrebuilt
from . import docker_worker
from .docker_worker import _build_docker_run_cmd, build_shell_run_spec
from .execution_worker import JobDispatch, RunnerResult


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
        {"A": "1"},
    )
    joined = " ".join(cmd)
    assert "aaiclick.orchestration.execution.remote_result" in joined
    assert "--task-id" in joined
    assert "--run-epoch" in joined


def test_jvm_cmd_relies_on_image_entrypoint():
    cmd = _build_docker_run_cmd(
        _cmdtask(entry_type=ENTRY_JVM, entrypoint="com.example.Pipeline"),
        "ghcr.io/example/pipeline:1.0",
        {"AAICLICK_SQL_URL": "u"},
    )
    # The image's own ENTRYPOINT is the aaiclick-task-api shim — the runner
    # appends only the shim arguments after the image tag.
    image_idx = cmd.index("ghcr.io/example/pipeline:1.0")
    assert cmd[image_idx + 1 :] == ["--task-id", "1", "--run-epoch", "0"]
    assert "-e AAICLICK_SQL_URL=u" in " ".join(cmd)


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
        {"AAICLICK_SQL_URL": "u"},
    )
    joined = " ".join(cmd)
    assert "docker run --detach" in joined
    # --rm is intentionally absent — the host parent calls docker rm itself
    # so docker wait can race-freely report the exit code.
    assert "--rm" not in cmd
    assert "-e AAICLICK_SQL_URL=u" in joined
    assert joined.endswith(
        "aaiclick-job:abc python -m aaiclick.orchestration.execution.remote_result --task-id 1 --run-epoch 0"
    )


async def test_run_task_in_container_cancellation_flag_overrides_result(monkeypatch):
    """When the cancel watcher fires while the container is running, the
    ``cancelled`` Event must reach the host parent and override whatever
    result row the container managed to write before being killed.

    Regression guard for the original race where the host inspected
    ``cancel_watcher`` task state directly: the watcher could still be
    sleeping in ``asyncio.wait_for`` when ``done`` got set externally,
    causing the host to miss the cancellation."""
    monkeypatch.setattr(docker_worker, "_docker_pull_if_registered", AsyncMock(return_value=None))

    cancelled_seen = []

    async def fake_run_detached(cmd):
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
    # Container wrote a stale success row before the host killed it.
    monkeypatch.setattr(docker_worker, "read_task_run_result", AsyncMock(return_value=RunnerResult(True, {}, None)))
    monkeypatch.setattr(docker_worker, "_docker_rm", AsyncMock())
    monkeypatch.setattr(docker_worker, "_docker_kill", AsyncMock())
    monkeypatch.setattr(docker_worker, "execution_worker_heartbeat", AsyncMock())
    monkeypatch.setattr(docker_worker, "check_task_cancelled", fake_check_cancelled)
    # Speed up the poll interval so the watcher actually fires within the test.
    monkeypatch.setattr(docker_worker, "POLL_INTERVAL", 0.05)

    dispatch = JobDispatch(RUNNER_DOCKER, None, image_source=ImagePrebuilt(image_tag="aaiclick-job:abc"))
    success, _, error = await docker_worker._run_task_in_container(_task(), execution_worker_id=1, dispatch=dispatch)
    assert success is False
    assert error == "cancelled"
