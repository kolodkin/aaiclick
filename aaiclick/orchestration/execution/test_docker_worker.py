"""Tests for the docker host-side runner — dispatch, cancellation, timeout."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from ..logging import read_task_logs
from ..models import Task
from ..runner_config import ENTRY_JVM, ENTRY_MODULE, RUNNER_DOCKER, EntryType, ImagePrebuilt
from . import docker_worker, execution_worker
from .docker_worker import _build_docker_run_cmd, build_shell_run_spec
from .execution_worker import JobDispatch, RunnerResult
from .log_test_helpers import dispatch_with_fake_cli


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
    assert "-e AAICLICK_SQL_URL" in " ".join(cmd)


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
    assert "-e AAICLICK_SQL_URL" in joined
    assert "u" not in cmd
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

    async def fake_run_detached(cmd, env):
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
    # No DB here: skip the post-exit output handling (covered by the fake-CLI tests).
    monkeypatch.setattr(execution_worker, "_collect_unfollowed_output", AsyncMock())

    dispatch = JobDispatch(RUNNER_DOCKER, None, image_source=ImagePrebuilt(image_tag="aaiclick-job:abc"))
    success, _, error = await docker_worker._run_task_in_container(
        _task(), execution_worker_id=1, dispatch=dispatch, log_run_id=None
    )
    assert success is False
    assert error == "cancelled"


_FAKE_DOCKER = """#!/bin/sh
case "$1" in
  run) echo fake-cid ;;
  wait) echo 0 ;;
  logs) echo "container says hi"; echo "container warns" 1>&2 ;;
esac
"""


def _docker_dispatch(entry_type: EntryType) -> JobDispatch:
    return JobDispatch(RUNNER_DOCKER, None, entry_type=entry_type, image_source=ImagePrebuilt(image_tag="img:1"))


async def test_jvm_container_output_reaches_task_logs(orch_ctx, monkeypatch, tmp_path):
    """The jvm shim writes no logs itself: the host registers the attempt and
    follows ``docker logs`` into task_logs, keeping each line's stream."""
    stored = await dispatch_with_fake_cli(
        monkeypatch, tmp_path, "AAICLICK_DOCKER_BIN", _FAKE_DOCKER, _docker_dispatch(ENTRY_JVM)
    )

    assert len(stored.run_ids) == 1
    lines = await read_task_logs(stored.id, stored.run_ids[0])
    assert {(line.stream, line.text) for line in lines} == {
        ("stdout", "container says hi"),
        ("stderr", "container warns"),
    }


async def test_module_container_that_registered_its_run_is_left_alone(orch_ctx, monkeypatch, tmp_path):
    """A module image registers its own run and captures its own output, so the
    host adds no run and writes nothing — that would log every line twice."""
    stored = await dispatch_with_fake_cli(
        monkeypatch,
        tmp_path,
        "AAICLICK_DOCKER_BIN",
        _FAKE_DOCKER,
        _docker_dispatch(ENTRY_MODULE),
        container_registers_run=True,
    )

    assert len(stored.run_ids) == 1
    assert await read_task_logs(stored.id, stored.run_ids[0]) == []


async def test_module_container_that_died_in_bootstrap_gets_its_output_logged(orch_ctx, monkeypatch, tmp_path):
    """A container that failed before ``execute_task`` (bad DB URL, broken
    image) registered no run and wrote no logs: the host registers the
    attempt and copies the container's output into task_logs."""
    stored = await dispatch_with_fake_cli(
        monkeypatch, tmp_path, "AAICLICK_DOCKER_BIN", _FAKE_DOCKER, _docker_dispatch(ENTRY_MODULE), result_row=None
    )

    assert len(stored.run_ids) == 1
    lines = await read_task_logs(stored.id, stored.run_ids[0])
    assert {(line.stream, line.text) for line in lines} == {
        ("stdout", "container says hi"),
        ("stderr", "container warns"),
    }


async def test_echo_prints_a_module_containers_output(orch_ctx, monkeypatch, tmp_path, capsys):
    """With echo on, a module container's output (already in task_logs) is
    printed to the worker's console, each line prefixed with its task id."""
    monkeypatch.setenv("AAICLICK_ECHO_TASK_OUTPUT", "1")
    stored = await dispatch_with_fake_cli(
        monkeypatch,
        tmp_path,
        "AAICLICK_DOCKER_BIN",
        _FAKE_DOCKER,
        _docker_dispatch(ENTRY_MODULE),
        container_registers_run=True,
    )

    captured = capsys.readouterr()
    assert f"[task {stored.id}] container says hi" in captured.out
    assert f"[task {stored.id}] container warns" in captured.err
