"""Tests for the docker host-side runner — dispatch, cancellation, timeout."""

from __future__ import annotations

import asyncio
import tempfile
from unittest.mock import AsyncMock

import pytest

from aaiclick.data.object.refs import native_ref

from ...testing import get_task_runs
from ..logging import read_task_logs
from ..models import TASK_COMPLETED, TASK_PENDING_FAILURE_CLEANUP, Task
from ..runner_config import ENTRY_JVM, ENTRY_MODULE, EntryType, ImagePrebuilt
from . import docker_worker, execution_worker
from .docker_worker import build_docker_create_cmd, write_command_env_file
from .execution_worker import JobDispatch, RunnerResult
from .log_test_helpers import dispatch_with_fake_cli
from .runner_env import RUNNER_DOCKER


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


def test_build_docker_create_cmd_sets_io_dir_and_env_file():
    cmd = build_docker_create_cmd(
        "aaiclick-task-7-2", "img:tag", ["echo", "hi"], {"limits": {"cpu": "500m", "memory": "256Mi"}}, "/tmp/x.env"
    )
    assert cmd[:4] == ["docker", "create", "--name", "aaiclick-task-7-2"]
    assert cmd[cmd.index("--cpus") + 1] == "0.5"
    assert cmd[cmd.index("--memory") + 1] == str(256 * 1024**2)
    assert cmd[cmd.index("--env-file") + 1] == "/tmp/x.env"
    assert cmd[cmd.index("-e") + 1] == "AAICLICK_IO_DIR=/aaiclick"
    assert cmd[-3:] == ["img:tag", "echo", "hi"]


def test_build_docker_create_cmd_without_env_file():
    cmd = build_docker_create_cmd("n", "img:tag", ["true"], None, None)
    assert "--env-file" not in cmd
    assert "--cpus" not in cmd and "--memory" not in cmd


def test_write_command_env_file_rejects_newline():
    """docker's env-file format is one ``KEY=VALUE`` per line, so a value with a
    newline cannot be carried; fail loudly rather than inject a mangled env."""
    with pytest.raises(ValueError, match="newline"):
        write_command_env_file({"K": "a\nb"})


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
        None,
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

    async def fake_check_aborted(task_id, run_epoch):
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
    monkeypatch.setattr(execution_worker, "check_run_aborted", fake_check_aborted)
    # Speed up the poll interval so the watcher actually fires within the test.
    monkeypatch.setattr(docker_worker, "POLL_INTERVAL", 0.05)
    # No DB here: skip the run count and post-exit output handling (covered by the fake-CLI tests).
    monkeypatch.setattr(execution_worker, "get_run_count", AsyncMock(return_value=0))
    monkeypatch.setattr(execution_worker, "_collect_unfollowed_output", AsyncMock())

    dispatch = JobDispatch(RUNNER_DOCKER, None, image_source=ImagePrebuilt(image_tag="aaiclick-job:abc"))
    success, _, error = await docker_worker._run_task_in_container(
        _task(), execution_worker_id=1, dispatch=dispatch, log_run_id=None
    )
    assert success is False
    assert error == "cancelled"


_FAKE_DOCKER = """#!/bin/sh
case "$1" in
  run) echo "$@" > "$(dirname "$0")/run-argv"; echo fake-cid ;;
  wait) echo 0 ;;
  logs) echo "container says hi"; echo "container warns" 1>&2 ;;
esac
"""


def _docker_dispatch(entry_type: EntryType, resources: dict | None = None) -> JobDispatch:
    return JobDispatch(
        RUNNER_DOCKER, None, entry_type=entry_type, image_source=ImagePrebuilt(image_tag="img:1"), resources=resources
    )


async def test_module_container_honours_job_resources(orch_ctx, monkeypatch, tmp_path):
    """The job's ``resources`` limits reach the real ``docker run`` argv."""
    await dispatch_with_fake_cli(
        monkeypatch,
        tmp_path,
        "AAICLICK_DOCKER_BIN",
        _FAKE_DOCKER,
        _docker_dispatch(ENTRY_MODULE, {"limits": {"cpu": "500m", "memory": "64Mi"}}),
    )
    argv = (tmp_path / "run-argv").read_text().split()
    assert argv[argv.index("--cpus") + 1] == "0.5"
    assert argv[argv.index("--memory") + 1] == str(64 * 1024**2)


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

    (run,) = await get_task_runs(stored.id)
    assert await read_task_logs(stored.id, run.run_id) == []


async def test_module_container_that_died_in_bootstrap_gets_its_output_logged(orch_ctx, monkeypatch, tmp_path):
    """A container that failed before ``execute_task`` (bad DB URL, broken
    image) registered no run and wrote no logs: the host registers the
    attempt and copies the container's output into task_logs."""
    stored = await dispatch_with_fake_cli(
        monkeypatch, tmp_path, "AAICLICK_DOCKER_BIN", _FAKE_DOCKER, _docker_dispatch(ENTRY_MODULE), result_row=None
    )

    (run,) = await get_task_runs(stored.id)
    lines = await read_task_logs(stored.id, run.run_id)
    assert {(line.stream, line.text) for line in lines} == {
        ("stdout", "container says hi"),
        ("stderr", "container warns"),
    }


async def test_module_container_output_is_printed_by_default(orch_ctx, monkeypatch, tmp_path, capsys):
    """Task logs go to both destinations by default: a module container's
    output (already in task_logs) is printed to the worker's console, each
    line prefixed with its task id."""
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


async def test_bootstrap_failure_is_only_printed_when_task_logs_go_to_console(orch_ctx, monkeypatch, tmp_path, capsys):
    """With ``AAICLICK_TASK_LOGS=console`` the host does not rescue a bootstrap
    failure into task_logs — it prints the container's output instead."""
    monkeypatch.setenv("AAICLICK_TASK_LOGS", "console")
    stored = await dispatch_with_fake_cli(
        monkeypatch, tmp_path, "AAICLICK_DOCKER_BIN", _FAKE_DOCKER, _docker_dispatch(ENTRY_MODULE), result_row=None
    )

    assert await get_task_runs(stored.id) == []
    assert f"[task {stored.id}] container says hi" in capsys.readouterr().out


# The shell vehicle: create → cp in.json → start → wait → cp out.json → rm.
_FAKE_DOCKER_SHELL = """#!/bin/sh
d="$(dirname "$0")"
case "$1" in
  create) echo "$@" > "$d/create-argv"; echo fake-cid ;;
  cp) case "$2" in
        *:/aaiclick/.)
          case "$FAKE_OUT_MODE" in
            missing) : ;;
            broken) echo "error during connect: EOF" >&2; exit 1 ;;
            *) printf '%s' "$FAKE_OUT_JSON" > "$3/out.json" ;;
          esac ;;
        *) cp -r "$2" "$d/in-copy"; stat -c %a "$2" > "$d/in-mode" ;;
      esac ;;
  start) : ;;
  wait) echo 0 ;;
  logs) echo "container says hi" ;;
  rm|kill) : ;;
esac
"""


async def test_shell_container_round_trips_in_and_out_json(orch_ctx, monkeypatch, tmp_path):
    """The host writes the resolved kwargs into the container, reads back what
    the command wrote to ``out.json`` and stores it as the task result; the
    env file and the host-side IO dir are gone afterwards."""
    monkeypatch.setenv("FAKE_OUT_JSON", '{"_aaiclick_type": "native", "value": {"k": "v"}}')
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    spec = JobDispatch(
        RUNNER_DOCKER,
        None,
        entry_type="shell",
        command=["sh", "-c", "x"],
        command_env={"K": "v"},
        image_source=ImagePrebuilt(image_tag="img:1"),
    )
    stored = await dispatch_with_fake_cli(monkeypatch, tmp_path, "AAICLICK_DOCKER_BIN", _FAKE_DOCKER_SHELL, spec)

    assert stored.result == native_ref({"k": "v"}), stored.error
    argv = (tmp_path / "create-argv").read_text().split()
    assert argv[argv.index("-e") + 1] == "AAICLICK_IO_DIR=/aaiclick"
    assert argv[-3:] == ["sh", "-c", "x"]
    assert (tmp_path / "in-copy" / "in.json").read_text() == "{}"
    # docker cp keeps the mode and makes root the owner: a non-root image
    # (the scaffolded one runs as USER aaiclick) must still write out.json.
    assert (tmp_path / "in-mode").read_text().strip() == "777"
    assert not list(tmp_path.glob("aaiclick-io-*")) and not list(tmp_path.glob("aaiclick-task-*.env"))


@pytest.mark.parametrize(
    "mode, status, error",
    [
        # An absent out.json is "no result"; a failed copy-back fails the task.
        pytest.param("missing", TASK_COMPLETED, None, id="missing-out-json"),
        pytest.param("broken", TASK_PENDING_FAILURE_CLEANUP, "error during connect", id="daemon-failure"),
    ],
)
async def test_shell_container_out_json_read_failures(orch_ctx, monkeypatch, tmp_path, mode, status, error):
    monkeypatch.setenv("FAKE_OUT_MODE", mode)
    spec = JobDispatch(
        RUNNER_DOCKER, None, entry_type="shell", command=["true"], image_source=ImagePrebuilt(image_tag="img:1")
    )
    stored = await dispatch_with_fake_cli(monkeypatch, tmp_path, "AAICLICK_DOCKER_BIN", _FAKE_DOCKER_SHELL, spec)
    assert (stored.status, stored.result) == (status, None)
    if error:
        assert error in (stored.error or "")


async def test_jvm_container_output_reaches_task_logs(orch_ctx, monkeypatch, tmp_path):
    """The jvm shim writes no logs itself: the host registers the attempt and
    follows ``docker logs`` into task_logs. The class name is the shim's one
    argument on the ``docker create`` argv."""
    monkeypatch.setenv("FAKE_OUT_JSON", "null")
    stored = await dispatch_with_fake_cli(
        monkeypatch,
        tmp_path,
        "AAICLICK_DOCKER_BIN",
        _FAKE_DOCKER_SHELL,
        _docker_dispatch(ENTRY_JVM)._replace(command=["com.example.Pipeline"]),
    )

    (run,) = await get_task_runs(stored.id)
    lines = await read_task_logs(stored.id, run.run_id)
    assert [(line.stream, line.text) for line in lines] == [("stdout", "container says hi")]
    argv = (tmp_path / "create-argv").read_text().split()
    assert argv[-1] == "com.example.Pipeline"
