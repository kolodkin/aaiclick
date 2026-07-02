"""Docker runner — host-side ExecuteFn and container-side entrypoint.

Mirrors ``mp_worker.py``'s convention of holding both sides of the IPC
in one module:

- ``_run_task_in_container`` runs in the host worker process; it pulls
  the image (when a registry is configured), spawns ``docker run``,
  sends heartbeats and watches for cancellation while the container
  runs, then reads ``result.json`` from the bind-mounted IPC tmpdir.

- ``_container_main`` runs inside the container, invoked as
  ``python -m aaiclick.orchestration.execution.docker_worker --task-id N``.
  It boots ``orch_context()``, executes the task via the same
  ``runner.execute_task`` code path as in-process and mp_worker, and
  writes the result as JSON for the host parent to consume.

Reaper invariant: the container never writes terminal task status. The
``run_ids`` / ``run_statuses`` arrays appended by ``execute_task`` are
append-only and carry only ``TASK_RUNNING`` for the new attempt — never
COMPLETED or FAILED. Terminal writes happen exclusively in the host
worker via ``_handle_task_result``, or in the background reaper via
``mark_dead_execution_workers``."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import NamedTuple

from sqlmodel import select

from ..docker_config import add_host_flags
from ..logging import get_logs_dir
from ..models import Task
from ..orch_context import get_sql_session
from ..runner_config import ENTRY_SHELL
from . import cli
from .claiming import check_task_cancelled
from .execution_worker import (
    POLL_INTERVAL,
    JobDispatch,
    RunnerResult,
    TaskVehicle,
    drive_vehicle,
    execution_worker_heartbeat,
    parse_task_timeout,
)
from .image_builder import resolve_image_tag
from .runner import execute_task, register_returned_tasks, serialize_task_result
from .runner_env import build_runner_env

CONTAINER_IPC_DIR = "/aaiclick-ipc"
CONTAINER_RESULT_FILE = "result.json"
# How long to give `docker wait` to return after we've issued `docker kill`
# (timeout / cancellation path). The container should exit immediately once
# SIGKILL'd; this is a safety bound so a stuck daemon doesn't wedge the
# worker.
DOCKER_KILL_REAP_TIMEOUT = 10.0


class _ContainerResult(NamedTuple):
    success: bool
    result_ref: dict | None
    log_path: str | None
    error: str | None


def _docker_bin() -> str:
    return os.environ.get("AAICLICK_DOCKER_BIN", "docker")


# ---------------------------------------------------------------------------
# Host-side
# ---------------------------------------------------------------------------


def _build_docker_run_cmd(
    task: Task,
    image_tag: str,
    ipc_dir: str,
    log_base: str,
    env: dict[str, str],
) -> list[str]:
    """Construct the detached ``docker run`` command line.

    For ``shell`` tasks, the task's argv runs directly in the container with
    only ``command_env`` injected — no IPC mount, no ``AAICLICK_LOG_DIR``, no
    runner env. Success is the container's exit code; container stdout/stderr
    is captured separately into the per-task log file.

    For ``module`` tasks (unchanged): the IPC tmpdir is mounted at
    ``/aaiclick-ipc``; the host log base is bind-mounted at the same path
    inside the container so absolute log paths produced by
    ``capture_task_output`` resolve in both places.

    The framework deliberately does **not** pass ``--network`` — the
    operator is responsible for ensuring AAICLICK_SQL_URL and
    AAICLICK_CH_URL resolve from inside a container (real hostnames or
    ``host.docker.internal``).

    ``--rm`` is intentionally **not** used. The host parent calls
    ``docker rm`` explicitly after reading the result file so that
    ``docker wait`` can race-freely report the exit code (a ``--rm``
    container that exits between polls disappears from the daemon
    before we can inspect it)."""
    base = [_docker_bin(), "run", "--detach"]
    if task.entry_type == ENTRY_SHELL:
        cmd: list[str] = [
            *base,
            "-v",
            f"{log_base}:{log_base}",
            *add_host_flags("AAICLICK_DOCKER_RUN_ADD_HOST"),
        ]
        for key, value in (task.command_env or {}).items():
            cmd.extend(["-e", f"{key}={value}"])
        cmd.append(image_tag)
        cmd.extend(task.command or [])
        return cmd
    # Module entry: mount the IPC dir, inject the full runner env, and run the
    # in-container bootstrap shim (``python -m ...docker_worker --task-id N``).
    cmd = [
        *base,
        "-v",
        f"{ipc_dir}:{CONTAINER_IPC_DIR}",
        "-v",
        f"{log_base}:{log_base}",
        "-e",
        f"AAICLICK_LOG_DIR={log_base}",
        *add_host_flags("AAICLICK_DOCKER_RUN_ADD_HOST"),
    ]
    for key, value in env.items():
        cmd.extend(["-e", f"{key}={value}"])
    cmd.extend(
        [
            image_tag,
            "python",
            "-m",
            "aaiclick.orchestration.execution.docker_worker",
            "--task-id",
            str(task.id),
        ]
    )
    return cmd


async def _docker_pull_if_registered(image_tag: str) -> None:
    if not os.environ.get("AAICLICK_REGISTRY"):
        return
    await cli.run(_docker_bin(), "pull", image_tag, check=False, stream=False)


async def _docker_run_detached(cmd: list[str]) -> str:
    """Run ``docker run --detach``; returns the container id."""
    rc, stdout, stderr = await cli.run(*cmd, check=False, stream=False)
    if rc != 0:
        raise RuntimeError(f"docker run failed (exit {rc}): {stderr.strip() or stdout.strip()}")
    container_id = stdout.strip().splitlines()[-1]
    if not container_id:
        raise RuntimeError("docker run returned no container id")
    return container_id


async def _docker_kill(container_id: str) -> None:
    await cli.run(_docker_bin(), "kill", container_id, check=False, stream=False)


async def _docker_rm(container_id: str) -> None:
    """Remove the (already-stopped) container. Replaces the ``--rm`` flag
    on ``docker run``; we do it explicitly so the container survives long
    enough for ``docker wait`` to read its exit code without a race."""
    await cli.run(_docker_bin(), "rm", "--force", container_id, check=False, stream=False)


async def _wait_for_container(container_id: str, timeout: float | None) -> tuple[int, str | None]:
    """Block until the container exits, returning ``(exit_code, error)``.

    Uses ``docker wait``, which blocks on the daemon and prints the
    exit code on stdout. Race-free vs. an inspect-polling loop: the
    container can't exit "between polls" because there are no polls.

    On timeout we ``docker kill`` the container, which causes the
    in-flight ``docker wait`` to return immediately with the SIGKILL
    exit code (137); we still surface a "timed out" error to the caller."""
    wait_proc = await asyncio.create_subprocess_exec(
        _docker_bin(),
        "wait",
        container_id,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    timeout_fired = False
    try:
        stdout_b, stderr_b = await asyncio.wait_for(wait_proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        timeout_fired = True
        await _docker_kill(container_id)
        try:
            stdout_b, stderr_b = await asyncio.wait_for(wait_proc.communicate(), timeout=DOCKER_KILL_REAP_TIMEOUT)
        except asyncio.TimeoutError:
            wait_proc.kill()
            await wait_proc.wait()
            return -1, f"Task timed out after {timeout}s (docker wait did not return)"

    if wait_proc.returncode != 0:
        return -1, f"docker wait failed: {stderr_b.decode(errors='replace').strip()}"

    try:
        exit_code = int(stdout_b.decode(errors="replace").strip().splitlines()[-1])
    except (ValueError, IndexError):
        return -1, f"docker wait produced unexpected output: {stdout_b!r}"

    if timeout_fired:
        return exit_code, f"Task timed out after {timeout}s"
    return exit_code, None


async def _capture_container_logs(container_id: str, log_path: str) -> None:
    """Write the container's stdout+stderr to the per-task log file (shell tasks
    have no result.json to carry a log path)."""
    _, out, err = await cli.run(_docker_bin(), "logs", container_id, check=False, stream=False)
    Path(log_path).write_text(out + err)


def _read_result_or_synthesize_failure(
    ipc_dir: str, exit_code: int, error: str | None, was_cancelled: bool
) -> _ContainerResult:
    """Read ``result.json`` from the IPC dir, falling back to synthesized
    failure when the file is missing or malformed."""
    if was_cancelled:
        return _ContainerResult(False, None, None, "cancelled")
    if error is not None:
        return _ContainerResult(False, None, None, error)

    result_path = Path(ipc_dir) / CONTAINER_RESULT_FILE
    if not result_path.is_file():
        return _ContainerResult(
            False,
            None,
            None,
            f"container exited with code {exit_code} but produced no result file",
        )

    try:
        payload = json.loads(result_path.read_text())
    except json.JSONDecodeError as e:
        return _ContainerResult(False, None, None, f"container produced malformed result.json: {e}")

    return _ContainerResult(
        success=bool(payload.get("success")),
        result_ref=payload.get("result_ref"),
        log_path=payload.get("log_path"),
        error=payload.get("error"),
    )


class _DockerHandle(NamedTuple):
    container_id: str
    ipc_dir: str
    log_path: str | None


class _DockerVehicle(TaskVehicle["_DockerHandle", None]):
    """``TaskVehicle`` for the Docker runner.

    The IPC tmpdir is created by ``_run_task_in_container`` (its lifetime
    is the ``TemporaryDirectory`` context) and handed in; ``launch`` only
    spawns the container into it.

    For ``shell`` tasks there is no IPC result file: success is the
    container exit code and the per-task log file is filled from
    ``docker logs`` once the container exits."""

    def __init__(self, image_tag: str, log_base: str, env: dict[str, str], ipc_dir: str, entry_type: str) -> None:
        self._image_tag = image_tag
        self._log_base = log_base
        self._env = env
        self._ipc_dir = ipc_dir
        self._entry_type = entry_type

    async def launch(self, task: Task, execution_worker_id: int) -> _DockerHandle:
        log_path = None
        if task.entry_type == ENTRY_SHELL:
            # The dir already encodes job/task; the filename is just the attempt
            # (run_epoch) so a retry doesn't clobber the prior run's log.
            log_path = os.path.join(self._log_base, str(task.job_id), str(task.id), f"{task.run_epoch}.log")
            Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        cmd = _build_docker_run_cmd(task, self._image_tag, self._ipc_dir, self._log_base, self._env)
        container_id = await _docker_run_detached(cmd)
        return _DockerHandle(container_id, self._ipc_dir, log_path)

    async def wait(self, handle: _DockerHandle, timeout: float | None) -> tuple[int, str | None, None]:
        exit_code, error = await _wait_for_container(handle.container_id, timeout)
        if self._entry_type == ENTRY_SHELL and handle.log_path is not None:
            await _capture_container_logs(handle.container_id, handle.log_path)
        return exit_code, error, None

    async def poll_cancelled(self, task: Task) -> bool:
        return await check_task_cancelled(task.id)

    async def terminate(self, handle: _DockerHandle) -> None:
        await _docker_kill(handle.container_id)

    def collect(
        self, handle: _DockerHandle, exit_code: int, error: str | None, was_cancelled: bool, payload: None
    ) -> RunnerResult:
        if self._entry_type == ENTRY_SHELL:
            # ``exit_code`` is the container's main-process status from
            # ``docker wait`` (see ``wait``) — and the shell task's argv *is*
            # that main process, so this is the command's own exit code.
            if was_cancelled:
                return RunnerResult(False, None, handle.log_path, "cancelled")
            if error is not None:
                return RunnerResult(False, None, handle.log_path, error)
            return RunnerResult(
                exit_code == 0,
                None,
                handle.log_path,
                None if exit_code == 0 else f"exit {exit_code}",
            )
        # Docker reads its result from the bind-mounted IPC file, not ``payload``.
        result = _read_result_or_synthesize_failure(handle.ipc_dir, exit_code, error, was_cancelled=was_cancelled)
        return RunnerResult(result.success, result.result_ref, result.log_path, result.error)

    async def cleanup(self, handle: _DockerHandle) -> None:
        # We dropped --rm so we own cleanup; do this last so a panic
        # before docker_rm doesn't leak the container's IPC tmpdir.
        await _docker_rm(handle.container_id)


async def _run_task_in_container(
    task: Task, execution_worker_id: int, dispatch: JobDispatch
) -> tuple[bool, dict | None, str | None, str | None]:
    """ExecuteFn for the Docker runner.

    Pulls the image (when a registry is configured), bind-mounts an IPC
    tmpdir + the host log base, then hands a ``_DockerVehicle`` to the
    shared ``drive_vehicle`` driver, which heartbeats and polls for
    cancellation while the container runs. Cancellation and timeout both
    terminate the container via ``docker kill``."""
    image_tag = await resolve_image_tag(task, dispatch.image_source, dispatch.image_tag, execution_worker_id)
    await _docker_pull_if_registered(image_tag)

    timeout = parse_task_timeout()

    log_base = get_logs_dir()
    env = build_runner_env() if task.entry_type != ENTRY_SHELL else {}

    with tempfile.TemporaryDirectory(prefix="aaiclick-ipc-") as ipc_dir:
        vehicle = _DockerVehicle(image_tag, log_base, env, ipc_dir, task.entry_type)
        result = await drive_vehicle(
            task,
            execution_worker_id,
            vehicle,
            timeout=timeout,
            poll_interval=POLL_INTERVAL,
            heartbeat_fn=execution_worker_heartbeat,
        )
        return result.success, result.result_ref, result.log_path, result.error


# ---------------------------------------------------------------------------
# Container-side
# ---------------------------------------------------------------------------


async def _container_main(task_id: int) -> int:
    """Entry point invoked inside the container.

    Boots ``orch_context`` (connecting to whatever the env vars point at),
    runs the task via the shared ``execute_task`` code path, and writes a
    JSON result file the host parent reads after waiting for the
    container to exit."""
    from ..orch_context import orch_context

    payload: dict = {
        "success": False,
        "result_ref": None,
        "log_path": None,
        "error": None,
    }
    exit_code = 0
    try:
        async with orch_context():
            async with get_sql_session() as session:
                result = await session.execute(select(Task).where(Task.id == task_id))
                task = result.scalar_one()

            data_result, log_path = await execute_task(task)
            data_result = await register_returned_tasks(data_result, task.id, task.job_id)
            result_ref = serialize_task_result(data_result, task.job_id)
            payload = {
                "success": True,
                "result_ref": result_ref,
                "log_path": log_path,
                "error": None,
            }
    except BaseException as e:
        payload = {
            "success": False,
            "result_ref": None,
            "log_path": None,
            "error": f"{type(e).__name__}: {e}",
        }
        exit_code = 1

    result_path = Path(CONTAINER_IPC_DIR) / CONTAINER_RESULT_FILE
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(payload))
    return exit_code


def _container_cli() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m aaiclick.orchestration.execution.docker_worker",
        description="Container-side entrypoint for the Docker runner.",
    )
    parser.add_argument("--task-id", type=int, required=True)
    args = parser.parse_args()
    sys.exit(asyncio.run(_container_main(args.task_id)))


if __name__ == "__main__":
    _container_cli()
