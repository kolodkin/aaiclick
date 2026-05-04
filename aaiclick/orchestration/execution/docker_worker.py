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
``mark_dead_workers``."""

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

from ..docker_config import BUILD_TASK_ENTRYPOINT
from ..logging import get_logs_dir
from ..models import RUNNER_DOCKER, RUNNER_SUBPROCESS, Job, RunnerMode, Task
from ..orch_context import get_sql_session
from .claiming import check_task_cancelled
from .runner import execute_task, register_returned_tasks, serialize_task_result
from .worker import HEARTBEAT_INTERVAL, POLL_INTERVAL, worker_heartbeat

CONTAINER_IPC_DIR = "/aaiclick-ipc"
CONTAINER_RESULT_FILE = "result.json"
CONTAINER_WAIT_POLL_INTERVAL = 0.5

ALWAYS_PASSED_ENV_VARS = (
    "AAICLICK_SQL_URL",
    "AAICLICK_CH_URL",
    "AAICLICK_TASK_TIMEOUT",
    "AAICLICK_DEFAULT_PRESERVATION_MODE",
)
"""Env vars always copied into the container without opt-in.

The container can't function without SQL and CH URLs; the timeout var
must propagate so child container tasks honor the same wall-clock cap;
the preservation-mode default must propagate so subjobs the user spawns
inherit the same setting."""


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


def _build_container_env() -> dict[str, str]:
    """Collect env vars to forward into the container.

    Always-passed vars + comma-separated extras from
    ``AAICLICK_DOCKER_PASSTHROUGH_ENV``."""
    env: dict[str, str] = {}
    for key in ALWAYS_PASSED_ENV_VARS:
        value = os.environ.get(key)
        if value is not None:
            env[key] = value

    extras = os.environ.get("AAICLICK_DOCKER_PASSTHROUGH_ENV", "")
    for raw in extras.split(","):
        key = raw.strip()
        if key and key in os.environ:
            env[key] = os.environ[key]
    return env


def _build_docker_run_cmd(
    image_tag: str,
    task_id: int,
    ipc_dir: str,
    log_base: str,
    env: dict[str, str],
) -> list[str]:
    """Construct the detached ``docker run`` command line.

    The IPC tmpdir is mounted at ``/aaiclick-ipc``; the host log base is
    bind-mounted at the same path inside the container so absolute log
    paths produced by ``capture_task_output`` resolve in both places.

    The framework deliberately does **not** pass ``--network`` — the
    operator is responsible for ensuring AAICLICK_SQL_URL and
    AAICLICK_CH_URL resolve from inside a container (real hostnames or
    ``host.docker.internal``)."""
    cmd: list[str] = [
        _docker_bin(),
        "run",
        "--rm",
        "--detach",
        "-v",
        f"{ipc_dir}:{CONTAINER_IPC_DIR}",
        "-v",
        f"{log_base}:{log_base}",
        "-e",
        f"AAICLICK_LOG_DIR={log_base}",
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
            str(task_id),
        ]
    )
    return cmd


async def _run_subprocess_capture(*cmd: str, check: bool = True) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_b, stderr_b = await proc.communicate()
    stdout = stdout_b.decode(errors="replace")
    stderr = stderr_b.decode(errors="replace")
    rc = proc.returncode or 0
    if check and rc != 0:
        raise RuntimeError(f"command {' '.join(cmd)!r} failed with exit code {rc}: {stderr}")
    return rc, stdout, stderr


async def _docker_pull_if_registered(image_tag: str) -> None:
    if not os.environ.get("AAICLICK_DOCKER_REGISTRY"):
        return
    await _run_subprocess_capture(_docker_bin(), "pull", image_tag, check=False)


async def _docker_run_detached(cmd: list[str]) -> str:
    """Run ``docker run --detach``; returns the container id."""
    rc, stdout, stderr = await _run_subprocess_capture(*cmd, check=False)
    if rc != 0:
        raise RuntimeError(f"docker run failed (exit {rc}): {stderr.strip() or stdout.strip()}")
    container_id = stdout.strip().splitlines()[-1]
    if not container_id:
        raise RuntimeError("docker run returned no container id")
    return container_id


async def _docker_kill(container_id: str) -> None:
    await _run_subprocess_capture(_docker_bin(), "kill", container_id, check=False)


async def _docker_inspect_exit_code(container_id: str) -> int | None:
    """Return the container's exit code, or ``None`` if it's still running."""
    rc, stdout, _ = await _run_subprocess_capture(
        _docker_bin(),
        "inspect",
        "--format",
        "{{.State.Status}}|{{.State.ExitCode}}",
        container_id,
        check=False,
    )
    if rc != 0:
        return None
    line = stdout.strip().splitlines()[-1] if stdout.strip() else ""
    if "|" not in line:
        return None
    state, exit_code = line.split("|", 1)
    if state in ("exited", "dead"):
        try:
            return int(exit_code)
        except ValueError:
            return None
    return None


async def _wait_for_container(container_id: str, timeout: float | None) -> tuple[int, str | None]:
    """Poll the container's state until it exits or the timeout fires.

    Returns ``(exit_code, error_message)``. On timeout, kills the
    container and returns ``(-1, "Task timed out after Ns")``."""
    elapsed = 0.0
    while True:
        exit_code = await _docker_inspect_exit_code(container_id)
        if exit_code is not None:
            return exit_code, None

        await asyncio.sleep(CONTAINER_WAIT_POLL_INTERVAL)
        elapsed += CONTAINER_WAIT_POLL_INTERVAL

        if timeout is not None and elapsed >= timeout:
            await _docker_kill(container_id)
            await asyncio.sleep(CONTAINER_WAIT_POLL_INTERVAL)
            return -1, f"Task timed out after {timeout}s"


async def _heartbeat_while_waiting(worker_id: int, done: asyncio.Event) -> None:
    while not done.is_set():
        try:
            await asyncio.wait_for(done.wait(), timeout=HEARTBEAT_INTERVAL)
            return
        except asyncio.TimeoutError:
            await worker_heartbeat(worker_id)


async def _watch_for_cancellation(task_id: int, container_id: str, done: asyncio.Event) -> bool:
    """Poll for task-cancellation; ``docker kill`` the container on hit.

    Returns True if cancellation fired, False if the container finished
    on its own first."""
    while not done.is_set():
        try:
            await asyncio.wait_for(done.wait(), timeout=POLL_INTERVAL)
            return False
        except asyncio.TimeoutError:
            pass
        if await check_task_cancelled(task_id):
            await _docker_kill(container_id)
            return True
    return False


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


async def _fetch_image_tag(job_id: int) -> str:
    async with get_sql_session() as session:
        result = await session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one_or_none()
        if job is None or not job.image_tag:
            raise ValueError(f"Job {job_id} has no image_tag — was it submitted in docker mode?")
        return job.image_tag


async def _resolve_runner(task: Task) -> RunnerMode:
    """Pick the runner for a task.

    The auto-injected build task always runs on the host (subprocess)
    runner — it produces the image the rest of the job's container
    tasks need. Every other task inherits the job's ``runner_mode``."""
    if task.entrypoint == BUILD_TASK_ENTRYPOINT:
        return RUNNER_SUBPROCESS

    async with get_sql_session() as session:
        result = await session.execute(select(Job).where(Job.id == task.job_id))
        job = result.scalar_one_or_none()
    return job.runner_mode if job is not None else RUNNER_SUBPROCESS


async def _run_task_in_container(task: Task, worker_id: int) -> tuple[bool, dict | None, str | None, str | None]:
    """ExecuteFn for the Docker runner.

    Pulls the image (when a registry is configured), bind-mounts an IPC
    tmpdir + the host log base, runs the container detached, and waits
    for it to exit. Heartbeats and cancellation poll concurrently.
    Cancellation and timeout both terminate the container via
    ``docker kill``."""
    image_tag = await _fetch_image_tag(task.job_id)
    await _docker_pull_if_registered(image_tag)

    raw_timeout = os.environ.get("AAICLICK_TASK_TIMEOUT")
    timeout = float(raw_timeout) if raw_timeout else None

    log_base = get_logs_dir()
    env = _build_container_env()

    with tempfile.TemporaryDirectory(prefix="aaiclick-ipc-") as ipc_dir:
        cmd = _build_docker_run_cmd(image_tag, task.id, ipc_dir, log_base, env)
        container_id = await _docker_run_detached(cmd)

        done = asyncio.Event()
        heartbeat = asyncio.create_task(_heartbeat_while_waiting(worker_id, done))
        cancel_watcher = asyncio.create_task(_watch_for_cancellation(task.id, container_id, done))

        try:
            exit_code, error = await _wait_for_container(container_id, timeout)
            was_cancelled = (
                cancel_watcher.done()
                and not cancel_watcher.cancelled()
                and (cancel_watcher.exception() is None and cancel_watcher.result())
            )
            result = _read_result_or_synthesize_failure(ipc_dir, exit_code, error, was_cancelled)
            return result.success, result.result_ref, result.log_path, result.error
        finally:
            done.set()
            await asyncio.gather(heartbeat, cancel_watcher, return_exceptions=True)


async def dispatch_execute(task: Task, worker_id: int) -> tuple[bool, dict | None, str | None, str | None]:
    """ExecuteFn that picks the runner per task.

    Plugged into ``_worker_loop`` instead of either bare ``ExecuteFn``,
    so a single worker process can serve a mixed Docker job (one
    host-side build task + N container tasks) without runner affinity
    rules."""
    if await _resolve_runner(task) == RUNNER_DOCKER:
        return await _run_task_in_container(task, worker_id)
    # Imported here to avoid a circular import at module load time —
    # mp_worker pulls in docker_worker via dispatch_execute.
    from .mp_worker import _run_task_in_child

    return await _run_task_in_child(task, worker_id)


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
