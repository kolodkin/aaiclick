"""Docker runner — host-side ExecuteFns driving containers.

See docs/designs/orchestration.md — "Execution layers".

``_run_task_in_container`` runs in the host worker process; it pulls the
image (when a registry is configured), spawns ``docker run``, sends
heartbeats and watches for cancellation while the container runs, then
reads the ``RemoteTaskResult`` row the container wrote. The container runs
the shared entrypoint (``remote_result``), the same one the kubernetes
runner's Pods use.

``_run_shell_in_container`` is the shell/jvm counterpart on the file data
plane (``io_plane``): ``docker create`` → ``docker cp`` the host-resolved
``in.json`` in → ``docker start`` → wait → ``docker cp`` ``out.json`` out.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any, NamedTuple

from ..docker_config import add_host_flags, get_registry, resource_flags
from ..models import Task
from . import cli
from .claiming import check_run_aborted
from .docker_build import resolve_launch_image
from .execution_worker import (
    POLL_INTERVAL,
    JobDispatch,
    RunnerResult,
    ShellVehicle,
    TaskVehicle,
    drive_shell_vehicle,
    drive_vehicle,
    execution_worker_heartbeat,
    parse_task_timeout,
)
from .io_plane import CONTAINER_IO_DIR, IO_DIR_ENV, OUT_JSON, read_out_json
from .remote_result import REMOTE_ENTRYPOINT, collect_remote_result, read_task_run_result
from .runner import stage_in_json
from .runner_env import build_runner_env

# How long to give `docker wait` to return after we've issued `docker kill`
# (timeout / cancellation path). The container should exit immediately once
# SIGKILL'd; this is a safety bound so a stuck daemon doesn't wedge the
# worker.
DOCKER_KILL_REAP_TIMEOUT = 10.0


def _docker_bin() -> str:
    return os.environ.get("AAICLICK_DOCKER_BIN", "docker")


# ---------------------------------------------------------------------------
# Host-side
# ---------------------------------------------------------------------------


def _env_flags(keys: Iterable[str]) -> list[str]:
    """``-e KEY`` flags, name only: docker reads each value from the CLI's own
    env, keeping values (DB passwords) out of ``ps``."""
    return [flag for key in keys for flag in ("-e", key)]


def _shell_container_name(task: Task) -> str:
    """Unique-per-attempt container name so cleanup can address it."""
    return f"aaiclick-task-{task.id}-{task.run_epoch}"


def write_command_env_file(command_env: dict[str, str]) -> str:
    """Write ``command_env`` as a docker ``--env-file`` (one ``KEY=VALUE`` per
    line, 0600) and return its path.

    The format has no quoting or escaping, so a key or value containing a
    newline cannot be carried and is rejected rather than injected mangled."""
    for key, value in command_env.items():
        if "\n" in key or "\n" in value:
            raise ValueError(f"command_env {key!r} contains a newline, which docker's env-file format cannot carry")
    with tempfile.NamedTemporaryFile("w", prefix="aaiclick-task-", suffix=".env", delete=False) as f:
        f.writelines(f"{key}={value}\n" for key, value in command_env.items())
        return f.name


def build_docker_create_cmd(
    name: str, image_tag: str, command: list[str], resources: dict | None, env_file: str | None
) -> list[str]:
    """``docker create`` for a shell/jvm task: the job's limits
    (``resource_flags``), ``command_env`` from a private ``--env-file``
    (on the argv the values would show in ``ps``), and ``AAICLICK_IO_DIR``
    pointing at the in-container IO dir. No runner env reaches the image."""
    argv = [
        _docker_bin(),
        "create",
        "--name",
        name,
        *add_host_flags("AAICLICK_DOCKER_RUN_ADD_HOST"),
        *resource_flags(resources),
    ]
    if env_file is not None:
        argv.extend(["--env-file", env_file])
    argv.extend(["-e", f"{IO_DIR_ENV}={CONTAINER_IO_DIR}", image_tag, *command])
    return argv


def _build_docker_run_cmd(
    task: Task,
    image_tag: str,
    env: dict[str, str],
    resources: dict | None,
) -> list[str]:
    """Construct the detached ``docker run`` command line for a module task:
    inject the full runner env (names only — ``_docker_run_detached`` passes
    the values), apply the job's limits (``resource_flags``), and run the
    in-container bootstrap shim (``python -m ...remote_result --task-id N
    --run-epoch M``).

    The framework deliberately does **not** pass ``--network`` — the
    operator is responsible for ensuring AAICLICK_SQL_URL and
    AAICLICK_CH_URL resolve from inside a container (real hostnames or
    ``host.docker.internal``).

    ``--rm`` is intentionally **not** used. The host parent calls
    ``docker rm`` explicitly after reading the result row so that
    ``docker wait`` can race-freely report the exit code (a ``--rm``
    container that exits between polls disappears from the daemon
    before we can inspect it)."""
    cmd = [
        _docker_bin(),
        "run",
        "--detach",
        *add_host_flags("AAICLICK_DOCKER_RUN_ADD_HOST"),
        *resource_flags(resources),
        *_env_flags(env),
    ]
    cmd.extend(
        [
            image_tag,
            *REMOTE_ENTRYPOINT,
            "--task-id",
            str(task.id),
            "--run-epoch",
            str(task.run_epoch),
        ]
    )
    return cmd


async def _docker_pull_if_registered(image_tag: str) -> None:
    if get_registry() is None:
        return
    await cli.run(_docker_bin(), "pull", image_tag, check=False, stream=False)


async def _docker_run_detached(cmd: list[str], env: dict[str, str]) -> str:
    """Run ``docker run --detach`` with ``env`` (the ``_env_flags`` values)
    in the CLI's env; returns the container id."""
    rc, stdout, stderr = await cli.run(*cmd, check=False, stream=False, env=env)
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


class _DockerHandle(NamedTuple):
    container_id: str
    task_id: int
    run_epoch: int


class _DockerVehicle(TaskVehicle["_DockerHandle", "RunnerResult | None"]):
    """``TaskVehicle`` for module tasks on the Docker runner (shell and jvm
    tasks use :class:`_DockerShellVehicle`)."""

    def __init__(self, image_tag: str, env: dict[str, str], resources: dict | None) -> None:
        self._image_tag = image_tag
        self._env = env
        self._resources = resources

    async def launch(self, task: Task, execution_worker_id: int) -> _DockerHandle:
        cmd = _build_docker_run_cmd(task, self._image_tag, self._env, self._resources)
        container_id = await _docker_run_detached(cmd, self._env)
        return _DockerHandle(container_id, task.id, task.run_epoch)

    async def wait(self, handle: _DockerHandle, timeout: float | None) -> tuple[int, str | None, RunnerResult | None]:
        exit_code, error = await _wait_for_container(handle.container_id, timeout)
        result_row = await read_task_run_result(handle.task_id, handle.run_epoch)
        return exit_code, error, result_row

    async def poll_cancelled(self, task: Task) -> bool:
        return await check_run_aborted(task.id, task.run_epoch)

    async def terminate(self, handle: _DockerHandle) -> None:
        await _docker_kill(handle.container_id)

    def collect(
        self,
        handle: _DockerHandle,
        exit_code: int,
        error: str | None,
        was_cancelled: bool,
        payload: RunnerResult | None,
    ) -> RunnerResult:
        return collect_remote_result(exit_code, error, was_cancelled, payload, "container")

    async def output_argv(self, handle: _DockerHandle) -> list[str]:
        return [_docker_bin(), "logs", "--follow", handle.container_id]

    async def cleanup(self, handle: _DockerHandle) -> None:
        # We dropped --rm so we own cleanup.
        await _docker_rm(handle.container_id)


class _ShellHandle(NamedTuple):
    name: str
    io_dir: Path
    env_file: str | None


class _DockerShellVehicle(ShellVehicle["_ShellHandle"]):
    """Shell and jvm tasks on the Docker runner: ``docker create`` → ``cp``
    in.json in → ``start`` → wait → ``cp`` out.json out → ``rm``."""

    def __init__(self, image_tag: str, dispatch: JobDispatch) -> None:
        self._image_tag = image_tag
        self._dispatch = dispatch

    async def launch(self, task: Task, execution_worker_id: int) -> _ShellHandle:
        name = _shell_container_name(task)
        io_dir = await stage_in_json(task)
        # ``docker cp`` keeps the mode and makes root the owner, so the copied
        # dir is world-writable for images running as another user.
        io_dir.chmod(0o777)
        env_file = write_command_env_file(self._dispatch.command_env) if self._dispatch.command_env else None
        handle = _ShellHandle(name, io_dir, env_file)
        try:
            cmd = build_docker_create_cmd(
                name, self._image_tag, self._dispatch.command or [], self._dispatch.resources, env_file
            )
            await cli.run(*cmd, stream=False)
            # ``<dir>/.`` copies the contents, creating the target dir when the image lacks it.
            await cli.run(_docker_bin(), "cp", f"{io_dir}/.", f"{name}:{CONTAINER_IO_DIR}", stream=False)
            await cli.run(_docker_bin(), "start", name, stream=False)
        except BaseException:
            await self.cleanup(handle)
            raise
        return handle

    async def wait(self, handle: _ShellHandle, timeout: float | None) -> tuple[int, str | None, Any]:
        exit_code, error = await _wait_for_container(handle.name, timeout)
        if error is not None or exit_code != 0:
            return exit_code, error, None
        # Copy the whole IO dir back: a missing out.json is then simply absent
        # on the host, with no daemon error text to interpret.
        rc, out, err = await cli.run(
            _docker_bin(), "cp", f"{handle.name}:{CONTAINER_IO_DIR}/.", str(handle.io_dir), check=False, stream=False
        )
        if rc != 0:
            return exit_code, f"reading {OUT_JSON} failed: {err.strip() or out.strip()}", None
        try:
            return exit_code, None, read_out_json(handle.io_dir)
        except ValueError as e:
            return exit_code, str(e), None

    async def terminate(self, handle: _ShellHandle) -> None:
        await _docker_kill(handle.name)

    async def output_argv(self, handle: _ShellHandle) -> list[str]:
        return [_docker_bin(), "logs", "--follow", handle.name]

    async def cleanup(self, handle: _ShellHandle) -> None:
        await _docker_rm(handle.name)
        shutil.rmtree(handle.io_dir, ignore_errors=True)
        if handle.env_file is not None:
            Path(handle.env_file).unlink(missing_ok=True)


async def _run_shell_in_container(
    task: Task, execution_worker_id: int, dispatch: JobDispatch, log_run_id: int | None
) -> tuple[bool, dict | None, str | None]:
    """ExecuteFn for shell and jvm tasks on the Docker runner."""
    image_tag = await resolve_launch_image(dispatch.image_source, task_id=task.id)
    await _docker_pull_if_registered(image_tag)
    vehicle = _DockerShellVehicle(image_tag, dispatch)
    return await drive_shell_vehicle(
        task, execution_worker_id, vehicle, timeout=parse_task_timeout(), log_run_id=log_run_id
    )


async def _run_task_in_container(
    task: Task, execution_worker_id: int, dispatch: JobDispatch, log_run_id: int | None
) -> tuple[bool, dict | None, str | None]:
    """ExecuteFn for the Docker runner.

    Pulls the image (when a registry is configured), then hands a
    ``_DockerVehicle`` to the shared ``drive_vehicle`` driver, which
    heartbeats and polls for cancellation while the container runs.
    Cancellation and timeout both terminate the container via
    ``docker kill``."""
    image_tag = await resolve_launch_image(dispatch.image_source, task_id=task.id)
    await _docker_pull_if_registered(image_tag)

    timeout = parse_task_timeout()

    vehicle = _DockerVehicle(image_tag, build_runner_env(), dispatch.resources)
    result = await drive_vehicle(
        task,
        execution_worker_id,
        vehicle,
        timeout=timeout,
        poll_interval=POLL_INTERVAL,
        heartbeat_fn=execution_worker_heartbeat,
        log_run_id=log_run_id,
    )
    return result.success, result.result_ref, result.error
