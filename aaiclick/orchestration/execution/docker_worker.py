"""Docker runner — host-side ExecuteFn driving containers.

``_run_task_in_container`` runs in the host worker process; it pulls the
image (when a registry is configured), spawns ``docker run``, sends
heartbeats and watches for cancellation while the container runs, then
reads the ``RemoteTaskResult`` row the container wrote. The container runs
the shared entrypoint (``remote_result``), the same one the kubernetes
runner's Pods use.
"""

from __future__ import annotations

import asyncio
import os
from typing import NamedTuple

from ..docker_config import add_host_flags, get_registry
from ..models import Task
from ..runner_config import ENTRY_JVM
from . import cli
from .claiming import check_task_cancelled
from .docker_build import resolve_launch_image
from .execution_worker import (
    POLL_INTERVAL,
    JobDispatch,
    RunnerResult,
    TaskVehicle,
    drive_vehicle,
    execution_worker_heartbeat,
    parse_task_timeout,
)
from .remote_result import REMOTE_ENTRYPOINT, collect_remote_result, read_task_run_result
from .runner import ShellSpec
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


def _shell_container_name(task: Task) -> str:
    """Unique-per-attempt container name so cleanup can address it."""
    return f"aaiclick-task-{task.id}-{task.run_epoch}"


def build_shell_run_spec(task: Task, image_tag: str) -> ShellSpec:
    """Wrap a shell task's argv as a foreground ``docker run``.

    Only ``command_env`` is injected — no IPC mount, no runner env, so no
    aaiclick secrets reach a vanilla user image. ``--rm`` is safe here
    (unlike module tasks' detached run): the docker CLI is the wrapper
    process, so its own exit code *is* the container's — no ``docker wait``
    race. ``cleanup_argv`` kills the container by name for the
    timeout/cancel path, where killing the CLI alone would leave it
    running."""
    name = _shell_container_name(task)
    argv = [
        _docker_bin(),
        "run",
        "--rm",
        "--name",
        name,
        *add_host_flags("AAICLICK_DOCKER_RUN_ADD_HOST"),
    ]
    for key, value in (task.command_env or {}).items():
        argv.extend(["-e", f"{key}={value}"])
    argv.append(image_tag)
    argv.extend(task.command or [])
    return ShellSpec(argv, None, cleanup_argv=[_docker_bin(), "kill", name])


def _build_docker_run_cmd(
    task: Task,
    image_tag: str,
    env: dict[str, str],
) -> list[str]:
    """Construct the detached ``docker run`` command line for a module or jvm
    task: inject the full runner env and run the in-container bootstrap shim.
    For ``module`` that is the shared Python entrypoint
    (``python -m ...remote_result --task-id N --run-epoch M``); for ``jvm``
    only the ``--task-id``/``--run-epoch`` arguments are passed — the image's
    own ``ENTRYPOINT`` is the aaiclick-task-api shim (spec:
    docs/designs/java-sdk.md).

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
    ]
    for key, value in env.items():
        cmd.extend(["-e", f"{key}={value}"])
    entrypoint = [] if task.entry_type == ENTRY_JVM else REMOTE_ENTRYPOINT
    cmd.extend(
        [
            image_tag,
            *entrypoint,
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


class _DockerHandle(NamedTuple):
    container_id: str
    task_id: int
    run_epoch: int


class _DockerVehicle(TaskVehicle["_DockerHandle", "RunnerResult | None"]):
    """``TaskVehicle`` for the Docker runner (module tasks — shell tasks run
    through the mp task child with a ``build_shell_run_spec`` argv)."""

    def __init__(self, image_tag: str, env: dict[str, str]) -> None:
        self._image_tag = image_tag
        self._env = env

    async def launch(self, task: Task, execution_worker_id: int) -> _DockerHandle:
        cmd = _build_docker_run_cmd(task, self._image_tag, self._env)
        container_id = await _docker_run_detached(cmd)
        return _DockerHandle(container_id, task.id, task.run_epoch)

    async def wait(self, handle: _DockerHandle, timeout: float | None) -> tuple[int, str | None, RunnerResult | None]:
        exit_code, error = await _wait_for_container(handle.container_id, timeout)
        result_row = await read_task_run_result(handle.task_id, handle.run_epoch)
        return exit_code, error, result_row

    async def poll_cancelled(self, task: Task) -> bool:
        return await check_task_cancelled(task.id)

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

    async def cleanup(self, handle: _DockerHandle) -> None:
        # We dropped --rm so we own cleanup.
        await _docker_rm(handle.container_id)


async def _run_task_in_container(
    task: Task, execution_worker_id: int, dispatch: JobDispatch
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

    vehicle = _DockerVehicle(image_tag, build_runner_env())
    result = await drive_vehicle(
        task,
        execution_worker_id,
        vehicle,
        timeout=timeout,
        poll_interval=POLL_INTERVAL,
        heartbeat_fn=execution_worker_heartbeat,
    )
    return result.success, result.result_ref, result.error
