"""Per-task runner dispatch.

Neutral home for the routing that maps a task to its execution vehicle, so no
single runner module owns the cross-cutting dispatcher. ``_execution_worker_loop`` plugs
``dispatch_execute`` in as its ``ExecuteFn``; a job whose tasks span runners
(e.g. subprocess and container tasks) is served by one worker without runner
affinity rules.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path

from sqlmodel import select

from ..kubernetes_config import resolve_pod_config
from ..models import Job, Task
from ..orch_context import get_sql_session
from ..runner_config import ENTRY_JVM, ENTRY_SHELL, parse_image_source
from .docker_build import resolve_launch_image
from .docker_worker import _docker_pull_if_registered, _run_task_in_container, build_shell_run_spec
from .execution_worker import JobDispatch
from .kubernetes_worker import _run_task_in_pod, build_shell_pod_spec
from .mp_worker import _run_task_in_child
from .runner import ShellSpec, register_host_log_run
from .runner_env import ENV_WORKER_RUNNER, RUNNER_DOCKER, RUNNER_KUBERNETES, WorkerRunner, get_worker_runner

ExecuteResult = tuple[bool, dict | None, str | None]


class DispatchError(RuntimeError):
    """A task cannot be routed on this worker (e.g. a container task on a
    worker without ``AAICLICK_RUNNER``). Raised from ``_resolve_dispatch``; the
    worker loop records it as the task's failure."""


def _subprocess_dispatch(task: Task) -> JobDispatch:
    return JobDispatch(None, None, task.entry_type, task.command, task.command_env, None)


async def _resolve_dispatch(task: Task) -> JobDispatch:
    """Pick the runner for a task from its own ``image_source`` and the worker.

    NULL ``image_source`` ⇒ host subprocess on any worker — the rule that
    host-pins injected build tasks (spec: docs/designs/orchestration.md "Image
    source"). A container task runs on the worker's ``AAICLICK_RUNNER``; the
    job row is read only for the Pod ``resources`` snapshot."""
    if task.image_source is None:
        return _subprocess_dispatch(task)
    runner = get_worker_runner()
    if runner is None:
        raise DispatchError(
            f"task {task.name!r} declares an image_source but this worker has no {ENV_WORKER_RUNNER}; "
            "set it to docker or kubernetes on the worker"
        )
    source = parse_image_source(task.image_source)
    async with get_sql_session() as session:
        job = (await session.execute(select(Job).where(Job.id == task.job_id))).scalar_one_or_none()
    resources = job.resources if job is not None else None
    pod_config = resolve_pod_config(resources=resources) if runner == RUNNER_KUBERNETES else None
    return JobDispatch(runner, pod_config, task.entry_type, task.command, task.command_env, source)


# Image-based runners need the dispatch snapshot and the host-registered log
# run_id; subprocess is the default and needs neither, so it stays off the
# registry rather than carry unused args.
_IMAGE_RUNNERS: dict[WorkerRunner, Callable[[Task, int, JobDispatch, int | None], Awaitable[ExecuteResult]]] = {
    RUNNER_DOCKER: _run_task_in_container,
    RUNNER_KUBERNETES: _run_task_in_pod,
}


async def build_shell_spec(task: Task, dispatch: JobDispatch) -> ShellSpec:
    """Resolve a shell task's launch command for its runner mode.

    Subprocess mode runs the argv directly; container modes wrap it as a
    foreground ``docker run`` / ``kubectl run`` so the wrapper's exit code
    and merged stdout are the task's."""
    if dispatch.runner == RUNNER_DOCKER:
        image_tag = await resolve_launch_image(dispatch.image_source, task_id=task.id)
        await _docker_pull_if_registered(image_tag)
        return build_shell_run_spec(task, image_tag)
    if dispatch.runner == RUNNER_KUBERNETES:
        image_tag = await resolve_launch_image(dispatch.image_source, task_id=task.id)
        return await build_shell_pod_spec(task, dispatch, image_tag)
    return ShellSpec(dispatch.command or [], dispatch.command_env)


async def dispatch_execute(task: Task, execution_worker_id: int) -> ExecuteResult:
    """ExecuteFn that picks the runner per task."""
    dispatch = await _resolve_dispatch(task)
    if dispatch.entry_type == ENTRY_SHELL:
        spec = await build_shell_spec(task, dispatch)
        try:
            return await _run_task_in_child(task, execution_worker_id, shell_spec=spec)
        finally:
            # The child may be killed before its own cleanup runs, so the
            # env file (command_env values) is removed here, in the parent.
            if spec.env_file is not None:
                Path(spec.env_file).unlink(missing_ok=True)
    handler = _IMAGE_RUNNERS.get(dispatch.runner) if dispatch.runner is not None else None
    if dispatch.entry_type == ENTRY_JVM and handler is None:
        # Commit-point validation (validate_jvm_tasks) blocks this; the guard
        # keeps a stray row from being executed as a Python module task.
        return False, None, "jvm task requires a docker/kubernetes runner with an image_source"
    if handler is not None:
        log_run_id = await register_host_log_run(task, dispatch.entry_type)
        return await handler(task, execution_worker_id, dispatch, log_run_id)
    return await _run_task_in_child(task, execution_worker_id)
