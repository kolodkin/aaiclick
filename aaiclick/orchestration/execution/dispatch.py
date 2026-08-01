"""Per-task runner dispatch.

Neutral home for the routing that maps a task to its execution vehicle, so no
single runner module owns the cross-cutting dispatcher. ``_execution_worker_loop`` plugs
``dispatch_execute`` in as its ``ExecuteFn``; a job whose tasks span runners
(e.g. subprocess and container tasks) is served by one worker without runner
affinity rules.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from sqlmodel import select

from ..models import RUNNER_DOCKER, RUNNER_KUBERNETES, RUNNER_SUBPROCESS, Job, RunnerMode, Task
from ..orch_context import get_sql_session
from ..runner_config import (
    ENTRY_SHELL,
    KubernetesRunner,
    RunnerConfigT,
    parse_image_source,
    parse_runner_config,
)
from .docker_build import resolve_launch_image
from .docker_worker import _docker_pull_if_registered, _run_task_in_container, build_shell_run_spec
from .execution_worker import JobDispatch
from .kubernetes_worker import _run_task_in_pod, build_shell_pod_spec
from .mp_worker import _run_task_in_child
from .runner import ShellSpec

ExecuteResult = tuple[bool, dict | None, str | None]


def _kube_dict(runner: RunnerConfigT | None) -> dict | None:
    """Reconstruct the kubernetes_config dict the k8s worker expects from a
    KubernetesRunner; None for any other runner."""
    if not isinstance(runner, KubernetesRunner):
        return None
    return {
        "namespace": runner.namespace,
        "service_account": runner.service_account,
        "image_pull_secret": runner.image_pull_secret,
        "resources": runner.resources,
    }


def _subprocess_dispatch(task: Task) -> JobDispatch:
    return JobDispatch(RUNNER_SUBPROCESS, None, task.entry_type, task.command, task.command_env, None)


async def _resolve_dispatch(task: Task) -> JobDispatch:
    """Pick the runner for a task from its own ``image_source``.

    NULL ``image_source`` ⇒ host subprocess, regardless of the job's
    ``runner_mode`` — the rule that host-pins injected build tasks (spec:
    docs/designs/orchestration.md "Image source"). Container tasks read the
    job row only for ``runner_mode`` and kubernetes cluster config."""
    if task.image_source is None:
        return _subprocess_dispatch(task)
    source = parse_image_source(task.image_source)
    async with get_sql_session() as session:
        job = (await session.execute(select(Job).where(Job.id == task.job_id))).scalar_one_or_none()
    if job is None:
        return _subprocess_dispatch(task)
    runner = parse_runner_config(job.runner) if job.runner else None
    return JobDispatch(
        job.runner_mode,
        _kube_dict(runner),
        task.entry_type,
        task.command,
        task.command_env,
        source,
    )


# Image-based runners need the dispatch snapshot; subprocess is the default and
# needs nothing, so it stays off the registry rather than carry an unused arg.
_IMAGE_RUNNERS: dict[RunnerMode, Callable[[Task, int, JobDispatch], Awaitable[ExecuteResult]]] = {
    RUNNER_DOCKER: _run_task_in_container,
    RUNNER_KUBERNETES: _run_task_in_pod,
}


async def build_shell_spec(task: Task, dispatch: JobDispatch) -> ShellSpec:
    """Resolve a shell task's launch command for its runner mode.

    Subprocess mode runs the argv directly; container modes wrap it as a
    foreground ``docker run`` / ``kubectl run`` so the wrapper's exit code
    and merged stdout are the task's."""
    if dispatch.runner_mode == RUNNER_DOCKER:
        image_tag = await resolve_launch_image(dispatch.image_source, task_id=task.id)
        await _docker_pull_if_registered(image_tag)
        return build_shell_run_spec(task, image_tag)
    if dispatch.runner_mode == RUNNER_KUBERNETES:
        image_tag = await resolve_launch_image(dispatch.image_source, task_id=task.id)
        return build_shell_pod_spec(task, dispatch, image_tag)
    return ShellSpec(dispatch.command or [], dispatch.command_env)


async def dispatch_execute(task: Task, execution_worker_id: int) -> ExecuteResult:
    """ExecuteFn that picks the runner per task."""
    dispatch = await _resolve_dispatch(task)
    if dispatch.entry_type == ENTRY_SHELL:
        spec = await build_shell_spec(task, dispatch)
        return await _run_task_in_child(task, execution_worker_id, shell_spec=spec)
    handler = _IMAGE_RUNNERS.get(dispatch.runner_mode)
    if handler is not None:
        return await handler(task, execution_worker_id, dispatch)
    return await _run_task_in_child(task, execution_worker_id)
