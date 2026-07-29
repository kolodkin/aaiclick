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

from ..docker_config import effective_image_tag
from ..models import RUNNER_DOCKER, RUNNER_KUBERNETES, RUNNER_SUBPROCESS, Job, RunnerMode, Task
from ..orch_context import get_sql_session
from ..runner_config import BUILD_TASK_ENTRYPOINT, ENTRY_SHELL, KubernetesRunner, RunnerConfigT, parse_runner_config
from .docker_worker import _docker_pull_if_registered, _run_task_in_container, build_shell_run_spec
from .execution_worker import JobDispatch
from .image_builder import resolve_image_tag
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


async def _resolve_dispatch(task: Task) -> JobDispatch:
    """Pick the runner for a task and snapshot its job's launch spec.

    The auto-injected image-build task always runs on the host (subprocess)
    runner — it produces the image the rest of the job's container/pod tasks
    need. Every other task inherits the job's ``runner_mode``."""
    if task.entrypoint == BUILD_TASK_ENTRYPOINT:
        return JobDispatch(RUNNER_SUBPROCESS, None, None)

    async with get_sql_session() as session:
        job = (await session.execute(select(Job).where(Job.id == task.job_id))).scalar_one_or_none()
    if job is None:
        return JobDispatch(RUNNER_SUBPROCESS, None, None)
    runner = parse_runner_config(job.runner) if job.runner else None
    image_tag = effective_image_tag(runner) if runner is not None else None
    image_source = getattr(runner, "image", None)
    return JobDispatch(
        job.runner_mode,
        image_tag,
        _kube_dict(runner),
        task.entry_type,
        task.command,
        task.command_env,
        image_source,
    )


# Image-based runners need the dispatch snapshot; subprocess is the default and
# needs nothing, so it stays off the registry rather than carry an unused arg.
_IMAGE_RUNNERS: dict[RunnerMode, Callable[[Task, int, JobDispatch], Awaitable[ExecuteResult]]] = {
    RUNNER_DOCKER: _run_task_in_container,
    RUNNER_KUBERNETES: _run_task_in_pod,
}


async def build_shell_spec(task: Task, dispatch: JobDispatch, execution_worker_id: int) -> ShellSpec:
    """Resolve a shell task's launch command for its runner mode.

    Subprocess mode runs the argv directly; container modes wrap it as a
    foreground ``docker run`` / ``kubectl run`` so the wrapper's exit code
    and merged stdout are the task's."""
    if dispatch.runner_mode == RUNNER_DOCKER:
        image_tag = await resolve_image_tag(task, dispatch.image_source, dispatch.image_tag, execution_worker_id)
        await _docker_pull_if_registered(image_tag)
        return build_shell_run_spec(task, image_tag)
    if dispatch.runner_mode == RUNNER_KUBERNETES:
        image_tag = await resolve_image_tag(task, dispatch.image_source, dispatch.image_tag, execution_worker_id)
        return build_shell_pod_spec(task, dispatch, image_tag)
    return ShellSpec(dispatch.command or [], dispatch.command_env)


async def dispatch_execute(task: Task, execution_worker_id: int) -> ExecuteResult:
    """ExecuteFn that picks the runner per task."""
    dispatch = await _resolve_dispatch(task)
    if dispatch.entry_type == ENTRY_SHELL:
        spec = await build_shell_spec(task, dispatch, execution_worker_id)
        return await _run_task_in_child(task, execution_worker_id, shell_spec=spec)
    handler = _IMAGE_RUNNERS.get(dispatch.runner_mode)
    if handler is not None:
        return await handler(task, execution_worker_id, dispatch)
    return await _run_task_in_child(task, execution_worker_id)
