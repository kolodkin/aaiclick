"""Per-task runner dispatch.

Neutral home for the routing that maps a task to its execution vehicle, so no
single runner module owns the cross-cutting dispatcher. ``_worker_loop`` plugs
``dispatch_execute`` in as its ``ExecuteFn``; a mixed job (e.g. a docker job's
host-side build task + container tasks) is served by one worker without runner
affinity rules.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from sqlmodel import select

from ..docker_config import BUILD_TASK_ENTRYPOINT, effective_image_tag
from ..models import RUNNER_DOCKER, RUNNER_KUBERNETES, RUNNER_SUBPROCESS, Job, RunnerMode, Task
from ..orch_context import get_sql_session
from ..runner_config import KubernetesRunner, parse_runner_config
from .docker_worker import _run_task_in_container
from .kubernetes_worker import _run_task_in_pod
from .mp_worker import _run_task_in_child
from .worker import JobDispatch

ExecuteResult = tuple[bool, dict | None, str | None, str | None]


def _kube_dict(runner) -> dict | None:
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

    The auto-injected build task always runs on the host (subprocess) runner —
    it produces the image the rest of the job's container/pod tasks need. Every
    other task inherits the job's ``runner_mode``."""
    if task.entrypoint == BUILD_TASK_ENTRYPOINT:
        return JobDispatch(RUNNER_SUBPROCESS, None, None)

    async with get_sql_session() as session:
        job = (await session.execute(select(Job).where(Job.id == task.job_id))).scalar_one_or_none()
    if job is None:
        return JobDispatch(RUNNER_SUBPROCESS, None, None)
    runner = parse_runner_config(job.runner) if job.runner else None
    image_tag = effective_image_tag(runner) if runner is not None else None
    return JobDispatch(
        job.runner_mode,
        image_tag,
        _kube_dict(runner),
        task.entry_type,
        task.command,
        task.command_env,
    )


# Image-based runners need the dispatch snapshot; subprocess is the default and
# needs nothing, so it stays off the registry rather than carry an unused arg.
_IMAGE_RUNNERS: dict[RunnerMode, Callable[[Task, int, JobDispatch], Awaitable[ExecuteResult]]] = {
    RUNNER_DOCKER: _run_task_in_container,
    RUNNER_KUBERNETES: _run_task_in_pod,
}


async def dispatch_execute(task: Task, worker_id: int) -> ExecuteResult:
    """ExecuteFn that picks the runner per task."""
    dispatch = await _resolve_dispatch(task)
    handler = _IMAGE_RUNNERS.get(dispatch.runner_mode)
    if handler is not None:
        return await handler(task, worker_id, dispatch)
    return await _run_task_in_child(task, worker_id)
