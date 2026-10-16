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

from ..kubernetes_config import resolve_pod_config
from ..models import Job, Task
from ..orch_context import get_sql_session
from ..runner_config import ENTRY_JVM, ENTRY_SHELL, parse_image_source
from .docker_worker import _run_shell_in_container, _run_task_in_container
from .execution_worker import JobDispatch
from .kubernetes_worker import _run_shell_in_pod, _run_task_in_pod
from .mp_worker import _run_task_in_child
from .runner import ShellSpec, register_host_log_run, require_container_runner
from .runner_env import RUNNER_DOCKER, RUNNER_KUBERNETES, WorkerRunner, get_worker_runner

ExecuteResult = tuple[bool, dict | None, str | None]


def _subprocess_dispatch(task: Task) -> JobDispatch:
    return JobDispatch(None, None, task.entry_type, task.command, task.command_env, None)


async def _resolve_dispatch(task: Task) -> JobDispatch:
    """Pick the runner for a task from its own ``image_source`` and the worker.

    NULL ``image_source`` ⇒ host subprocess on any worker — the rule that
    host-pins injected build tasks (spec: docs/designs/orchestration.md "Image
    source"). A container task runs on the worker's ``AAICLICK_RUNNER`` with
    the job's ``resources`` snapshot."""
    if task.image_source is None:
        return _subprocess_dispatch(task)
    runner = get_worker_runner()
    require_container_runner(task, runner)
    source = parse_image_source(task.image_source)
    async with get_sql_session() as session:
        resources = (await session.execute(select(Job.resources).where(Job.id == task.job_id))).scalar_one_or_none()
    pod_config = resolve_pod_config() if runner == RUNNER_KUBERNETES else None
    return JobDispatch(runner, pod_config, task.entry_type, task.command, task.command_env, source, resources)


# Image-based runners need the dispatch snapshot and the host-registered log
# run_id; subprocess is the default and needs neither, so it stays off the
# registry rather than carry unused args.
_IMAGE_RUNNERS: dict[WorkerRunner, Callable[[Task, int, JobDispatch, int | None], Awaitable[ExecuteResult]]] = {
    RUNNER_DOCKER: _run_task_in_container,
    RUNNER_KUBERNETES: _run_task_in_pod,
}


_SHELL_RUNNERS: dict[WorkerRunner, Callable[[Task, int, JobDispatch, int | None], Awaitable[ExecuteResult]]] = {
    RUNNER_DOCKER: _run_shell_in_container,
    RUNNER_KUBERNETES: _run_shell_in_pod,
}
"""Shell/jvm tasks on the file data plane run host-side: the launch is a
multi-step create/cp/start sequence, not one argv for the mp child."""


async def dispatch_execute(task: Task, execution_worker_id: int) -> ExecuteResult:
    """ExecuteFn that picks the runner per task."""
    dispatch = await _resolve_dispatch(task)
    if dispatch.entry_type == ENTRY_SHELL:
        shell_handler = _SHELL_RUNNERS.get(dispatch.runner) if dispatch.runner is not None else None
        if shell_handler is not None:
            log_run_id = await register_host_log_run(task, ENTRY_SHELL)
            return await shell_handler(task, execution_worker_id, dispatch, log_run_id)
        spec = ShellSpec(dispatch.command or [], dispatch.command_env)
        return await _run_task_in_child(task, execution_worker_id, shell_spec=spec)
    handler = _IMAGE_RUNNERS.get(dispatch.runner) if dispatch.runner is not None else None
    if dispatch.entry_type == ENTRY_JVM and handler is None:
        # Commit-point validation (validate_plain_data_plane_tasks) blocks this; the guard
        # keeps a stray row from being executed as a Python module task.
        return False, None, "jvm task requires a docker/kubernetes runner with an image_source"
    if handler is not None:
        log_run_id = await register_host_log_run(task, dispatch.entry_type)
        return await handler(task, execution_worker_id, dispatch, log_run_id)
    return await _run_task_in_child(task, execution_worker_id)
