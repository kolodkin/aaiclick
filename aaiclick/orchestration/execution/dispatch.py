"""Per-task runner dispatch.

Neutral home for the routing that maps a task to its execution vehicle, so no
single runner module owns the cross-cutting dispatcher. ``_worker_loop`` plugs
``dispatch_execute`` in as its ``ExecuteFn``; a mixed job (e.g. a docker job's
host-side build task + container tasks) is served by one worker without runner
affinity rules.
"""

from __future__ import annotations

from sqlmodel import select

from ..docker_config import BUILD_TASK_ENTRYPOINT
from ..models import RUNNER_DOCKER, RUNNER_KUBERNETES, RUNNER_SUBPROCESS, Job, RunnerMode, Task
from ..orch_context import get_sql_session
from .docker_worker import _run_task_in_container
from .kubernetes_worker import _run_task_in_pod
from .mp_worker import _run_task_in_child


async def _resolve_runner(task: Task) -> RunnerMode:
    """Pick the runner for a task.

    The auto-injected build task always runs on the host (subprocess) runner —
    it produces the image the rest of the job's container/pod tasks need. Every
    other task inherits the job's ``runner_mode``."""
    if task.entrypoint == BUILD_TASK_ENTRYPOINT:
        return RUNNER_SUBPROCESS

    async with get_sql_session() as session:
        result = await session.execute(select(Job).where(Job.id == task.job_id))
        job = result.scalar_one_or_none()
    return job.runner_mode if job is not None else RUNNER_SUBPROCESS


async def dispatch_execute(task: Task, worker_id: int) -> tuple[bool, dict | None, str | None, str | None]:
    """ExecuteFn that picks the runner per task."""
    runner = await _resolve_runner(task)
    if runner == RUNNER_DOCKER:
        return await _run_task_in_container(task, worker_id)
    if runner == RUNNER_KUBERNETES:
        return await _run_task_in_pod(task, worker_id)
    return await _run_task_in_child(task, worker_id)
