"""Commit-time image stamping, validation, and build-task injection.

Called from every commit point (``orch_context.commit_tasks`` and
``factories.create_built_job``) so a committed task's ``image_source`` is
final by the time its row lands — dispatch never resolves inheritance. It
injects one ordinary build task per distinct build image and wires
``build >> dependent`` edges; the scheduler's existing dependency filter is
the whole coordination story (spec: docs/designs/orchestration.md "Image source").
Whether the build pushes to a registry or stays host-local is the worker's
decision (``docker_config.get_build_mode``), so submission needs no build env.

Deliberately imports neither ``factories`` nor ``orch_context`` (both reach
this module), building ``Task`` rows directly instead.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..datetime_utils import utc_now
from ..snowflake import get_snowflake_id
from .docker_config import get_registry, image_key
from .execution.image_build_task import IMAGE_BUILD_ENTRYPOINT, build_task_name
from .models import RUNNER_DOCKER, RUNNER_KUBERNETES, TASK_PENDING, Job, RunnerMode, Task
from .runner_config import ImageBuild, parse_image_source

BUILD_TASK_MAX_RETRIES = 2


def stamp_inherited_image(tasks: list[Task], parent_image_source: dict | None) -> None:
    """Fill ``image_source`` on tasks that didn't declare their own.

    ``parent_image_source`` is the committing task's own stamped value (or
    None outside task execution / for a NULL-image parent, in which case
    undeclared children stay NULL ⇒ host subprocess)."""
    if parent_image_source is None:
        return
    for task in tasks:
        if task.image_source is None:
            task.image_source = parent_image_source


def validate_image_sources(tasks: list[Task], runner_mode: RunnerMode) -> None:
    """Enforce the commit-point rules; raises ``ValueError``."""
    for task in tasks:
        if task.image_source is None:
            continue
        if runner_mode not in (RUNNER_DOCKER, RUNNER_KUBERNETES):
            raise ValueError(
                f"task {task.name!r} declares an image_source but the job's runner_mode "
                f"is {runner_mode!r}; images are only valid on docker/kubernetes jobs"
            )
        source = parse_image_source(task.image_source)
        if isinstance(source, ImageBuild) and runner_mode == RUNNER_KUBERNETES and get_registry() is None:
            raise ValueError(
                "kubernetes build image sources require AAICLICK_REGISTRY — "
                "the cluster cannot pull from a worker's local docker daemon (AAICLICK_LOCAL_BUILD)"
            )


def _make_build_task(source: ImageBuild, job_id: int) -> Task:
    """Build-task kwargs mirror ``run_image_build``'s signature exactly (the
    module task is invoked as ``run_image_build(**kwargs)``)."""
    return Task(
        id=get_snowflake_id(),
        job_id=job_id,
        entrypoint=IMAGE_BUILD_ENTRYPOINT,
        name=build_task_name(source),
        kwargs={
            "git_remote": source.git_remote,
            "git_sha": source.git_sha,
            "git_branch": source.git_branch,
            "dockerfile": source.dockerfile,
        },
        is_image_build=True,
        status=TASK_PENDING,
        created_at=utc_now(),
        max_retries=BUILD_TASK_MAX_RETRIES,
    )


def _build_task_key(build: Task) -> str:
    """Dedup key of an existing build task, recomputed from its stored build
    coordinates — no derived value is persisted, so the key algorithm can
    change without diverging from old rows."""
    source = ImageBuild(
        git_remote=build.kwargs["git_remote"],
        git_sha=build.kwargs["git_sha"],
        dockerfile=build.kwargs.get("dockerfile"),
    )
    return image_key(source)


async def inject_build_tasks(session: AsyncSession, tasks: list[Task], job: Job) -> list[Task]:
    """Ensure a build task exists per distinct build image and wire
    ``build >> dependent`` edges onto ``tasks``.

    Docker/kubernetes jobs only. Returns newly created build tasks — the
    caller commits them alongside ``tasks``. Two concurrent commits in one job
    can race past the lookup and double-inject; both builds are cache-first
    (registry pull or local daemon) so the loser is a cheap no-op (accepted,
    spec "Races")."""
    if job.runner_mode not in (RUNNER_DOCKER, RUNNER_KUBERNETES):
        return []

    groups: dict[str, tuple[ImageBuild, list[Task]]] = {}
    for task in tasks:
        if task.image_source is None or task.is_image_build:
            continue
        source = parse_image_source(task.image_source)
        if not isinstance(source, ImageBuild):
            continue
        groups.setdefault(image_key(source), (source, []))[1].append(task)
    if not groups:
        return []

    existing = (
        (await session.execute(select(Task).where(Task.job_id == job.id, Task.is_image_build == True)))  # noqa: E712
        .scalars()
        .all()
    )
    build_by_key: dict[str, Task] = {_build_task_key(t): t for t in existing}

    injected: list[Task] = []
    for key, (source, dependents) in groups.items():
        build = build_by_key.get(key)
        if build is None:
            build = _make_build_task(source, job.id)
            build_by_key[key] = build
            injected.append(build)
        for task in dependents:
            task.depends_on(build)
    return injected
