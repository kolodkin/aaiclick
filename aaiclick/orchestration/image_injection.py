"""Commit-time image stamping, validation, and build-task injection.

Called from every commit point (``orch_context.commit_tasks`` and
``factories.create_container_job``) so a committed task's ``image_source`` is
final by the time its row lands — dispatch never resolves inheritance. It
injects one ordinary build task per distinct build image and wires
``build >> dependent`` edges; the scheduler's existing dependency filter is
the whole coordination story (spec: docs/designs/orchestration.md "Image source").
Whether the build pushes to a registry or stays host-local is the worker's
decision (``docker_config.get_build_mode``), so submission needs no build env.

Deliberately imports neither ``factories`` nor ``orch_context`` (both reach
this module), building ``Task`` rows directly instead.

See docs/designs/java-sdk.md — "Validation".
See docs/designs/task_interface.md — "Where it runs".
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.data.object.refs import NATIVE, UPSTREAM, ref_kind

from ..datetime_utils import utc_now
from ..snowflake import get_snowflake_id
from .docker_config import image_key
from .execution.image_build_task import IMAGE_BUILD_ENTRYPOINT, build_task_name
from .models import TASK_PENDING, Job, Task
from .runner_config import ENTRY_JVM, ENTRY_SHELL, EntryType, ImageBuild, parse_image_source

BUILD_TASK_MAX_RETRIES = 2


def _inherits(child: Task, parent_entry_type: EntryType) -> bool:
    """Whether a child with no image of its own can run in its parent's image.
    A shell argv runs anywhere; a module child needs a Python image (module or
    shell parents); a jvm child needs the shim's image (a jvm parent)."""
    if child.entry_type == ENTRY_SHELL:
        return True
    if child.entry_type == ENTRY_JVM:
        return parent_entry_type == ENTRY_JVM
    return parent_entry_type != ENTRY_JVM


def stamp_inherited_image(tasks: list[Task], parent_image_source: dict | None, parent_entry_type: EntryType) -> None:
    """Fill ``image_source`` on tasks that didn't declare their own and can run
    in the committing task's image (``_inherits``).

    ``parent_image_source`` is the committing task's own stamped value (or
    None outside task execution / for a NULL-image parent, in which case
    undeclared children stay NULL ⇒ host subprocess)."""
    if parent_image_source is None:
        return
    for task in tasks:
        if task.image_source is None and _inherits(task, parent_entry_type):
            task.image_source = parent_image_source


_PLAIN_REF_KINDS = (UPSTREAM, NATIVE)
"""Ref kinds the shell/jvm data plane carries: ``upstream`` is resolved host-side
into ``in.json``, ``native`` is unwrapped there."""


def _unsupported_plain_ref(value: Any) -> str | None:
    """Name of the first ref in a serialized kwarg value the shell/jvm data
    plane cannot carry, or None."""
    kind = ref_kind(value)
    if kind is not None:
        return None if kind in _PLAIN_REF_KINDS else f"{kind} ref"
    if isinstance(value, dict):
        value = list(value.values())
    if not isinstance(value, list):
        return None
    return next((r for r in map(_unsupported_plain_ref, value) if r), None)


def validate_plain_data_plane_tasks(tasks: list[Task]) -> None:
    """Enforce the shell/jvm commit-point rules; raises ``ValueError``.

    Both entry types read their kwargs from ``in.json`` (spec:
    docs/designs/orchestration.md "Shell entry type"), which carries plain
    values and resolved upstream refs only, so any other ref fails here, not
    at launch. A jvm task additionally needs a class name and its own image:
    there is no host-subprocess JVM contract."""
    for task in tasks:
        if task.entry_type not in (ENTRY_SHELL, ENTRY_JVM):
            continue
        if task.entry_type == ENTRY_JVM:
            if not task.entrypoint:
                raise ValueError(f"jvm task {task.name!r} requires a Java class name as its entrypoint")
            if task.image_source is None:
                raise ValueError(
                    f"jvm task {task.name!r} declares no image_source; jvm tasks run only in their own container image"
                )
        # Per kwarg: the kwargs map itself is never a ref.
        for name, value in (task.kwargs or {}).items():
            ref = _unsupported_plain_ref(value)
            if ref:
                raise ValueError(
                    f"{task.entry_type} task {task.name!r} receives a {ref} in kwarg {name!r}; "
                    "the shell/jvm data plane is plain values and upstream refs only"
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

    Returns newly created build tasks — the caller commits them alongside
    ``tasks``. Two concurrent commits in one job
    can race past the lookup and double-inject; both builds are cache-first
    (registry pull or local daemon) so the loser is a cheap no-op (accepted,
    spec "Races")."""
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
