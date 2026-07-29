"""On-demand image build seam.

The primary caller is ``build_job_image`` — the body of the image-build task
injected into every build-source docker/kubernetes job (see
``factories.create_built_job``). The job's other tasks depend on that task, so
the scheduler never claims a container/pod task before its image is ``READY``
and no worker slot is spent waiting on an in-flight build.

``ensure_image`` guarantees the image exists, building it exactly once across
all concurrent workers and jobs via the ``build_tasks`` row:

- ``UNIQUE(image_key)`` => one build record per image identity.
- An atomic claim (``INSERT ... ON CONFLICT DO NOTHING`` / conditional reclaim
  ``UPDATE``) => one live builder; everyone else polls until ``READY``/``FAILED``.

The row is created at claim time (``BUILDING``), before the build starts, so an
in-flight build counts as "already in place" and concurrent tasks attach to it
instead of starting a second build. ``resolve_image_tag`` at container launch
remains as a fast path (the row is ``READY`` by dependency ordering) and as a
fallback that still builds inline if the row is missing.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import NamedTuple

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ...backend import is_sqlite
from ...datetime_utils import utc_now
from ...snowflake import get_snowflake_id
from ..docker_config import compute_image_tag, image_key
from ..models import BUILD_BUILDING, BUILD_FAILED, BUILD_READY, BuildTask, Job, Task
from ..orch_context import get_sql_session
from ..runner_config import ImageBuild, ImageSourceT, parse_runner_config
from .docker_build import build_image_to_tag
from .execution_worker_context import get_current_task_info

LEASE_SECONDS = 600
BUILD_POLL_INTERVAL = 2.0


class EnsuredImage(NamedTuple):
    image_tag: str
    build_task_id: int


class BuildFailed(RuntimeError):
    """An image build failed and exhausted its retries."""


def _lease_until():
    return utc_now() + timedelta(seconds=LEASE_SECONDS)


async def _get_row(key: str) -> BuildTask | None:
    async with get_sql_session() as session:
        return (await session.execute(select(BuildTask).where(BuildTask.image_key == key))).scalar_one_or_none()


async def _claim(source: ImageBuild, key: str, image_tag: str, holder_id: int) -> int | None:
    """Atomically claim the build. Returns the claimed ``BuildTask`` id, or None
    if another worker holds a live lease."""
    now = utc_now()
    async with get_sql_session() as session:
        insert = sqlite_insert if is_sqlite() else pg_insert
        stmt = (
            insert(BuildTask)
            .values(
                id=get_snowflake_id(),
                image_key=key,
                image_tag=image_tag,
                git_remote=source.git_remote,
                git_sha=source.git_sha,
                dockerfile=source.dockerfile,
                status=BUILD_BUILDING,
                holder_execution_worker_id=holder_id,
                lease_expires_at=_lease_until(),
                attempts=1,
                started_at=now,
            )
            .on_conflict_do_nothing(index_elements=["image_key"])
            .returning(BuildTask.id)
        )
        inserted_id = (await session.execute(stmt)).scalar_one_or_none()
        if inserted_id is None:
            # Row exists — try to reclaim a stale (expired) or retryable-failed lease.
            reclaim = (
                update(BuildTask)
                .where(
                    BuildTask.image_key == key,
                    # lease_expires_at is a nullable Column; the `< now` is SQL, not a Python
                    # comparison — pyright can't see through the ORM Optional here.
                    (BuildTask.lease_expires_at < now)  # pyright: ignore[reportOptionalOperand]
                    | ((BuildTask.status == BUILD_FAILED) & (BuildTask.attempts <= BuildTask.max_retries)),
                )
                .values(
                    status=BUILD_BUILDING,
                    holder_execution_worker_id=holder_id,
                    lease_expires_at=_lease_until(),
                    attempts=BuildTask.attempts + 1,
                    started_at=now,
                    finished_at=None,
                    error=None,
                )
                .returning(BuildTask.id)
            )
            reclaimed_id = (await session.execute(reclaim)).scalar_one_or_none()
            await session.commit()
            return reclaimed_id
        await session.commit()
        return inserted_id


async def _finish(build_task_id: int, holder_id: int, *, status: str, error: str | None) -> None:
    """Record the build outcome, but only if ``holder_id`` still holds the lease.

    A stale worker whose lease was reclaimed by another worker after an
    expired timeout will lose the ``holder_execution_worker_id`` match here, so its
    write affects zero rows instead of clobbering the current holder's
    result."""
    async with get_sql_session() as session:
        await session.execute(
            update(BuildTask)
            .where(BuildTask.id == build_task_id, BuildTask.holder_execution_worker_id == holder_id)
            .values(status=status, error=error, finished_at=utc_now())
        )
        await session.commit()


async def ensure_image(source: ImageBuild, holder_id: int) -> EnsuredImage:
    """Build (or wait for) the image for ``source``; return its tag and the
    ``BuildTask`` id it resolved to. Exactly one build runs per image identity.

    ``holder_id`` is the lease-fencing token stored in
    ``holder_execution_worker_id``: any id unique to this build attempt.
    Dispatch-time callers pass their execution worker id; ``build_job_image``
    mints a fresh snowflake per attempt so a stale attempt's finish write can
    never clobber a reclaiming builder's result."""
    key = image_key(source)
    image_tag = compute_image_tag(source.git_sha)
    while True:
        row = await _get_row(key)
        if row is not None:
            if row.status == BUILD_READY:
                return EnsuredImage(image_tag, row.id)
            if row.status == BUILD_FAILED and row.attempts > row.max_retries:
                raise BuildFailed(f"image build failed (BuildTask {row.id}): {row.error}")

        build_task_id = await _claim(source, key, image_tag, holder_id)
        if build_task_id is None:
            await asyncio.sleep(BUILD_POLL_INTERVAL)
            continue

        try:
            await build_image_to_tag(source, image_tag)
        except Exception as e:  # noqa: BLE001 — record any build error, then retry-or-raise
            await _finish(build_task_id, holder_id, status=BUILD_FAILED, error=f"{type(e).__name__}: {e}")
            continue

        await _finish(build_task_id, holder_id, status=BUILD_READY, error=None)
        return EnsuredImage(image_tag, build_task_id)


async def ensure_built_image(task_id: int, source: ImageBuild, holder_id: int) -> str:
    """Build the image for ``source`` on demand (once, shared across workers) and
    stamp the task's ``build_task_id`` link. Returns the resolved image tag."""
    ensured = await ensure_image(source, holder_id)
    async with get_sql_session() as session:
        await session.execute(update(Task).where(Task.id == task_id).values(build_task_id=ensured.build_task_id))
        await session.commit()
    return ensured.image_tag


async def build_job_image(job_id: int) -> None:
    """Body of the image-build task injected into build-source jobs.

    Every build-source docker/kubernetes job gets one of these as a root task
    that the entry task depends on (``factories.create_built_job``), so the
    scheduler holds back the job's container/pod tasks until the image is
    ``READY`` instead of letting workers claim them and wait on the build.
    Dispatch pins this task to the host (subprocess) runner — it produces the
    image the containers need — so it runs as a normal module task with logs,
    cancellation, and timeout handling.

    Delegates to ``ensure_built_image``: concurrent jobs on the same image
    share one ``build_tasks`` row, so this either builds, attaches to an
    in-flight build, or returns immediately on a ``READY`` row.
    """
    async with get_sql_session() as session:
        job = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one()
    runner = parse_runner_config(job.runner) if job.runner else None
    source = getattr(runner, "image", None)
    if not isinstance(source, ImageBuild):
        raise ValueError(f"Job {job_id} has no build image source; nothing to build")
    task_id = get_current_task_info().task_id
    await ensure_built_image(task_id, source, get_snowflake_id())


async def resolve_image_tag(
    task: Task, image_source: ImageSourceT | None, image_tag: str | None, holder_id: int
) -> str:
    """Resolve the image tag a task's container should run: build on demand for an
    ``ImageBuild`` source (stamping the task's ``build_task_id``), or the prebuilt
    tag verbatim. Shared by the docker and kubernetes runners so the build-vs-prebuilt
    decision lives in one place. Raises if a prebuilt source carries no tag."""
    if isinstance(image_source, ImageBuild):
        return await ensure_built_image(task.id, image_source, holder_id)
    if not image_tag:
        raise ValueError(f"Job {task.job_id} has no image_tag")
    return image_tag
