"""CRUD operations for registered jobs."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from croniter import croniter
from sqlmodel import select

from ..backend import is_local
from ..datetime_utils import utc_now
from ..snowflake import get_snowflake_id
from .docker_config import requested_image_kind, resolve_image_source
from .factories import create_container_job, create_job, create_task
from .models import RUN_MANUAL, Job, PreservationMode, RegisteredJob, RunType
from .orch_context import get_sql_session
from .runner_config import (
    ENTRY_JVM,
    ENTRY_MODULE,
    EntryType,
    validate_image_exclusivity,
    validate_task_entry,
)


class RegisteredJobAlreadyExists(ValueError):
    """Raised when registering a name that already exists."""


class RegisteredJobNotFound(ValueError):
    """Raised when enabling/disabling a non-existent registration."""


def _by_name(name: str):
    """Select the registration named ``name``."""
    return select(RegisteredJob).where(RegisteredJob.name == name)


def compute_next_run(cron_expr: str, after: datetime | None = None) -> datetime:
    """Compute the next fire time for a cron expression.

    Args:
        cron_expr: Cron expression (e.g. "0 8 * * *")
        after: Base time to compute from (default: utcnow)

    Returns:
        Next fire datetime
    """
    base = after or utc_now()
    return croniter(cron_expr, base).get_next(datetime)


def _next_run_at(schedule: str | None, enabled: bool, now: datetime) -> datetime | None:
    """Compute next_run_at from schedule if enabled, else None."""
    return compute_next_run(schedule, now) if schedule and enabled else None


_RESOURCES_REQUIRE_IMAGE = "resources require an image source (build or image)"


def _validate_registration_fields(
    *,
    image: str | None,
    build: bool,
    git_remote: str | None,
    dockerfile: str | None,
    resources: dict[str, Any] | None,
) -> None:
    """Refuse to store defaults no run would read: build modifiers without
    ``build``, ``resources`` without an image source, or both sides at once."""
    validate_image_exclusivity(image, git_remote, dockerfile, build=build)
    modifiers = [name for name, value in (("git_remote", git_remote), ("dockerfile", dockerfile)) if value is not None]
    if modifiers and not build:
        # A registration default that is not a build must not become one
        # silently (runs, by contrast, let a modifier imply --build).
        raise ValueError(f"set without build (--build on the CLI): {', '.join(modifiers)}")
    if resources is not None and not (build or image is not None):
        raise ValueError(_RESOURCES_REQUIRE_IMAGE)


def _build_registered_job(
    *,
    name: str,
    entrypoint: str,
    schedule: str | None,
    default_kwargs: dict[str, Any] | None,
    enabled: bool,
    preservation_mode: PreservationMode | None,
    build: bool,
    dockerfile: str | None,
    git_remote: str | None,
    image: str | None,
    resources: dict[str, Any] | None,
    now: datetime,
) -> RegisteredJob:
    """Build an uncommitted RegisteredJob row with computed next_run_at."""
    return RegisteredJob(
        id=get_snowflake_id(),
        name=name,
        entrypoint=entrypoint,
        enabled=enabled,
        schedule=schedule,
        default_kwargs=default_kwargs,
        preservation_mode=preservation_mode,
        build=build,
        dockerfile=dockerfile,
        git_remote=git_remote,
        image=image,
        resources=resources,
        next_run_at=_next_run_at(schedule, enabled, now),
        created_at=now,
        updated_at=now,
    )


async def register_job(
    *,
    name: str,
    entrypoint: str,
    schedule: str | None = None,
    default_kwargs: dict[str, Any] | None = None,
    enabled: bool = True,
    preservation_mode: PreservationMode | None = None,
    build: bool = False,
    image: str | None = None,
    git_remote: str | None = None,
    dockerfile: str | None = None,
    resources: dict[str, Any] | None = None,
) -> RegisteredJob:
    """Register a new job in the catalog.

    Args:
        name: Unique job name
        entrypoint: Python dotted path (e.g. "myapp.pipelines.etl_job")
        schedule: Cron expression for scheduled runs (optional)
        default_kwargs: Default kwargs for scheduled runs (optional)
        enabled: Whether the job is enabled (default: True)
        preservation_mode: Default preservation mode for every run of
            this job. Individual runs can override via ``run_job()``.
        build: Build the task image from the repo at the submitted commit;
            runs of this job are containerized. Mutually exclusive with
            ``image``.
        image: Default prebuilt image tag; runs of this job are
            containerized from it. Mutually exclusive with ``build``.
        git_remote: Build modifier — default git remote URL. ``None`` falls
            back to ``git config remote.origin.url`` at submission time.
        dockerfile: Build modifier — default Dockerfile path relative to
            the repo root. ``None`` falls back to ``"Dockerfile"``.
        resources: Default Kubernetes requests/limits for every run's Pods
            (ignored by the docker runner).

    Returns:
        Created RegisteredJob

    Raises:
        ValueError: If ``image`` is combined with the build side, a build
            modifier is set without ``build``, or ``resources`` is set on a
            subprocess registration.
        RegisteredJobAlreadyExists: If a job with this name already exists.
    """
    _validate_registration_fields(
        image=image, build=build, git_remote=git_remote, dockerfile=dockerfile, resources=resources
    )
    now = utc_now()
    registered_job = _build_registered_job(
        name=name,
        entrypoint=entrypoint,
        schedule=schedule,
        default_kwargs=default_kwargs,
        enabled=enabled,
        preservation_mode=preservation_mode,
        build=build,
        dockerfile=dockerfile,
        git_remote=git_remote,
        image=image,
        resources=resources,
        now=now,
    )

    async with get_sql_session() as session:
        existing = await session.execute(_by_name(name))
        if existing.scalar_one_or_none() is not None:
            raise RegisteredJobAlreadyExists(f"Registered job '{name}' already exists")

        session.add(registered_job)
        await session.commit()
        await session.refresh(registered_job)

    return registered_job


async def get_registered_job(name: str) -> RegisteredJob | None:
    """Look up a registered job by name.

    Args:
        name: Job name

    Returns:
        RegisteredJob if found, None otherwise
    """
    async with get_sql_session() as session:
        result = await session.execute(_by_name(name))
        return result.scalar_one_or_none()


async def upsert_registered_job(
    *,
    name: str,
    entrypoint: str,
    schedule: str | None = None,
    default_kwargs: dict[str, Any] | None = None,
    enabled: bool = True,
    preservation_mode: PreservationMode | None = None,
    build: bool = False,
    image: str | None = None,
    git_remote: str | None = None,
    dockerfile: str | None = None,
    resources: dict[str, Any] | None = None,
) -> RegisteredJob:
    """Insert or update a registered job.

    If a job with the given name exists, updates entrypoint, schedule,
    default_kwargs, preservation_mode and the image-source defaults.
    Otherwise creates a new entry.

    Args:
        name: Unique job name
        entrypoint: Python dotted path
        schedule: Cron expression (optional)
        default_kwargs: Default parameters (optional)
        enabled: Whether the job is enabled
        preservation_mode: Default preservation mode for every run
        build: Build the task image from the repo; mutually exclusive with
            ``image``.
        image: Default prebuilt image tag; mutually exclusive with ``build``.
        git_remote: Build modifier — default git remote URL.
        dockerfile: Build modifier — default Dockerfile path.
        resources: Default Kubernetes requests/limits for every run's Pods.

    Returns:
        The created or updated RegisteredJob

    Raises:
        ValueError: See ``register_job``.
    """
    _validate_registration_fields(
        image=image, build=build, git_remote=git_remote, dockerfile=dockerfile, resources=resources
    )
    now = utc_now()

    async with get_sql_session() as session:
        result = await session.execute(_by_name(name))
        existing = result.scalar_one_or_none()

        if existing is not None:
            existing.entrypoint = entrypoint
            existing.schedule = schedule
            existing.default_kwargs = default_kwargs
            existing.preservation_mode = preservation_mode
            existing.enabled = enabled
            existing.build = build
            existing.dockerfile = dockerfile
            existing.git_remote = git_remote
            existing.image = image
            existing.resources = resources
            existing.updated_at = now
            existing.next_run_at = _next_run_at(schedule, enabled, now)
            session.add(existing)
            await session.commit()
            await session.refresh(existing)
            return existing

        registered_job = _build_registered_job(
            name=name,
            entrypoint=entrypoint,
            schedule=schedule,
            default_kwargs=default_kwargs,
            enabled=enabled,
            preservation_mode=preservation_mode,
            build=build,
            dockerfile=dockerfile,
            git_remote=git_remote,
            image=image,
            resources=resources,
            now=now,
        )
        session.add(registered_job)
        await session.commit()
        await session.refresh(registered_job)
        return registered_job


async def _get_registered_or_raise(session, name: str) -> RegisteredJob:
    """Fetch a registration by name or raise RegisteredJobNotFound."""
    result = await session.execute(_by_name(name))
    job = result.scalar_one_or_none()
    if job is None:
        raise RegisteredJobNotFound(f"Registered job '{name}' not found")
    return job


async def enable_job(name: str) -> RegisteredJob:
    """Enable a registered job and recompute next_run_at.

    Args:
        name: Job name

    Returns:
        The enabled RegisteredJob

    Raises:
        RegisteredJobNotFound: If no job with this name exists
    """
    now = utc_now()

    async with get_sql_session() as session:
        job = await _get_registered_or_raise(session, name)
        job.enabled = True
        job.updated_at = now
        job.next_run_at = _next_run_at(job.schedule, True, now)
        session.add(job)
        await session.commit()
        await session.refresh(job)
        return job


async def disable_job(name: str) -> RegisteredJob:
    """Disable a registered job and clear next_run_at.

    Args:
        name: Job name

    Returns:
        The disabled RegisteredJob

    Raises:
        RegisteredJobNotFound: If no job with this name exists
    """
    async with get_sql_session() as session:
        job = await _get_registered_or_raise(session, name)
        job.enabled = False
        job.next_run_at = None
        job.updated_at = utc_now()
        session.add(job)
        await session.commit()
        await session.refresh(job)
        return job


async def list_registered_jobs(
    *,
    enabled_only: bool = False,
) -> list[RegisteredJob]:
    """List registered jobs.

    Args:
        enabled_only: If True, only return enabled jobs

    Returns:
        List of RegisteredJob entries
    """
    async with get_sql_session() as session:
        query = select(RegisteredJob).order_by(RegisteredJob.name)
        if enabled_only:
            query = query.where(RegisteredJob.enabled == True)  # noqa: E712
        result = await session.execute(query)
        return list(result.scalars().all())


async def run_job(
    name: str,
    entrypoint: str,
    *,
    kwargs: dict[str, Any] | None = None,
    run_type: RunType = RUN_MANUAL,
    preservation_mode: PreservationMode | None = None,
    entry_type: EntryType = ENTRY_MODULE,
    command: list[str] | None = None,
    command_env: dict[str, str] | None = None,
    image: str | None = None,
    build: bool = False,
    git_remote: str | None = None,
    git_sha: str | None = None,
    git_branch: str | None = None,
    dockerfile: str | None = None,
    resources: dict[str, Any] | None = None,
) -> Job:
    """Run a job immediately, linking to a registration if one exists.

    Looks up an existing ``RegisteredJob`` by name. If found, merges
    ``kwargs`` over its ``default_kwargs`` and links the new ``Job``
    via ``registered_job_id``. If no registration exists, the job runs
    standalone with ``registered_job_id=None`` — registration is not
    a prerequisite for running.

    The preservation mode resolves via the precedence chain
    (see ``factories.resolve_job_config``):
    explicit arg > registered-job default > env var > hardcoded NONE.

    The image source resolves via ``docker_config.resolve_image_source``
    (run ``image`` > run ``build`` / modifiers > registration ``image`` >
    registration ``build`` > subprocess) and is stamped onto the entry task
    (``tasks.image_source``). A build task is auto-injected as a
    ``build >> entry`` dependency for git-build sources (see
    ``image_injection.inject_build_tasks``). Which runner launches the
    container is the worker's ``AAICLICK_RUNNER``, not a job property.

    Args:
        name: Job name
        entrypoint: Python dotted path
        kwargs: Override parameters (merged over default_kwargs)
        run_type: How the job was triggered (default: MANUAL)
        preservation_mode: Level-1 override for the registered job's
            baseline. Pass ``None`` to inherit.
        entry_type: ``"module"`` (default) runs ``entrypoint`` as a dotted
            path; ``"shell"`` runs ``command`` directly in the runner's
            environment; ``"jvm"`` resolves ``entrypoint`` as a Java class
            name via the ``aaiclick-task-api`` shim inside the container
            image. Shell tasks work everywhere; jvm tasks require an image
            source.
        command: Argv list for shell tasks (required when
            ``entry_type="shell"``, rejected for ``"module"``).
        command_env: Env vars (``KEY: VALUE``) injected for shell tasks.
        image: Prebuilt image tag to run verbatim. Mutually exclusive with
            ``build`` and the ``git_*``/``dockerfile`` modifiers.
        build: Build the task image from the repo at the submitted commit.
            Any ``git_*`` / ``dockerfile`` modifier implies it.
        git_remote: Override the registered job's default git remote.
        git_sha: Pin the build to a specific commit SHA. ``None`` resolves
            the head of ``git_branch`` (or the default branch) on the
            effective remote via ``git ls-remote``; only when no remote is
            known either does it fall back to the working tree's HEAD
            (must be clean and pushed).
        git_branch: Captured as build-arg metadata; ``None`` means the
            remote's default branch (or the local branch when the remote
            is auto-detected).
        dockerfile: Override the registered job's dockerfile path.
        resources: Kubernetes requests/limits for this run's Pods; ``None``
            inherits the registration default. Rejected on a subprocess run.

    Returns:
        Created Job
    """
    validate_task_entry(entry_type=entry_type, command=command)
    validate_image_exclusivity(image, git_remote, git_sha, git_branch, dockerfile, build=build)

    registered = await get_registered_job(name)

    default_kwargs = registered.default_kwargs if registered is not None else None
    merged_kwargs = {**(default_kwargs or {}), **(kwargs or {})}

    kind = requested_image_kind(
        registered,
        image=image,
        build=build,
        git_remote=git_remote,
        git_sha=git_sha,
        git_branch=git_branch,
        dockerfile=dockerfile,
    )
    if kind is None:
        if resources is not None:
            raise ValueError(_RESOURCES_REQUIRE_IMAGE)
        if entry_type == ENTRY_JVM:
            raise ValueError(
                "jvm entry_type requires an image source (build or image) — the shim jar "
                "runs only inside the task's container image (spec: docs/designs/java-sdk.md)"
            )
        task = create_task(
            entrypoint or None,
            merged_kwargs,
            name=name,
            entry_type=entry_type,
            command=command,
            command_env=command_env,
        )
        return await create_job(
            name=name,
            entry=task,
            run_type=run_type,
            registered_job_id=registered.id if registered is not None else None,
            preservation_mode=preservation_mode,
            registered=registered,
        )

    # Gate on the pure decision before resolving: git auto-detect must not
    # mask the real problem when the local backend cannot run containers.
    if is_local():
        raise ValueError(
            "container jobs require distributed mode (Postgres + ClickHouse); "
            "got chdb + SQLite. Set AAICLICK_SQL_URL and AAICLICK_CH_URL to "
            "remote services before submitting these jobs."
        )
    source = await resolve_image_source(
        registered,
        image=image,
        build=build,
        git_remote=git_remote,
        git_sha=git_sha,
        git_branch=git_branch,
        dockerfile=dockerfile,
    )
    assert source is not None  # requested_image_kind said so
    return await create_container_job(
        name=name,
        entrypoint=entrypoint,
        image_source=source,
        resources=resources,
        entry_type=entry_type,
        command=command,
        command_env=command_env,
        kwargs=merged_kwargs,
        run_type=run_type,
        registered_job_id=registered.id if registered is not None else None,
        preservation_mode=preservation_mode,
        registered=registered,
    )
