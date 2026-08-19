"""CRUD operations for registered jobs."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from croniter import croniter
from sqlmodel import select

from ..backend import is_local
from ..datetime_utils import utc_now
from ..snowflake import get_snowflake_id
from .docker_config import resolve_image_source, resolve_runner_config
from .factories import create_built_job, create_job, create_task
from .kubernetes_config import resolve_kubernetes_config
from .models import (
    RUN_MANUAL,
    RUNNER_DOCKER,
    RUNNER_KUBERNETES,
    RUNNER_SUBPROCESS,
    Job,
    PreservationMode,
    RegisteredJob,
    RunnerMode,
    RunType,
)
from .orch_context import get_sql_session
from .runner_config import ENTRY_MODULE, EntryType, validate_image_exclusivity, validate_task_entry


class RegisteredJobAlreadyExists(ValueError):
    """Raised when registering a name that already exists."""


class RegisteredJobNotFound(ValueError):
    """Raised when enabling/disabling a non-existent registration."""


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


def _build_registered_job(
    *,
    name: str,
    entrypoint: str,
    schedule: str | None,
    default_kwargs: dict[str, Any] | None,
    enabled: bool,
    preservation_mode: PreservationMode | None,
    runner_mode: RunnerMode,
    dockerfile: str | None,
    git_remote: str | None,
    image: str | None,
    kubernetes_config: dict[str, Any] | None,
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
        runner_mode=runner_mode,
        dockerfile=dockerfile,
        git_remote=git_remote,
        image=image,
        kubernetes_config=kubernetes_config,
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
    runner_mode: RunnerMode = RUNNER_SUBPROCESS,
    dockerfile: str | None = None,
    git_remote: str | None = None,
    kubernetes_config: dict[str, Any] | None = None,
    image: str | None = None,
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
        runner_mode: ``"subprocess"`` (default) or ``"docker"``.
        dockerfile: Default Dockerfile path relative to the repo root.
            ``None`` falls back to ``"Dockerfile"`` at submission time.
        git_remote: Default git remote URL. ``None`` falls back to
            ``git config remote.origin.url`` at submission time.
        image: Default prebuilt image tag. When set, a prebuilt runner
            marker is stored so runs default to this image instead of a
            git build.

    Returns:
        Created RegisteredJob

    Raises:
        RegisteredJobAlreadyExists: If a job with this name already exists.
    """
    now = utc_now()
    registered_job = _build_registered_job(
        name=name,
        entrypoint=entrypoint,
        schedule=schedule,
        default_kwargs=default_kwargs,
        enabled=enabled,
        preservation_mode=preservation_mode,
        runner_mode=runner_mode,
        dockerfile=dockerfile,
        git_remote=git_remote,
        image=image,
        kubernetes_config=kubernetes_config,
        now=now,
    )

    async with get_sql_session() as session:
        existing = await session.execute(select(RegisteredJob).where(RegisteredJob.name == name))
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
        result = await session.execute(select(RegisteredJob).where(RegisteredJob.name == name))
        return result.scalar_one_or_none()


async def upsert_registered_job(
    *,
    name: str,
    entrypoint: str,
    schedule: str | None = None,
    default_kwargs: dict[str, Any] | None = None,
    enabled: bool = True,
    preservation_mode: PreservationMode | None = None,
    runner_mode: RunnerMode = RUNNER_SUBPROCESS,
    dockerfile: str | None = None,
    git_remote: str | None = None,
    kubernetes_config: dict[str, Any] | None = None,
    image: str | None = None,
) -> RegisteredJob:
    """Insert or update a registered job.

    If a job with the given name exists, updates entrypoint, schedule,
    default_kwargs, preservation_mode, runner_mode and the Docker
    defaults. Otherwise creates a new entry.

    Args:
        name: Unique job name
        entrypoint: Python dotted path
        schedule: Cron expression (optional)
        default_kwargs: Default parameters (optional)
        enabled: Whether the job is enabled
        preservation_mode: Default preservation mode for every run
        runner_mode: ``"subprocess"`` (default) or ``"docker"``.
        dockerfile: Default Dockerfile path relative to the repo root.
        git_remote: Default git remote URL.
        image: Default prebuilt image tag. When set, a prebuilt runner
            marker is stored so runs default to this image instead of a
            git build.

    Returns:
        The created or updated RegisteredJob
    """
    now = utc_now()

    async with get_sql_session() as session:
        result = await session.execute(select(RegisteredJob).where(RegisteredJob.name == name))
        existing = result.scalar_one_or_none()

        if existing is not None:
            existing.entrypoint = entrypoint
            existing.schedule = schedule
            existing.default_kwargs = default_kwargs
            existing.preservation_mode = preservation_mode
            existing.enabled = enabled
            existing.runner_mode = runner_mode
            existing.dockerfile = dockerfile
            existing.git_remote = git_remote
            existing.image = image
            existing.kubernetes_config = kubernetes_config
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
            runner_mode=runner_mode,
            dockerfile=dockerfile,
            git_remote=git_remote,
            image=image,
            kubernetes_config=kubernetes_config,
            now=now,
        )
        session.add(registered_job)
        await session.commit()
        await session.refresh(registered_job)
        return registered_job


async def _get_registered_or_raise(session, name: str) -> RegisteredJob:
    """Fetch a registration by name or raise RegisteredJobNotFound."""
    result = await session.execute(select(RegisteredJob).where(RegisteredJob.name == name))
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
    git_remote: str | None = None,
    git_sha: str | None = None,
    git_branch: str | None = None,
    dockerfile: str | None = None,
    namespace: str | None = None,
    service_account: str | None = None,
    image_pull_secret: str | None = None,
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

    For docker/kubernetes registrations, the image source resolves via the
    precedence chain (see ``docker_config.resolve_image_source``):
    explicit kwarg > registered-job default > git auto-detect — and is
    stamped onto the entry task (``tasks.image_source``). In registry mode
    a build task is auto-injected as a ``build >> entry`` dependency for
    git-build sources (see ``image_injection.inject_build_tasks``).

    Args:
        name: Job name
        entrypoint: Python dotted path
        kwargs: Override parameters (merged over default_kwargs)
        run_type: How the job was triggered (default: MANUAL)
        preservation_mode: Level-1 override for the registered job's
            baseline. Pass ``None`` to inherit.
        entry_type: ``"module"`` (default) runs ``entrypoint`` as a dotted
            path; ``"shell"`` runs ``command`` directly in the runner's
            environment. Shell tasks work on every runner.
        command: Argv list for shell tasks (required when
            ``entry_type="shell"``, rejected for ``"module"``).
        command_env: Env vars (``KEY: VALUE``) injected for shell tasks.
        image: Prebuilt image tag to run verbatim. Mutually exclusive with
            the ``git_*``/``dockerfile`` build fields.
        git_remote: Override the registered job's default git remote.
        git_sha: Pin the build to a specific commit SHA. ``None`` means
            auto-detect from the working tree (must be clean and pushed).
        git_branch: Captured as build-arg metadata; ``None`` means
            auto-detect.
        dockerfile: Override the registered job's dockerfile path.
        namespace: Override the kubernetes namespace for this run.
        service_account: Override the kubernetes service account for this run.
        image_pull_secret: Override the kubernetes imagePullSecret for this run.
            The three kubernetes overrides are ignored unless the registered
            job is in kubernetes mode; each falls through to the RegisteredJob
            default, then the ``AAICLICK_K8S_*`` env layer (see
            ``kubernetes_config.resolve_kubernetes_config``).

    Returns:
        Created Job
    """
    validate_task_entry(entry_type=entry_type, command=command)
    validate_image_exclusivity(image, git_remote, git_sha, git_branch, dockerfile)

    registered = await get_registered_job(name)

    default_kwargs = registered.default_kwargs if registered is not None else None
    merged_kwargs = {**(default_kwargs or {}), **(kwargs or {})}

    runner_mode = registered.runner_mode if registered is not None else RUNNER_SUBPROCESS

    if runner_mode in (RUNNER_DOCKER, RUNNER_KUBERNETES):
        if is_local():
            raise ValueError(
                f"{runner_mode} runner requires distributed mode (Postgres + ClickHouse); "
                "got chdb + SQLite. Set AAICLICK_SQL_URL and AAICLICK_CH_URL to "
                "remote services before submitting these jobs."
            )
        kube_cfg = None
        if runner_mode == RUNNER_KUBERNETES:
            kube_cfg = resolve_kubernetes_config(
                registered,
                namespace=namespace,
                service_account=service_account,
                image_pull_secret=image_pull_secret,
            )._asdict()
        source = await resolve_image_source(
            registered,
            image=image,
            git_remote=git_remote,
            git_sha=git_sha,
            git_branch=git_branch,
            dockerfile=dockerfile,
        )
        runner = resolve_runner_config(runner_mode=runner_mode, kubernetes_config=kube_cfg)
        return await create_built_job(
            name=name,
            entrypoint=entrypoint,
            runner=runner,
            image_source=source,
            entry_type=entry_type,
            command=command,
            command_env=command_env,
            kwargs=merged_kwargs,
            run_type=run_type,
            registered_job_id=registered.id if registered is not None else None,
            preservation_mode=preservation_mode,
            registered=registered,
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
