"""Factory functions for creating orchestration objects."""

import sys
from collections.abc import Callable
from pathlib import Path

from aaiclick.snowflake import get_snowflake_id

from ..datetime_utils import utc_now
from .env import get_default_preservation_mode
from .models import (
    JOB_PENDING,
    RUN_MANUAL,
    RUNNER_SUBPROCESS,
    TASK_PENDING,
    Job,
    PreservationMode,
    RegisteredJob,
    RunnerMode,
    RunType,
    Task,
)
from .image_injection import inject_build_tasks
from .orch_context import get_sql_session
from .runner_config import (
    ENTRY_MODULE,
    ENTRY_SHELL,
    DockerRunner,
    EntryType,
    ImageBuild,
    ImagePrebuilt,
    ImageSourceT,
    KubernetesRunner,
    dump_image_source,
    dump_runner_config,
)
from .task_registry import get_task_registry


def resolve_job_config(
    explicit_mode: PreservationMode | None,
    registered: RegisteredJob | None = None,
) -> PreservationMode:
    """Resolve ``preservation_mode`` for a job run.

    Precedence (highest first):

    1. Explicit ``explicit_mode`` argument
    2. ``registered.preservation_mode``
    3. ``AAICLICK_DEFAULT_PRESERVATION_MODE`` env var
    4. ``"NONE"`` (hardcoded fallback)

    The explicit override is considered "set" when it's not ``None`` —
    this lets callers pass ``None`` to mean "inherit from the next level".
    """
    mode = explicit_mode
    if mode is None and registered is not None:
        mode = registered.preservation_mode
    if mode is None:
        mode = get_default_preservation_mode()

    return mode


def new_job_row(
    name: str,
    *,
    run_type: RunType,
    registered_job_id: int | None = None,
    preservation_mode: PreservationMode | None = None,
    registered: RegisteredJob | None = None,
    runner_mode: RunnerMode = RUNNER_SUBPROCESS,
    runner: dict | None = None,
) -> Job:
    """Build an uncommitted PENDING Job row with a resolved preservation mode."""
    return Job(
        id=get_snowflake_id(),
        name=name,
        status=JOB_PENDING,
        run_type=run_type,
        registered_job_id=registered_job_id,
        preservation_mode=resolve_job_config(preservation_mode, registered),
        runner_mode=runner_mode,
        runner=runner,
        created_at=utc_now(),
    )


def _resolve_main_module(func: Callable) -> str:
    """Resolve the actual module path for a function defined in __main__.

    When a script is run directly, its __module__ is '__main__', but we need
    the actual importable module path for the worker to import it.

    Uses two strategies:
    1. Check __spec__ (works when run with `python -m module`)
    2. Fall back to file-based resolution from sys.path

    Args:
        func: A callable function

    Returns:
        The resolved module path (e.g., 'basic_worker')
    """
    # Strategy 1: Try __spec__ (cleanest when available, e.g., python -m)
    main_spec = getattr(sys.modules.get("__main__"), "__spec__", None)
    if main_spec and main_spec.name:
        return main_spec.name

    # Strategy 2: Resolve from file path and sys.path
    code = getattr(func, "__code__", None)
    if code is None:
        return "__main__"

    filepath = Path(code.co_filename).resolve()

    # Collect all possible module paths from sys.path
    candidates = []
    for path in sys.path:
        try:
            path = Path(path).resolve()
            if filepath.is_relative_to(path):
                relative = filepath.relative_to(path)
                # Convert path to module (remove .py, replace / with .)
                parts = list(relative.parts)
                if parts[-1].endswith(".py"):
                    parts[-1] = parts[-1][:-3]
                candidates.append(".".join(parts))
        except (ValueError, TypeError):
            continue

    if not candidates:
        return "__main__"

    # Prefer the longest module path (most specific, from project root)
    return max(candidates, key=len)


def _callable_to_string(func: Callable) -> str:
    """Convert a callable to its module.function string representation.

    Args:
        func: A callable function

    Returns:
        String in format "module.function_name"

    Note:
        For functions defined in __main__, attempts to resolve the actual
        module path so the function can be imported by workers.
    """
    module = getattr(func, "__module__", "__main__")

    # Resolve __main__ to actual module path
    if module == "__main__":
        module = _resolve_main_module(func)

    name = getattr(func, "__qualname__", func.__name__)
    return f"{module}.{name}"


def create_task(
    callback: str | Callable | None = None,
    kwargs: dict | None = None,
    *,
    name: str | None = None,
    max_retries: int = 0,
    entry_type: EntryType = ENTRY_MODULE,
    command: list[str] | None = None,
    command_env: dict[str, str] | None = None,
    image: str | None = None,
    git_remote: str | None = None,
    git_sha: str | None = None,
    git_branch: str | None = None,
    dockerfile: str | None = None,
) -> Task:
    """Create a Task object (not committed to database).

    For ``entry_type="module"`` (default), ``callback`` is a dotted path or
    callable run via ``execute_task``. For ``entry_type="shell"``, ``command``
    is an argv run directly in the runner's environment and ``callback`` is
    unused (``entrypoint`` is stored as an empty string).

    A task on a docker/kubernetes job may declare its own container image;
    tasks that declare none inherit the committing task's image at
    ``commit_tasks`` (dynamic children follow their parent).

    Args:
        callback: Callback string ("mymodule.task1") or callable, for module tasks.
        kwargs: Keyword arguments for the task function (default: empty dict).
        name: Human-readable name (default: derived from entrypoint/command).
        max_retries: Maximum number of retries on failure (default: 0).
        entry_type: ``"module"`` (default) or ``"shell"``.
        command: Argv list for shell tasks.
        command_env: Env vars (``KEY: VALUE``) injected for shell tasks.
        image: Prebuilt image tag to run this task in, verbatim
            (e.g. ``"ghcr.io/org/app:1.2"``). Mutually exclusive with the
            ``git_*``/``dockerfile`` build fields.
        git_remote: Git remote URL to build this task's image from.
            Required together with ``git_sha`` for a build declaration —
            ``create_task`` is synchronous, so there is no git auto-detect
            (that only exists at ``run_job`` submission).
        git_sha: 40-char commit SHA to build at.
        git_branch: Captured as build-arg metadata (optional).
        dockerfile: Dockerfile path relative to the repo root (optional;
            defaults to ``"Dockerfile"`` at build time).

    Returns:
        Task object with generated snowflake ID.
    """
    image_source: dict | None = None
    git_fields = (git_remote, git_sha, git_branch, dockerfile)
    if image is not None and any(v is not None for v in git_fields):
        raise ValueError("image (prebuilt) and git_* (build) are mutually exclusive")
    if image is not None:
        image_source = dump_image_source(ImagePrebuilt(image_tag=image))
    elif any(v is not None for v in git_fields):
        if git_remote is None or git_sha is None:
            raise ValueError(
                "a per-task build image requires both git_remote and git_sha "
                "(create_task has no git auto-detect; that only exists at run_job submission)"
            )
        image_source = dump_image_source(
            ImageBuild(git_remote=git_remote, git_sha=git_sha, git_branch=git_branch, dockerfile=dockerfile)
        )

    task_id = get_snowflake_id()

    if entry_type == ENTRY_SHELL:
        entrypoint = ""
        resolved_name = name or (command[0] if command else "shell")
    elif callable(callback):
        entrypoint = _callable_to_string(callback)
        resolved_name = name or getattr(callback, "__name__", entrypoint.rsplit(".", 1)[-1])
    else:
        entrypoint = callback or ""
        resolved_name = name or entrypoint.rsplit(".", 1)[-1]

    task = Task(
        id=task_id,
        entrypoint=entrypoint,
        name=resolved_name,
        kwargs=kwargs or {},
        entry_type=entry_type,
        command=command,
        command_env=command_env,
        image_source=image_source,
        status=TASK_PENDING,
        created_at=utc_now(),
        max_retries=max_retries,
    )
    registry = get_task_registry()
    if registry is not None:
        registry[task_id] = task
    return task


async def create_job(
    name: str,
    entry: str | Callable | Task,
    *,
    run_type: RunType = RUN_MANUAL,
    registered_job_id: int | None = None,
    preservation_mode: PreservationMode | None = None,
    registered: RegisteredJob | None = None,
) -> Job:
    """Create a Job and commit it to the database.

    Args:
        name: Job name
        entry: Callback string, callable function, or Task object
        run_type: How the job was triggered (MANUAL or SCHEDULED)
        registered_job_id: FK to registered_jobs (optional)
        preservation_mode: Which tables survive after the job completes.
            Overrides the registered job's default; falls through to the
            ``AAICLICK_DEFAULT_PRESERVATION_MODE`` env var, then
            ``"NONE"``.
        registered: Optional ``RegisteredJob`` to source level-2 defaults
            from. When supplied, ``registered.preservation_mode`` becomes
            the fallback value.

    Returns:
        Job object with id populated after database commit

    Example:
        # Using callback string
        job = await create_job("my_job", "mymodule.task1")

        # Using callable function
        job = await create_job("my_job", my_function)

        # Using Task object
        task = create_task("mymodule.task1", {"param": "value"})
        job = await create_job("my_job", task)
    """
    job = new_job_row(
        name,
        run_type=run_type,
        registered_job_id=registered_job_id,
        preservation_mode=preservation_mode,
        registered=registered,
    )

    # Create task from entry if it's not already a Task
    if isinstance(entry, Task):
        task = entry
    else:
        task = create_task(entry)

    # Set task's job_id
    task.job_id = job.id

    # Commit to database using OrchContext session
    async with get_sql_session() as session:
        # Add job and task using ORM
        session.add(job)
        session.add(task)

        # Commit transaction
        await session.commit()

    # Remove the entry task from the registry after commit so that subsequent
    # registry lookups for the same task ID don't return the now-detached object.
    registry = get_task_registry()
    if registry is not None:
        registry.pop(task.id, None)

    return job


async def create_built_job(
    *,
    name: str,
    entrypoint: str,
    runner: DockerRunner | KubernetesRunner,
    image_source: ImageSourceT,
    entry_type: EntryType = ENTRY_MODULE,
    command: list[str] | None = None,
    command_env: dict[str, str] | None = None,
    kwargs: dict | None = None,
    run_type: RunType = RUN_MANUAL,
    registered_job_id: int | None = None,
    preservation_mode: PreservationMode | None = None,
    registered: RegisteredJob | None = None,
) -> Job:
    """Create a docker/kubernetes Job. ``runner`` carries only cluster/vehicle
    config; ``image_source`` is stamped onto the entry task, and in registry
    mode a build task is injected with a ``build >> entry`` edge (spec:
    docs/designs/orchestration.md "Image source")."""
    job = new_job_row(
        name,
        run_type=run_type,
        registered_job_id=registered_job_id,
        preservation_mode=preservation_mode,
        registered=registered,
        runner_mode=runner.type,
        runner=dump_runner_config(runner),
    )

    entry_task = create_task(
        entrypoint or None,
        kwargs or {},
        name=name,
        entry_type=entry_type,
        command=command,
        command_env=command_env,
    )
    entry_task.job_id = job.id
    entry_task.image_source = dump_image_source(image_source)

    async with get_sql_session() as session:
        injected = await inject_build_tasks(session, [entry_task], job)
        to_add = [job, *injected, entry_task]
        for obj in to_add:
            session.add(obj)
        await session.commit()

    registry = get_task_registry()
    if registry is not None:
        for obj in to_add[1:]:
            registry.pop(obj.id, None)
    return job
