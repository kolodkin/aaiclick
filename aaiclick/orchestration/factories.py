"""Factory functions for creating orchestration objects."""

import sys
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from aaiclick.snowflake import get_snowflake_id

from .models import Job, JobStatus, Preserve, RegisteredJob, RunType, Task, TaskStatus
from .orch_context import get_sql_session
from .task_registry import get_task_registry


class _Unset:
    """Sentinel type for ``preserve=`` kwargs that distinguish "not supplied"
    from explicit ``None``. Use the module-level :data:`_UNSET` singleton; the
    class is exposed only so callers can type their forwarding kwargs."""


_UNSET = _Unset()


def resolve_preserve(
    explicit: Preserve | _Unset = _UNSET,
    registered: Preserve = None,
) -> Preserve:
    """Resolve effective ``preserve`` value with precedence: explicit > registered > None.

    The sentinel honors ``explicit=[]`` ("preserve nothing") instead of falling
    through to the registered default.
    """
    if isinstance(explicit, _Unset) or explicit is None:
        chosen: Preserve = registered
    elif explicit == "*" or isinstance(explicit, list):
        if isinstance(explicit, list) and not all(isinstance(x, str) for x in explicit):
            raise TypeError("preserve list must contain only str")
        chosen = explicit
    else:
        raise TypeError(f"preserve must be None, '*', or list[str]; got {type(explicit).__name__}")

    if chosen is None:
        return None
    if chosen == "*":
        return "*"
    return list(chosen)


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
    callback: str | Callable, kwargs: dict | None = None, *, name: str | None = None, max_retries: int = 0
) -> Task:
    """Create a Task object (not committed to database).

    Args:
        callback: Either a callback string (e.g., "mymodule.task1") or a callable function
        kwargs: Keyword arguments for the task function (default: empty dict)
        name: Human-readable name (default: function name from entrypoint)
        max_retries: Maximum number of retries on failure (default: 0, no retry)

    Returns:
        Task object with generated snowflake ID

    Example:
        # Using string
        task = create_task("mymodule.task1", {"param": "value"})

        # Using callable with retries
        task = create_task(my_function, {"param": "value"}, max_retries=3)

        # Using custom name
        task = create_task("mymodule.task1", name="my_task")
    """
    task_id = get_snowflake_id()

    # Convert callable to string if needed
    if callable(callback):
        entrypoint = _callable_to_string(callback)
        resolved_name = name or getattr(callback, "__name__", entrypoint.rsplit(".", 1)[-1])
    else:
        entrypoint = callback
        resolved_name = name or entrypoint.rsplit(".", 1)[-1]

    task = Task(
        id=task_id,
        entrypoint=entrypoint,
        name=resolved_name,
        kwargs=kwargs or {},
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow(),
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
    run_type: RunType = RunType.MANUAL,
    registered_job_id: int | None = None,
    preserve: Preserve | _Unset = _UNSET,
    registered: RegisteredJob | None = None,
) -> Job:
    """Create a Job and commit it to the database.

    Args:
        name: Job name
        entry: Callback string, callable function, or Task object
        run_type: How the job was triggered (MANUAL or SCHEDULED)
        registered_job_id: FK to registered_jobs (optional)
        preserve: Names of tables that survive past the run, the literal
            ``"*"`` (preserve every ``j_<id>_*`` table), ``[]`` (explicitly
            preserve nothing), or ``None`` (inherit the registered default).
        registered: Optional ``RegisteredJob`` to source defaults from.

    Returns:
        Job object with id populated after database commit
    """
    registered_preserve = registered.preserve if registered is not None else None
    resolved_preserve = resolve_preserve(explicit=preserve, registered=registered_preserve)

    job_id = get_snowflake_id()
    job = Job(
        id=job_id,
        name=name,
        status=JobStatus.PENDING,
        run_type=run_type,
        registered_job_id=registered_job_id,
        preserve=resolved_preserve,
        created_at=datetime.utcnow(),
    )

    # Create task from entry if it's not already a Task
    if isinstance(entry, Task):
        task = entry
    else:
        task = create_task(entry)

    # Set task's job_id
    task.job_id = job_id

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
