"""Factory functions for creating orchestration objects."""

import sys
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional, Union

from aaiclick.oplog.sampling import SamplingStrategy
from aaiclick.snowflake_id import get_snowflake_id

from .env import get_default_preservation_mode
from .orch_context import get_sql_session
from .models import Job, JobStatus, PreservationMode, RunType, Task, TaskStatus
from .task_registry import get_task_registry


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


def create_task(callback: Union[str, Callable], kwargs: dict = None, *, name: str = None, max_retries: int = 0) -> Task:
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
    entry: Union[str, Callable, Task],
    *,
    run_type: RunType = RunType.MANUAL,
    registered_job_id: int | None = None,
    preservation_mode: Optional[PreservationMode] = None,
    sampling_strategy: Optional[SamplingStrategy] = None,
) -> Job:
    """Create a Job and commit it to the database.

    Args:
        name: Job name
        entry: Callback string, callable function, or Task object
        run_type: How the job was triggered (MANUAL or SCHEDULED)
        registered_job_id: FK to registered_jobs (optional)
        preservation_mode: Which tables survive after the job completes.
            Defaults to the value of ``AAICLICK_DEFAULT_PRESERVATION_MODE``
            or ``PreservationMode.NONE``.
        sampling_strategy: Per-table WHERE clauses that tell the oplog which
            rows to track. Required when ``preservation_mode`` is
            ``STRATEGY``; rejected in every other mode.

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
    resolved_mode = preservation_mode or get_default_preservation_mode()
    if resolved_mode is PreservationMode.STRATEGY:
        if not sampling_strategy:
            raise ValueError(
                "preservation_mode=STRATEGY requires a non-empty sampling_strategy"
            )
    else:
        if sampling_strategy:
            raise ValueError(
                f"sampling_strategy is only valid with preservation_mode=STRATEGY "
                f"(got preservation_mode={resolved_mode.value})"
            )

    # Generate job ID
    job_id = get_snowflake_id()

    # Create Job object
    job = Job(
        id=job_id,
        name=name,
        status=JobStatus.PENDING,
        run_type=run_type,
        registered_job_id=registered_job_id,
        preservation_mode=resolved_mode,
        sampling_strategy=sampling_strategy if sampling_strategy else None,
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
