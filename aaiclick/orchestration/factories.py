"""Factory functions for creating orchestration objects."""

import sys
from datetime import datetime
from pathlib import Path
from typing import Callable, Union

from aaiclick.snowflake_id import get_snowflake_id

from .context import get_orch_context_session
from .models import Job, JobStatus, Task, TaskStatus


def _resolve_main_module(func: Callable) -> str:
    """Resolve the actual module path for a function defined in __main__.

    When a script is run directly, its __module__ is '__main__', but we need
    the actual importable module path for the worker to import it.

    Args:
        func: A callable function

    Returns:
        The resolved module path (e.g., 'aaiclick.example_projects.basic_worker_register')
    """
    # Get the file where the function is defined
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


def create_task(callback: Union[str, Callable], kwargs: dict = None) -> Task:
    """Create a Task object (not committed to database).

    Args:
        callback: Either a callback string (e.g., "mymodule.task1") or a callable function
        kwargs: Keyword arguments for the task function (default: empty dict)

    Returns:
        Task object with generated snowflake ID

    Example:
        # Using string
        task = create_task("mymodule.task1", {"param": "value"})

        # Using callable
        task = create_task(my_function, {"param": "value"})
    """
    task_id = get_snowflake_id()

    # Convert callable to string if needed
    if callable(callback):
        entrypoint = _callable_to_string(callback)
    else:
        entrypoint = callback

    return Task(
        id=task_id,
        entrypoint=entrypoint,
        kwargs=kwargs or {},
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow(),
    )


async def create_job(name: str, entry: Union[str, Callable, Task]) -> Job:
    """Create a Job and commit it to the database.

    Args:
        name: Job name
        entry: Callback string, callable function, or Task object

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
    # Generate job ID
    job_id = get_snowflake_id()

    # Create Job object
    job = Job(
        id=job_id,
        name=name,
        status=JobStatus.PENDING,
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
    async with get_orch_context_session() as session:
        # Add job and task using ORM
        session.add(job)
        session.add(task)

        # Commit transaction
        await session.commit()

    return job
