"""Decorators for defining tasks and jobs in aaiclick orchestration.

Provides a dynamic task execution API where:
- @task decorates functions to create TaskFactory instances
- Calling a TaskFactory with Task arguments creates dependencies automatically
- @job marks a function as the entry point task of a job
- Any @task returning Task/Group objects triggers dynamic registration

Example:
    @task
    async def extract(url: str) -> Object:
        return await create_object_from_url(url)

    @task
    async def transform(data: Object) -> Object:
        return await (data * 2)

    @job("my_pipeline")
    def pipeline(url: str):
        extracted = extract(url=url)
        transformed = transform(data=extracted)
        return [extracted, transformed]

    # Creates Job + entry point task "pipeline" in DB
    # Worker executes "pipeline" → returns [Task, Task] → those get registered
    # Worker then executes "extract" → "transform" (respecting dependencies)
    job = await pipeline(url="https://example.com/data.parquet")
"""

from __future__ import annotations

from datetime import datetime
from functools import wraps
from typing import Any, Callable, List, Union

from aaiclick.data.object import Object
from aaiclick.data.object.refs import callable_ref, group_results_ref, upstream_ref
from aaiclick.oplog.sampling import SamplingStrategy

from ..snowflake_id import get_snowflake_id
from .orch_context import commit_tasks, get_sql_session, orch_context
from .sql_context import _sql_engine_var
from .factories import _callable_to_string, resolve_job_config
from .models import Group, Job, JobStatus, PreservationMode, RunType, Task, TaskStatus


def _collect_upstreams(value: Any, upstream_tasks: List[Task]) -> None:
    """Recursively collect Task instances from nested structures."""
    if isinstance(value, Task):
        upstream_tasks.append(value)
    elif isinstance(value, (list, tuple)):
        for v in value:
            _collect_upstreams(v, upstream_tasks)
    elif isinstance(value, dict):
        for v in value.values():
            _collect_upstreams(v, upstream_tasks)



def _serialize_value(value: Any) -> Any:
    """Serialize a value for storage in task kwargs.

    Handles:
    - Task: Creates upstream reference for result injection
    - Object/View: Serializes to reference dict
    - Native Python types (str, int, list, dict, etc.): Passed through

    Args:
        value: Any value to serialize

    Returns:
        Serialized value suitable for JSON storage
    """
    if isinstance(value, Task):
        return upstream_ref(value.id)
    elif isinstance(value, Group):
        return group_results_ref(value.id)
    elif isinstance(value, Object):
        return value._serialize_ref()
    elif isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]
    elif isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    elif callable(value):
        if isinstance(value, TaskFactory):
            return callable_ref(value.entrypoint)
        return callable_ref(_callable_to_string(value))
    else:
        # Native Python types: str, int, float, bool, None
        return value


class TaskFactory:
    """Factory that creates Task instances when called.

    Wraps a function and creates Tasks with proper serialization
    and automatic dependency detection when Task arguments are passed.
    """

    def __init__(self, func: Callable, *, name: str, max_retries: int = 0):
        """Initialize TaskFactory.

        Args:
            func: The function to wrap
            name: Human-readable name for created tasks
            max_retries: Maximum number of retries on failure (default: 0)
        """
        self.func = func
        self.name = name
        self.entrypoint = _callable_to_string(func)
        self.max_retries = max_retries
        # Preserve function metadata
        wraps(func)(self)

    def __call__(self, *args, **kwargs) -> Task:
        """Create a Task instance.

        When Task objects are passed as arguments, automatically:
        1. Creates upstream references for result injection
        2. Sets up dependencies (upstream >> this_task)

        Args:
            *args: Positional arguments (not recommended, use kwargs)
            **kwargs: Keyword arguments for the task function

        Returns:
            Task: New Task instance with dependencies configured
        """
        if args:
            raise ValueError(
                "TaskFactory does not support positional arguments. "
                "Use keyword arguments instead."
            )

        # Collect upstream tasks for dependency creation
        upstream_tasks: List[Task] = []
        for value in kwargs.values():
            _collect_upstreams(value, upstream_tasks)

        # Serialize kwargs
        serialized_kwargs = {k: _serialize_value(v) for k, v in kwargs.items()}

        task_id = get_snowflake_id()
        task = Task(
            id=task_id,
            entrypoint=self.entrypoint,
            name=self.name,
            kwargs=serialized_kwargs,
            status=TaskStatus.PENDING,
            created_at=datetime.utcnow(),
            max_retries=self.max_retries,
        )

        # Set up dependencies: upstream >> task
        for upstream in upstream_tasks:
            upstream >> task

        return task

    def __repr__(self) -> str:
        return f"TaskFactory({self.entrypoint})"


def task(func: Callable = None, *, name: str = None, max_retries: int = 0) -> Union[TaskFactory, Callable]:
    """Decorator to create a TaskFactory from a function.

    Supports both bare and parameterized usage:
        @task
        async def my_task(data: Object) -> Object: ...

        @task(name="custom_name")
        async def my_task(data: Object) -> Object: ...

        @task(max_retries=3)
        async def my_retryable_task(data: Object) -> Object: ...

    Args:
        func: Async or sync function to wrap (when used as bare decorator)
        name: Human-readable name for created tasks (default: function name)
        max_retries: Maximum number of retries on failure (default: 0)

    Returns:
        TaskFactory or decorator function
    """
    if func is not None:
        return TaskFactory(func, name=name or func.__name__, max_retries=max_retries)

    def decorator(f: Callable) -> TaskFactory:
        return TaskFactory(f, name=name or f.__name__, max_retries=max_retries)

    return decorator


class JobFactory:
    """Factory that creates Jobs with an entry point task when called.

    The @job-decorated function becomes the entry point task of the job.
    It runs on a worker, and if it returns Task/Group objects, those get
    dynamically registered to the job.
    """

    def __init__(self, name: str, func: Callable):
        """Initialize JobFactory.

        Args:
            name: Job name
            func: Entry point function that will run as the first task
        """
        self.name = name
        self.func = func
        self.entrypoint = _callable_to_string(func)
        wraps(func)(self)

    async def __call__(
        self,
        *,
        preservation_mode: PreservationMode | None = None,
        sampling_strategy: SamplingStrategy | None = None,
        **kwargs,
    ) -> Job:
        """Create a Job with an entry point task.

        Manages database context automatically — no need to wrap
        in OrchContext externally.

        Args:
            preservation_mode: Override the job's preservation mode. Falls
                through to ``AAICLICK_DEFAULT_PRESERVATION_MODE`` then
                ``PreservationMode.NONE`` when unset.
            sampling_strategy: Per-table WHERE clauses for STRATEGY-mode
                oplog sampling. Required when the resolved mode is
                ``STRATEGY``; rejected otherwise (see
                ``resolve_job_config``).
            **kwargs: Arguments passed to the entry point task.

        Returns:
            Job: Created job with entry point task committed
        """
        # Check if we're already in an orch context
        if _sql_engine_var.get() is not None:
            return await self._create_job(
                preservation_mode=preservation_mode,
                sampling_strategy=sampling_strategy,
                **kwargs,
            )
        # Not in context, create one
        async with orch_context():
            return await self._create_job(
                preservation_mode=preservation_mode,
                sampling_strategy=sampling_strategy,
                **kwargs,
            )

    async def _create_job(
        self,
        run_type: RunType = RunType.MANUAL,
        registered_job_id: int | None = None,
        preservation_mode: PreservationMode | None = None,
        sampling_strategy: SamplingStrategy | None = None,
        **kwargs,
    ) -> Job:
        """Internal method to create job within an OrchContext."""
        # Serialize kwargs for the entry point task
        serialized_kwargs = {k: _serialize_value(v) for k, v in kwargs.items()}

        # Route through resolve_job_config so the @job decorator path honors
        # explicit overrides, the AAICLICK_DEFAULT_PRESERVATION_MODE env var,
        # and any future registered-job defaults instead of silently
        # defaulting to NONE.
        config = resolve_job_config(
            preservation_mode, sampling_strategy, registered=None
        )

        job = Job(
            id=get_snowflake_id(),
            name=self.name,
            status=JobStatus.PENDING,
            run_type=run_type,
            registered_job_id=registered_job_id,
            preservation_mode=config.preservation_mode,
            sampling_strategy=config.sampling_strategy,
            created_at=datetime.utcnow(),
        )

        # Commit job to database
        async with get_sql_session() as session:
            session.add(job)
            await session.commit()

        # Create entry point task
        entry_task = Task(
            id=get_snowflake_id(),
            entrypoint=self.entrypoint,
            name=self.name,
            kwargs=serialized_kwargs,
            status=TaskStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        # Commit entry point task with job_id
        await commit_tasks([entry_task], job.id)

        return job

    def __repr__(self) -> str:
        return f"JobFactory({self.name!r})"


def job(name_or_func: str | Callable | None = None, *, name: str | None = None):
    """Decorator to mark a function as a job's entry point task.

    The decorated function runs on a worker as the first task of the job.
    If it returns Task/Group objects, those are dynamically registered
    to the job with a dependency on the entry point task.

    Supports multiple usage forms:
        @job("my_pipeline")
        def my_workflow(): ...

        @job(name="my_pipeline")
        def my_workflow(): ...

        @job
        def my_workflow(): ...  # name defaults to "my_workflow"

    Args:
        name_or_func: Job name string, or the function itself (bare decorator)
        name: Job name as keyword argument
    """
    if callable(name_or_func):
        return JobFactory(name_or_func.__name__, name_or_func)

    resolved_name = name_or_func or name

    def decorator(func: Callable) -> JobFactory:
        return JobFactory(resolved_name or func.__name__, func)

    return decorator
