"""Decorators for defining tasks and jobs in aaiclick orchestration.

Provides Airflow-style TaskFlow API where:
- @task decorates functions to create TaskFactory instances
- Calling a TaskFactory with Task arguments creates dependencies automatically
- @job decorates functions that define workflow DAGs

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
        transformed = transform(extracted)  # Auto-dependency: extract >> transform
        return [extracted, transformed]

    job = await pipeline(url="https://example.com/data.parquet")
"""

from __future__ import annotations

from datetime import datetime
from functools import wraps
from typing import Any, Callable, List

from aaiclick.data.object import Object, View

from ..snowflake_id import get_snowflake_id
from .context import _current_orch_context, OrchContext, get_orch_context
from .factories import _callable_to_string
from .models import Group, Job, JobStatus, Task, TaskStatus


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
        return {"ref_type": "upstream", "task_id": value.id}
    elif isinstance(value, View):
        return {
            "object_type": "view",
            "table": value.table,
            "where": value.where,
            "limit": value.limit,
            "offset": value.offset,
            "order_by": value.order_by,
            "selected_fields": value.selected_fields,
        }
    elif isinstance(value, Object):
        return {"object_type": "object", "table": value.table}
    elif isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]
    elif isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    else:
        # Native Python types: str, int, float, bool, None
        return value


class TaskFactory:
    """Factory that creates Task instances when called.

    Wraps a function and creates Tasks with proper serialization
    and automatic dependency detection when Task arguments are passed.
    """

    def __init__(self, func: Callable):
        """Initialize TaskFactory.

        Args:
            func: The function to wrap
        """
        self.func = func
        self.entrypoint = _callable_to_string(func)
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

        task = Task(
            id=get_snowflake_id(),
            entrypoint=self.entrypoint,
            kwargs=serialized_kwargs,
            status=TaskStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        # Set up dependencies: upstream >> task
        for upstream in upstream_tasks:
            upstream >> task

        return task

    def __repr__(self) -> str:
        return f"TaskFactory({self.entrypoint})"


def task(func: Callable) -> TaskFactory:
    """Decorator to create a TaskFactory from a function.

    The decorated function can be called to create Task instances.
    When Task objects are passed as arguments, dependencies are
    automatically created and results are injected at runtime.

    Example:
        @task
        async def my_task(data: Object) -> Object:
            return await (data * 2)

        # Creates a Task with dependency on upstream_task
        t = my_task(data=upstream_task)

    Args:
        func: Async or sync function to wrap

    Returns:
        TaskFactory: Factory that creates Tasks when called
    """
    return TaskFactory(func)


class JobFactory:
    """Factory that creates and applies Jobs when called.

    Wraps a workflow definition function and handles:
    - Database context management (creates OrchContext internally if needed)
    - Job creation
    - Task collection from function return value
    - Applying all tasks to the database
    """

    def __init__(self, name: str, func: Callable):
        """Initialize JobFactory.

        Args:
            name: Job name
            func: Workflow definition function that returns tasks
        """
        self.name = name
        self.func = func
        wraps(func)(self)

    async def __call__(self, **kwargs) -> Job:
        """Execute workflow definition and create job with tasks.

        Manages database context automatically — no need to wrap
        in OrchContext externally.

        Args:
            **kwargs: Arguments passed to the workflow function

        Returns:
            Job: Created job with all tasks applied
        """
        # Check if we're already in an OrchContext
        try:
            _current_orch_context.get()
            # Already in context, just run
            return await self._create_job(**kwargs)
        except LookupError:
            # Not in context, create one
            async with OrchContext():
                return await self._create_job(**kwargs)

    async def _create_job(self, **kwargs) -> Job:
        """Internal method to create job within an OrchContext."""
        # Call workflow function to get tasks
        result = self.func(**kwargs)

        # Normalize result to list
        if isinstance(result, (Task, Group)):
            tasks = [result]
        elif isinstance(result, (list, tuple)):
            tasks = list(result)
        else:
            raise ValueError(
                f"Job function must return Task, Group, or list of them. "
                f"Got {type(result).__name__}"
            )

        job = Job(
            id=get_snowflake_id(),
            name=self.name,
            status=JobStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        # Get context and apply
        ctx = get_orch_context()

        async with ctx.get_session() as session:
            session.add(job)
            await session.commit()

        # Apply tasks with job_id
        await ctx.apply(tasks, job.id)

        return job

    def __repr__(self) -> str:
        return f"JobFactory({self.name!r})"


def job(name: str) -> Callable[[Callable], JobFactory]:
    """Decorator to create a JobFactory from a workflow function.

    The decorated function should return a list of Tasks (and Groups)
    that define the workflow DAG. When called, it creates a Job and
    applies all tasks to the database.

    Example:
        @job("my_pipeline")
        def my_workflow(input_url: str):
            t1 = extract(url=input_url)
            t2 = transform(data=t1)
            t1 >> t2
            return [t1, t2]

        job = await my_workflow(input_url="https://...")

    Args:
        name: Name for the created jobs

    Returns:
        Decorator function that creates JobFactory
    """

    def decorator(func: Callable) -> JobFactory:
        return JobFactory(name, func)

    return decorator
