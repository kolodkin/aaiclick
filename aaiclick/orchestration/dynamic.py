"""Dynamic task creation operators for orchestration backend.

Provides map() and reduce() operators that dynamically create parallel tasks
based on Object data size at runtime, inspired by Apache Spark's partition-based
parallelism and Python's native map/reduce interfaces.

Usage:
    from aaiclick.orchestration import job, task
    from aaiclick.orchestration.dynamic import map, reduce

    @task
    async def double(partition: Object) -> Object:
        return await (partition * 2)

    @task
    async def combine(results: list) -> Object:
        return await concat(*results)

    @job("parallel_pipeline")
    def pipeline():
        data = load_data()
        mapped = map(double, data, partition_size=1000)
        result = reduce(combine, mapped)
        return [data, mapped, result]
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from math import ceil
from typing import Any, Callable, Union

from sqlmodel import select

from aaiclick.data.data_context import get_ch_client
from aaiclick.snowflake_id import get_snowflake_id

from .context import commit_tasks, get_orch_session
from .decorators import TaskFactory, _serialize_value
from .execution import _deserialize_value, _resolve_upstream_ref, import_callback
from .factories import _callable_to_string
from .handles import MapHandle
from .models import Dependency, Group, Task, TaskStatus
from .worker_context import get_current_task_info


def _get_entrypoint(func: Union[Callable, TaskFactory]) -> str:
    """Get entrypoint string from a callable or TaskFactory."""
    if isinstance(func, TaskFactory):
        return func.entrypoint
    return _callable_to_string(func)


def _name_from_entrypoint(entrypoint: str) -> str:
    """Extract function name from an entrypoint string."""
    return entrypoint.rsplit(".", 1)[-1]


def map(func: Union[Callable, TaskFactory], obj: Union[Any, Task, MapHandle],
        partition_size: int = 1000, **kwargs) -> MapHandle:
    """Create a parallel map operation over partitions of an Object.

    Like Python's map(func, iterable), but partitions the Object into Views
    and creates parallel tasks for each partition at runtime.

    At job definition time, creates an expander task + group. At execution time,
    the expander queries ClickHouse for the Object's row count and creates N
    partition tasks (N = ceil(row_count / partition_size)).

    Args:
        func: @task-decorated function or callable. First parameter receives
              a partition View of the Object.
        obj: Object, Task, or MapHandle to partition. If Task/MapHandle,
             the expander waits for it to complete first.
        partition_size: Number of rows per partition (default 1000).
        **kwargs: Extra keyword arguments passed to each partition task unchanged.

    Returns:
        MapHandle containing the expander task and output group.

    Example:
        mapped = map(double, data, partition_size=500)
        mapped = map(scale, data, partition_size=1000, factor=2)
    """
    func_name = getattr(func, '__name__', str(func))
    target_entrypoint = _get_entrypoint(func)

    # Create group for dynamic partition tasks
    group = Group(id=get_snowflake_id(), name=f"map_{func_name}")

    # Serialize the object reference and extra kwargs
    serialized_obj = _serialize_value(obj)
    serialized_kwargs = {k: _serialize_value(v) for k, v in kwargs.items()}

    # Create expander task
    expander = Task(
        id=get_snowflake_id(),
        entrypoint="aaiclick.orchestration.dynamic._expand_map",
        name=f"_expand_map({func_name})",
        kwargs={
            "target_entrypoint": target_entrypoint,
            "object_ref": serialized_obj,
            "extra_kwargs": serialized_kwargs,
            "partition_size": partition_size,
            "group_id": group.id,
        },
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow(),
    )

    # Wire dependencies: if obj is a Task, expander waits for it
    if isinstance(obj, Task):
        obj >> expander
    elif isinstance(obj, MapHandle):
        obj.group >> expander

    # Also handle Task dependencies in extra kwargs
    for v in kwargs.values():
        if isinstance(v, Task):
            v >> expander
        elif isinstance(v, MapHandle):
            v.group >> expander

    return MapHandle(expander=expander, group=group)


def reduce(func: Union[Callable, TaskFactory], mapped: MapHandle) -> Task:
    """Create a reduce task that collects results from a map operation.

    Like Python's functools.reduce(func, iterable). The reduce task waits for
    all partition tasks in the map group to complete, then passes their results
    as a list to func.

    Args:
        func: @task-decorated function or callable. Receives a single positional
              argument: list of all partition task results (in order).
        mapped: MapHandle from a map() call.

    Returns:
        Task that will execute after all map partitions complete.

    Example:
        result = reduce(combine, mapped)
    """
    target_entrypoint = _get_entrypoint(func)

    func_name = _name_from_entrypoint(target_entrypoint)

    reduce_task = Task(
        id=get_snowflake_id(),
        entrypoint="aaiclick.orchestration.dynamic._execute_reduce",
        name=f"_execute_reduce({func_name})",
        kwargs={
            "target_entrypoint": target_entrypoint,
            "group_id": mapped.group.id,
        },
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow(),
    )

    # Reduce depends on the map group (all partition tasks must complete)
    mapped.group >> reduce_task

    return reduce_task


async def _expand_map(
    target_entrypoint: str,
    object_ref: dict,
    extra_kwargs: dict,
    partition_size: int,
    group_id: int,
) -> None:
    """Expander task: queries Object row count and creates partition tasks.

    This function is executed by a worker at runtime. It:
    1. Resolves the Object reference to get the table name
    2. Queries ClickHouse for the row count
    3. Creates N partition tasks, each with a View (offset/limit)
    4. Commits them to PostgreSQL in the same job

    Args:
        target_entrypoint: Import path of the user's map function.
        object_ref: Serialized Object or upstream reference.
        extra_kwargs: Additional kwargs to pass to each partition task.
        partition_size: Number of rows per partition.
        group_id: Group ID for the partition tasks.
    """
    # Resolve the object reference to get the table name
    table_name = await _resolve_object_table(object_ref)

    # Query ClickHouse for row count
    ch_client = get_ch_client()
    result = await ch_client.query(f"SELECT count() FROM {table_name}")
    row_count = result.first_row[0]

    # Calculate number of partitions
    n_partitions = max(1, ceil(row_count / partition_size))

    # Get current task's job_id from orch context
    job_id = await _get_current_job_id()

    # Get expander task ID for dependency wiring
    expander_task_id = await _get_current_task_id()

    # Create N child tasks
    child_name = _name_from_entrypoint(target_entrypoint)
    tasks = []
    for i in range(n_partitions):
        # Build kwargs: partition view + extra kwargs
        partition_kwargs = dict(extra_kwargs)
        partition_kwargs["partition"] = {
            "object_type": "view",
            "table": table_name,
            "limit": partition_size,
            "offset": i * partition_size,
            "order_by": "id",
        }

        child = Task(
            id=get_snowflake_id(),
            entrypoint=target_entrypoint,
            name=child_name,
            kwargs=partition_kwargs,
            group_id=group_id,
            status=TaskStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        # Child depends on expander task completing
        dep = Dependency(
            previous_id=expander_task_id,
            previous_type="task",
            next_id=child.id,
            next_type="task",
        )
        child.previous_dependencies.append(dep)

        tasks.append(child)

    # Commit child tasks to database
    await commit_tasks(tasks, job_id)


async def _execute_reduce(
    target_entrypoint: str,
    group_id: int,
) -> Any:
    """Reduce task: collects all partition results and calls the reduce function.

    This function is executed by a worker after all partition tasks complete.
    It queries all completed tasks in the map group, deserializes their results,
    and passes them as a list to the user's reduce function.

    Args:
        target_entrypoint: Import path of the user's reduce function.
        group_id: Group ID containing the partition tasks.

    Returns:
        Result of the user's reduce function.
    """
    func = import_callback(target_entrypoint)

    # Query all completed tasks in the group, ordered by ID (preserves partition order)
    async with get_orch_session() as session:
        result = await session.execute(
            select(Task.result, Task.job_id)
            .where(
                TaskModel.group_id == group_id,
                TaskModel.status == TaskStatus.COMPLETED,
            )
            .order_by(TaskModel.id)
        )
        rows = result.all()

        # Deserialize each task's result
        results = []
        for row in rows:
            task_result, job_id = row
            if task_result is not None and isinstance(task_result, dict):
                task_result["job_id"] = job_id
            deserialized = await _deserialize_value(task_result, session)
            results.append(deserialized)

    # Call user's reduce function
    if asyncio.iscoroutinefunction(func):
        return await func(results)
    return func(results)


async def _resolve_object_table(ref: dict) -> str:
    """Resolve an object reference to its ClickHouse table name.

    Handles both direct Object references and upstream Task references.
    """
    if ref.get("ref_type") == "upstream":
        # Resolve upstream task to get its result
        async with get_orch_session() as session:
            upstream_result = await _resolve_upstream_ref(ref, session)
            if isinstance(upstream_result, dict) and "table" in upstream_result:
                return upstream_result["table"]
            raise ValueError(f"Upstream task result is not an Object: {upstream_result}")

    if "table" in ref:
        return ref["table"]

    raise ValueError(f"Cannot resolve table name from reference: {ref}")


async def _get_current_job_id() -> int:
    """Get the job_id of the currently executing task.

    The expander task's job_id is set when it was committed to the database.
    We retrieve it from the task record.
    """
    info = get_current_task_info()
    return info.job_id


async def _get_current_task_id() -> int:
    """Get the task_id of the currently executing task."""
    info = get_current_task_info()
    return info.task_id
