"""Dynamic task creation operators for orchestration backend.

Provides map() for parallel data processing, inspired by Apache Spark's
partition-based parallelism.

Usage:
    from aaiclick.orchestration import job, task, map

    @task
    async def scale(row, factor=1):
        ...

    @job("parallel_pipeline")
    def pipeline():
        data = load_data()
        mapped = map(cbk=scale, obj=data, partition=5000, kwargs={"factor": 2})
        return [data, mapped]
"""

from __future__ import annotations

import asyncio
from math import ceil
from typing import Any, Callable, Dict, Tuple, Union

from aaiclick.data.data_context import create_object, get_ch_client
from aaiclick.data.object import Object, View
from aaiclick.snowflake_id import get_snowflake_id

from .decorators import TaskFactory, task
from .models import Group, Task


def map(cbk: Union[Callable, TaskFactory], obj: Union[Task, Object],
        partition: int = 5000,
        args: Tuple = (), kwargs: Dict[str, Any] = None) -> Group:
    """Create a parallel map operation over partitions of an Object.

    At definition time, creates an expander Task + Group and returns the Group.
    At runtime, the expander queries ClickHouse for row count and creates
    N partition child tasks (one per partition).

    Args:
        cbk: Callback function applied to each row.
        obj: Task or Object to partition. If Task, expander waits for it.
        partition: Number of rows per partition (default 5000).
        args: Extra positional arguments forwarded to cbk after row.
        kwargs: Extra keyword arguments forwarded to cbk.

    Returns:
        Group containing the expander task.
    """
    if kwargs is None:
        kwargs = {}

    group = Group(id=get_snowflake_id(), name="map")

    expander = _expand_map(
        cbk=cbk,
        obj=obj,
        partition=partition,
        group_id=group.id,
        cbk_args=list(args),
        cbk_kwargs=kwargs,
    )

    group.add_task(expander)
    return group


@task
async def _expand_map(cbk: Callable, obj: Object, partition: int,
                      group_id: int, cbk_args: list,
                      cbk_kwargs: dict) -> list:
    """Expander task: queries Object row count and creates partition tasks.

    Runs at execution time. Partitions the Object into Views and creates
    N map_part child tasks.

    Args:
        cbk: Callback function applied to each row.
        obj: Object to partition.
        partition: Number of rows per partition.
        group_id: Group ID for the partition tasks.
        cbk_args: Extra positional arguments forwarded to cbk.
        cbk_kwargs: Extra keyword arguments forwarded to cbk.

    Returns:
        List of child tasks for dynamic registration.
    """
    table_name = obj.table
    ch_client = get_ch_client()
    result = await ch_client.query(f"SELECT count() FROM {table_name}")
    row_count = result.first_row[0]
    n_partitions = max(1, ceil(row_count / partition))

    out = await create_object(obj.schema)

    tasks = []
    for i in range(n_partitions):
        child = map_part(
            cbk=cbk,
            part={
                "object_type": "view",
                "table": table_name,
                "limit": partition,
                "offset": i * partition,
                "order_by": "aai_id",
            },
            out=out,
            cbk_args=cbk_args,
            cbk_kwargs=cbk_kwargs,
        )
        child.group_id = group_id
        tasks.append(child)

    return tasks


@task
async def map_part(cbk: Callable, part: View, out: Object,
                   cbk_args: list = None, cbk_kwargs: dict = None) -> None:
    """Apply a callback to each row in a partition View.

    Reads rows from the partition, calls cbk(row, *args, **kwargs) for each.

    Args:
        cbk: Callback function. Signature: cbk(row, *args, **kwargs) -> None.
        part: View (partition) of the source Object.
        out: Output Object to write results to.
        cbk_args: Extra positional arguments forwarded to cbk.
        cbk_kwargs: Extra keyword arguments forwarded to cbk.
    """
    if cbk_args is None:
        cbk_args = []
    if cbk_kwargs is None:
        cbk_kwargs = {}
    is_async = asyncio.iscoroutinefunction(cbk)
    rows = await part.data()
    for row in rows:
        if is_async:
            await cbk(row, *cbk_args, **cbk_kwargs)
        else:
            cbk(row, *cbk_args, **cbk_kwargs)
