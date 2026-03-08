"""Dynamic task creation operators for orchestration backend.

Provides map() for parallel data processing, inspired by Apache Spark's
partition-based parallelism.

Usage:
    from aaiclick.orchestration import job, task, map

    @task
    async def double(row):
        ...

    @job("parallel_pipeline")
    def pipeline():
        data = load_data()
        mapped = map(cbk=double, obj=data, partition=5000)
        return [data, mapped]
"""

from __future__ import annotations

import asyncio
from math import ceil
from typing import Callable, Union

from aaiclick.data.data_context import create_object, get_ch_client
from aaiclick.data.object import Object, View
from aaiclick.snowflake_id import get_snowflake_id

from .decorators import TaskFactory, task
from .models import Group, Task


def map(cbk: Union[Callable, TaskFactory], obj: Union[Task, Object],
        partition: int = 5000) -> Group:
    """Create a parallel map operation over partitions of an Object.

    At definition time, creates an expander Task + Group and returns the Group.
    At runtime, the expander queries ClickHouse for row count and creates
    N partition child tasks (one per partition).

    Args:
        cbk: Callback function applied to each row. Signature: cbk(row) -> None.
        obj: Task or Object to partition. If Task, expander waits for it.
        partition: Number of rows per partition (default 5000).

    Returns:
        Group containing the expander task.
    """
    group = Group(id=get_snowflake_id(), name="map")

    # Create expander task with serialized args
    expander = _expand_map(
        cbk=cbk,
        obj=obj,
        partition=partition,
        group_id=group.id,
    )

    group.add_task(expander)
    return group


@task
async def _expand_map(cbk: Callable, obj: Object, partition: int,
                      group_id: int) -> list:
    """Expander task: queries Object row count and creates partition tasks.

    Runs at execution time. Partitions the Object into Views and creates
    N map_part child tasks.

    Args:
        cbk: Callback function applied to each row.
        obj: Object to partition.
        partition: Number of rows per partition.
        group_id: Group ID for the partition tasks.

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
        )
        child.group_id = group_id
        tasks.append(child)

    return tasks


@task
async def map_part(cbk: Callable, part: View, out: Object) -> None:
    """Apply a callback to each row in a partition View.

    Reads rows from the partition, calls cbk(row) for each, and writes
    results to the output Object preserving aai_id.

    Args:
        cbk: Callback function. Signature: cbk(row) -> None.
        part: View (partition) of the source Object.
        out: Output Object to write results to.
    """
    is_async = asyncio.iscoroutinefunction(cbk)
    rows = await part.data()
    for row in rows:
        if is_async:
            await cbk(row)
        else:
            cbk(row)
