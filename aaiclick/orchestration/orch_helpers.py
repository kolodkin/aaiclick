"""Dynamic task creation operators for orchestration backend.

Provides map() and map_part() as @task-decorated functions for parallel
data processing, inspired by Apache Spark's partition-based parallelism.

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
from datetime import datetime
from math import ceil
from typing import Callable

from aaiclick.data.data_context import create_object, get_ch_client
from aaiclick.data.object import Object, View
from aaiclick.snowflake_id import get_snowflake_id

from .decorators import task
from .models import Group, Task, TaskStatus
from .worker_context import get_current_task_info


@task
async def map(cbk: Callable, obj: Object, partition: int = 5000) -> list:
    """Partition an Object and create parallel tasks for each partition.

    Queries ClickHouse for the Object's row count, creates an output Object,
    a Group, and N map_part child tasks (one per partition). Each child task
    applies cbk to every row in its partition View.

    Args:
        cbk: Callback function applied to each row. Signature: cbk(row) -> None.
        obj: Object to partition.
        partition: Number of rows per partition (default 5000).

    Returns:
        List of [output_object, group, *child_tasks] for dynamic registration.
    """
    table_name = obj.table
    ch_client = get_ch_client()
    result = await ch_client.query(f"SELECT count() FROM {table_name}")
    row_count = result.first_row[0]
    n_partitions = max(1, ceil(row_count / partition))

    out = await create_object(obj.schema)

    group = Group(id=get_snowflake_id(), name="map")

    info = get_current_task_info()
    expander_task_id = info.task_id

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
        child.group_id = group.id
        tasks.append(child)

    return [group, *tasks]


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
