"""Dynamic task creation operators for orchestration backend.

Provides map() for parallel data processing and reduce() for layered parallel
reduction, inspired by Apache Spark's partition-based parallelism.

Usage:
    from aaiclick.orchestration import job, task, map, reduce

    @task
    async def scale(row, factor=1):
        ...

    @task
    async def aggregate(partition: Object) -> Object:
        ...

    @job("parallel_pipeline")
    def pipeline():
        data = load_data()
        mapped = map(cbk=scale, obj=data, partition=5000, kwargs={"factor": 2})
        return [data, mapped]

    @job("reduce_pipeline")
    def reduce_pipeline(data: Object):
        return reduce(aggregate, data, partition=5000)
"""

from __future__ import annotations

import asyncio
from math import ceil
from typing import Any, Callable, Dict, Tuple, Union

from aaiclick.data.data_context import (
    create_object,
    create_object_from_value,
    get_ch_client,
    get_data_lifecycle,
)
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
    N _map_part child tasks.

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
        child = _map_part(
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
async def _map_part(cbk: Callable, part: View, out: Object,
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


def reduce(
    cbk: Union[Callable, TaskFactory],
    obj: Union[Task, Object],
    initializer=None,
    *,
    partition: int = 5000,
    args: Tuple = (),
    kwargs: Dict[str, Any] = None,
) -> Group:
    """Create a layered parallel reduction over an Object.

    At definition time, creates an expander Task + Group and returns the Group.
    At runtime, the expander queries row count, pre-allocates all layer Objects,
    and creates all layer subgroups and partition tasks at once.

    All layers are registered together in a single expansion step — no lazy
    layer-by-layer creation. The final Object (1 row) is returned as
    _expand_reduce's task result.

    Args:
        cbk: Callback applied to each partition. Must be homomorphic:
             output schema must match input schema. Returns 1 row.
             Signature: async def f(partition: Object, *args, **kwargs) -> Object
        obj: Task or Object to reduce. If Task, expander waits for it.
        initializer: Optional value prepended to input before reduction.
                     Raises TypeError if obj is empty and no initializer.
        partition: Max rows per partition task (default 5000).
        args: Extra positional arguments forwarded to cbk.
        kwargs: Extra keyword arguments forwarded to cbk.

    Returns:
        Group containing the expander task. The expander's task result is
        the final single-row Object.
    """
    if kwargs is None:
        kwargs = {}

    group = Group(id=get_snowflake_id(), name="reduce")

    expander = _expand_reduce(
        cbk=cbk,
        obj=obj,
        partition=partition,
        cbk_args=list(args),
        cbk_kwargs=kwargs,
        initializer=initializer,
    )

    expander.group_id = group.id
    group.add_task(expander)
    return group


@task
async def _expand_reduce(
    cbk: Callable,
    obj: Object,
    partition: int,
    cbk_args: list,
    cbk_kwargs: dict,
    initializer,
) -> tuple:
    """Expander task: queries count, pre-allocates all layers, creates all tasks.

    Runs once at execution time. Returns the final Object as its task result
    alongside all layer subgroups and partition tasks as dynamic children.

    Returns:
        Tuple of (final_obj, [layer_groups]) for mixed task+data extraction.
    """
    ch = get_ch_client()

    if initializer is not None:
        init_obj = await create_object_from_value(initializer)
        combined = await create_object(obj.schema)
        await ch.command(f"INSERT INTO {combined.table} SELECT * FROM {init_obj.table}")
        await ch.command(f"INSERT INTO {combined.table} SELECT * FROM {obj.table}")
        lifecycle = get_data_lifecycle()
        if lifecycle is not None and not combined.persistent:
            lifecycle.pin(combined.table)
        obj = combined

    result = await ch.query(f"SELECT count() FROM {obj.table}")
    count = result.first_row[0]

    if count == 0:
        raise TypeError("reduce() of empty sequence with no initial value")

    # Compute layer sizes: sizes[0]=N, sizes[1]=ceil(N/P), ..., last=1
    sizes = [count]
    while sizes[-1] > 1:
        sizes.append(ceil(sizes[-1] / partition))
    num_layers = len(sizes) - 1

    if num_layers == 0:
        # Input already has 1 row — copy to a fresh Object
        result_obj = await create_object(obj.schema)
        await ch.command(f"INSERT INTO {result_obj.table} SELECT * FROM {obj.table}")
        return (result_obj, [])

    # Pre-allocate all layer Objects
    layer_objs = [await create_object(obj.schema) for _ in range(num_layers)]

    # Pin intermediate layers so they outlive _expand_reduce's data_context.
    # The last layer is pinned by execute_task via the (data, tasks) tuple.
    if state.lifecycle is not None:
        for lo in layer_objs[:-1]:
            if not lo.persistent:
                state.lifecycle.pin(lo.table)

    # Build all layer groups and partition tasks in one shot
    all_groups = []
    prev_layer_group = None

    for L in range(num_layers):
        src = obj if L == 0 else layer_objs[L - 1]
        M = ceil(sizes[L] / partition)
        layer_group = Group(id=get_snowflake_id(), name=f"layer_{L}")

        if prev_layer_group is not None:
            layer_group.depends_on(prev_layer_group)

        for i in range(M):
            part_task = _reduce_part(
                cbk=cbk,
                part={
                    "object_type": "view",
                    "table": src.table,
                    "limit": partition,
                    "offset": i * partition,
                    "order_by": "aai_id",
                },
                layer_obj=layer_objs[L],
                cbk_args=cbk_args,
                cbk_kwargs=cbk_kwargs,
            )
            part_task.group_id = layer_group.id
            layer_group.add_task(part_task)

        all_groups.append(layer_group)
        prev_layer_group = layer_group

    return (layer_objs[-1], all_groups)


@task
async def _reduce_part(
    cbk: Callable,
    part: View,
    layer_obj: Object,
    cbk_args: list = None,
    cbk_kwargs: dict = None,
) -> None:
    """Apply callback to a partition View, writing results into layer_obj.

    Args:
        cbk: Homomorphic reduction function. Signature:
             async def f(partition: Object, output: Object, *args, **kwargs) -> None
        part: View (partition) of the source Object.
        layer_obj: Pre-allocated destination Object for this layer.
        cbk_args: Extra positional arguments forwarded to cbk.
        cbk_kwargs: Extra keyword arguments forwarded to cbk.
    """
    if cbk_args is None:
        cbk_args = []
    if cbk_kwargs is None:
        cbk_kwargs = {}

    is_async = asyncio.iscoroutinefunction(cbk)
    if is_async:
        await cbk(part, layer_obj, *cbk_args, **cbk_kwargs)
    else:
        cbk(part, layer_obj, *cbk_args, **cbk_kwargs)
