"""Dynamic task creation operators for orchestration backend.

Provides map() for parallel data processing and reduce() for layered parallel
reduction, inspired by Apache Spark's partition-based parallelism.

Usage:
    from aaiclick.orchestration import job, task, map, reduce

    @task
    async def scale(row, factor=1):
        ...

    @task
    async def aggregate(partition: Object, output: Object) -> None:
        ...

    @job("parallel_pipeline")
    def pipeline():
        data = load_data()
        mapped = map(cbk=scale, obj=data, partition=5000, kwargs={"factor": 2})
        return task_result(data=mapped, tasks=[data, mapped])

    @job("reduce_pipeline")
    def reduce_pipeline(data: Object):
        return reduce(aggregate, data, partition=5000)
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from math import ceil, log
from typing import Any

from aaiclick.data.data_context import (
    create_object,
    get_ch_client,
)
from aaiclick.data.models import ORIENT_RECORDS
from aaiclick.data.object import Object, View
from aaiclick.data.object.refs import ViewRef
from aaiclick.snowflake import get_snowflake_id

from .decorators import TaskFactory, task
from .models import Group, Task
from .result import TaskResult, data_list, task_result


def map(
    cbk: Callable | TaskFactory,
    obj: Task | Object,
    partition: int = 5000,
    args: tuple = (),
    kwargs: dict[str, Any] | None = None,
) -> Task:
    """Create a parallel map over partitions of an Object.

    Args:
        cbk: Callback applied to each row: ``cbk(row, *args, **kwargs)``.
            Its return value is appended to the output; ``None`` adds no row.
            The output schema equals the input schema, and returns are cast
            to it on insert.
        obj: Task or Object to partition. If Task, the expander waits for it.
        partition: Number of rows per partition.
        args: Extra positional arguments forwarded to cbk after row.
        kwargs: Extra keyword arguments forwarded to cbk.

    Returns:
        The expander Task. At runtime it creates one ``_map_part`` child per
        partition; its result is the output Object, and tasks that consume it
        wait for every partition.
    """
    if kwargs is None:
        kwargs = {}

    return _expand_map(
        cbk=cbk,
        obj=obj,
        partition=partition,
        cbk_args=list(args),
        cbk_kwargs=kwargs,
    )


@task
async def _expand_map(cbk: Callable, obj: Object, partition: int, cbk_args: list, cbk_kwargs: dict) -> TaskResult:
    """Expander task: queries Object row count and creates partition tasks.

    Returns a ``map`` Group of ``_map_part`` children writing into a
    pre-allocated output, and the ``_finalize`` task that returns it as data.
    """
    row_count = await obj.count().data()

    out = await create_object(obj.schema)

    group = Group(id=get_snowflake_id(), name="map")
    for part in _partition_refs(obj, partition, row_count):
        group.add_task(_map_part(cbk=cbk, part=part, out=out, cbk_args=cbk_args, cbk_kwargs=cbk_kwargs))

    finalize = _finalize(out=out)
    group >> finalize
    return task_result(data=finalize, tasks=[group, finalize])


@task
async def _finalize(out: Object) -> Object:
    """Join task: runs after every partition task and hands the filled output on.

    An expander returns it as data, so consumers of the expander wait for it
    and it pins ``out`` for them.
    """
    return out


def _partition_refs(src: Object | View, partition: int, rows: int) -> list[dict]:
    """Serialized View refs slicing the ``rows`` rows of ``src`` into LIMIT/OFFSET partitions.

    A View source keeps its WHERE, field selection and renames, and each slice
    is offset within the View's own window, so partitions cover exactly the
    rows the View selects. LIMIT/OFFSET needs a stable ordering for the slices
    to be disjoint: honour the source's order_by, falling back to tuple() (no-op).
    """
    base = ViewRef.model_validate(src._serialize_ref()) if isinstance(src, View) else ViewRef(table=src.table)
    start = base.offset or 0
    order_by = src.order_by or "tuple()"
    return [
        base.model_copy(
            update={
                "offset": start + i * partition,
                # An empty source still gets one partition; it reads nothing.
                "limit": min(partition, rows - i * partition) if rows else partition,
                "order_by": order_by,
            }
        ).to_dict()
        for i in range(max(1, ceil(rows / partition)))
    ]


@task
async def _map_part(
    cbk: Callable, part: View, out: Object, cbk_args: list | None = None, cbk_kwargs: dict | None = None
) -> None:
    """Apply a callback to each row in a partition View, appending returns to ``out``.

    Args:
        cbk: Callback function. Signature: cbk(row, *args, **kwargs) -> value | None.
        part: View (partition) of the source Object.
        out: Output Object; every non-None return is inserted.
        cbk_args: Extra positional arguments forwarded to cbk.
        cbk_kwargs: Extra keyword arguments forwarded to cbk.
    """
    if cbk_args is None:
        cbk_args = []
    if cbk_kwargs is None:
        cbk_kwargs = {}
    is_async = inspect.iscoroutinefunction(cbk)
    rows = await part.data(orient=ORIENT_RECORDS)
    results = []
    for row in rows:
        value = await cbk(row, *cbk_args, **cbk_kwargs) if is_async else cbk(row, *cbk_args, **cbk_kwargs)
        if value is not None:
            results.append(value)
    await out.insert(results)


def reduce(
    cbk: Callable | TaskFactory,
    obj: Task | Object,
    *,
    partition: int = 5000,
    args: tuple = (),
    kwargs: dict[str, Any] | None = None,
) -> Task:
    """Create a layered parallel reduction over an Object.

    Args:
        cbk: Callback applied to each partition; writes one row into
             ``output`` with the input schema (homomorphic).
             Signature: async def f(partition: Object, output: Object, *args, **kwargs) -> None
        obj: Task or Object to reduce. If Task, the expander waits for it.
        partition: Max rows per partition task.
        args: Extra positional arguments forwarded to cbk.
        kwargs: Extra keyword arguments forwarded to cbk.

    Returns:
        The expander Task. At runtime it pre-allocates every layer Object and
        registers all layer groups at once; its result is the final
        single-row Object, and tasks that consume it wait for every layer.
    """
    if kwargs is None:
        kwargs = {}

    return _expand_reduce(
        cbk=cbk,
        obj=obj,
        partition=partition,
        cbk_args=list(args),
        cbk_kwargs=kwargs,
    )


def _reduce_num_layers(count: int, partition: int) -> int:
    """Return the number of reduction layers needed for count rows at partition size."""
    return ceil(log(count, partition)) if count > 1 else 0


def _build_layer_group(
    L: int,
    src: Object | View,
    layer_obj: Object,
    src_size: int,
    partition: int,
    prev_group: Group | None,
    cbk: Callable,
    cbk_args: list,
    cbk_kwargs: dict,
) -> Group:
    """Build one reduce layer: a Group with ceil(src_size/partition) part tasks."""
    group = Group(id=get_snowflake_id(), name=f"layer_{L}")
    if prev_group is not None:
        group.depends_on(prev_group)
    for part in _partition_refs(src, partition, src_size):
        group.add_task(_reduce_part(cbk=cbk, part=part, layer_obj=layer_obj, cbk_args=cbk_args, cbk_kwargs=cbk_kwargs))
    return group


@task
async def _expand_reduce(
    cbk: Callable,
    obj: Object,
    partition: int,
    cbk_args: list,
    cbk_kwargs: dict,
) -> TaskResult:
    """Expander task: queries count, pre-allocates all layers, creates all tasks.

    Runs once at execution time. Returns all layer subgroups as dynamic
    children and, as data, the ``_finalize`` task that hands on the final
    Object once the last layer is done.
    """
    ch = get_ch_client()

    count = await obj.count().data()

    if count == 0:
        raise TypeError("reduce() of empty sequence with no initial value")

    num_layers = _reduce_num_layers(count, partition)

    if num_layers == 0:
        # Input already has 1 row — copy to a fresh Object
        result_obj = await create_object(obj.schema)
        await ch.command(f"INSERT INTO {result_obj.table} SELECT * FROM {obj.table}")
        return data_list(result_obj)

    # Pre-allocate all layer Objects. Registration pins each layer for the
    # part tasks that reference it; the last layer is also pinned for the
    # finalize task and, through its result, for the consumers.
    layer_objs = [await create_object(obj.schema) for _ in range(num_layers)]

    all_groups = []
    src_size = count
    for L in range(num_layers):
        src = obj if L == 0 else layer_objs[L - 1]
        group = _build_layer_group(
            L,
            src,
            layer_objs[L],
            src_size,
            partition,
            all_groups[-1] if all_groups else None,
            cbk,
            cbk_args,
            cbk_kwargs,
        )
        all_groups.append(group)
        src_size = ceil(src_size / partition)

    finalize = _finalize(out=layer_objs[-1])
    all_groups[-1] >> finalize
    return task_result(data=finalize, tasks=[*all_groups, finalize])


@task
async def _reduce_part(
    cbk: Callable,
    part: View,
    layer_obj: Object,
    cbk_args: list | None = None,
    cbk_kwargs: dict | None = None,
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

    is_async = inspect.iscoroutinefunction(cbk)
    if is_async:
        await cbk(part, layer_obj, *cbk_args, **cbk_kwargs)
    else:
        cbk(part, layer_obj, *cbk_args, **cbk_kwargs)
