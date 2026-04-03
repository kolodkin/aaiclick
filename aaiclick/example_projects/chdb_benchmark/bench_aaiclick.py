"""aaiclick benchmark — Object API over ClickHouse, compute only (no .data()).

Uses aaiclick's high-level API: create_object_from_value, group_by, agg,
where, view, arithmetic operators, unique, count, sum. All computation
stays inside ClickHouse — Python only orchestrates SQL generation.
"""

import aaiclick
from aaiclick import create_object_from_value
from aaiclick.data import Agg
from aaiclick.data.data_context import data_context

NAME = "aaiclick"
VERSION = aaiclick.__version__
IS_ASYNC = True


def context(lifecycle=False):
    return data_context(lifecycle=lifecycle)


async def convert(data, filter_threshold):
    return await create_object_from_value(data)


async def _col_sum(obj):
    return await obj["amount"].sum()


async def _col_mul(obj):
    return await (obj["amount"] * obj["quantity"])


async def _filter(obj, filter_threshold):
    view = obj.where(f"amount > {filter_threshold}")
    return await view.copy()


async def _sort(obj):
    # copy() regenerates aai_ids in sorted insertion order via
    # INSERT...SELECT...ORDER BY amount DESC, aai_id.
    # Read 1000 rows to verify sort and measure end-to-end cost.
    sorted_obj = await obj.view(order_by="amount DESC").copy()
    return await sorted_obj.view(limit=1000).data()


async def _count_distinct(obj):
    return await obj["category"].nunique()


async def _groupby_sum(obj):
    return await obj.group_by("category").sum("amount")


async def _groupby_count(obj):
    return await obj.group_by("category").count()


async def _groupby_multi(obj):
    """Single .agg() call — one GROUP BY query with all four aggregations."""
    return await obj.group_by("category").agg({
        "amount": [
            Agg("sum", "amount_sum"),
            Agg("mean", "amount_mean"),
            Agg("min", "amount_min"),
            Agg("max", "amount_max"),
        ],
    })


async def _groupby_multikey(obj):
    return await obj.group_by("category", "subcategory").sum("amount")


async def _groupby_highcard(obj):
    return await obj.group_by("subcategory").sum("amount")


def make_benchmarks(filter_threshold):
    return {
        "Column sum": _col_sum,
        "Column multiply": _col_mul,
        "Filter rows": lambda obj: _filter(obj, filter_threshold),
        "Sort": _sort,
        "Count distinct": _count_distinct,
        "Group-by sum": _groupby_sum,
        "Group-by count": _groupby_count,
        "Group-by multi-agg": _groupby_multi,
        "Multi-key group-by": _groupby_multikey,
        "High-card group-by": _groupby_highcard,
    }
