"""aaiclick benchmark — Object API over ClickHouse, compute stays in-engine.

Uses an explicit Schema with LowCardinality(String) for the categorical
columns so the schema matches the native chdb baseline exactly. All
operations (sum, filter, sort, group-by) run as ClickHouse SQL — Python
only orchestrates query generation.
"""

import aaiclick
from aaiclick import ColumnInfo, Schema, create_object
from aaiclick.data import Agg
from aaiclick.data.data_context import data_context

from .config import FILTER_THRESHOLD

NAME = "aaiclick"
VERSION = aaiclick.__version__
IS_ASYNC = True

_SCHEMA = Schema(
    fieldtype="d",
    columns={
        "aai_id": ColumnInfo("UInt64"),
        "id": ColumnInfo("Int64"),
        "category": ColumnInfo("String", low_cardinality=True),
        "subcategory": ColumnInfo("String", low_cardinality=True),
        "amount": ColumnInfo("Float64"),
        "quantity": ColumnInfo("Int64"),
    },
)


def context():
    """Open a fresh DataContext. Called once per benchmark operation."""
    return data_context()


async def convert(data):
    obj = await create_object(_SCHEMA)
    await obj.insert(data)
    return obj


async def _col_sum(obj):
    return await obj["amount"].sum()


async def _col_mul(obj):
    return await (obj["amount"] * obj["quantity"])


async def _filter(obj):
    view = obj.where(f"amount > {FILTER_THRESHOLD}")
    return await view.copy()


async def _sort(obj):
    view = obj.view(order_by="amount DESC")
    return await view.copy()


async def _count_distinct(obj):
    return await obj["category"].nunique()


async def _groupby_sum(obj):
    return await obj.group_by("category").sum("amount")


async def _groupby_count(obj):
    return await obj.group_by("category").count()


async def _groupby_multi(obj):
    return await obj.group_by("category").agg(
        {
            "amount": [
                Agg("sum", "amount_sum"),
                Agg("mean", "amount_mean"),
                Agg("min", "amount_min"),
                Agg("max", "amount_max"),
            ],
        }
    )


async def _groupby_multikey(obj):
    return await obj.group_by("category", "subcategory").sum("amount")


async def _groupby_highcard(obj):
    return await obj.group_by("subcategory").sum("amount")


BENCHMARKS = {
    "Column sum": _col_sum,
    "Column multiply": _col_mul,
    "Filter rows": _filter,
    "Sort": _sort,
    "Count distinct": _count_distinct,
    "Group-by sum": _groupby_sum,
    "Group-by count": _groupby_count,
    "Group-by multi-agg": _groupby_multi,
    "Multi-key group-by": _groupby_multikey,
    "High-card group-by": _groupby_highcard,
}
