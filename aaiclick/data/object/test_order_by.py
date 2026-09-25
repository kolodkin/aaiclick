"""
Tests for custom order_by support on Object creation.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data import ENGINE_MERGE_TREE, ColumnInfo, Object, Schema
from aaiclick.data.data_context import data_context, get_ch_client
from aaiclick.data.models import build_order_by_clause


@pytest.mark.parametrize(
    "columns, expected",
    [
        # Empty input yields tuple() — no implicit key, and never an injected aai_id.
        pytest.param([], "tuple()", id="empty-returns-tuple"),
        pytest.param(["date"], "(date)", id="single-column"),
        pytest.param(["date", "category"], "(date, category)", id="multiple-columns"),
    ],
)
def test_build_order_by_clause(columns, expected):
    """Pure function: the returned ORDER BY clause is the contract, user columns kept verbatim."""
    assert build_order_by_clause(columns) == expected


def test_object_init_order_by_sets_schema():
    """Object.__init__ with order_by sets schema.order_by."""
    schema = Schema(
        fieldtype="d",
        columns={"date": ColumnInfo("String"), "category": ColumnInfo("String")},
    )
    obj = Object(schema=schema, order_by=["date"])
    assert obj.schema.order_by == "(date)"


def test_object_init_no_order_by():
    """Object.__init__ without order_by leaves schema.order_by as None."""
    schema = Schema(
        fieldtype="d",
        columns={"date": ColumnInfo("String"), "category": ColumnInfo("String")},
    )
    obj = Object(schema=schema)
    assert obj.schema.order_by is None


async def test_order_by_warns_on_memory(ctx):
    """order_by on a Memory-default context emits a UserWarning."""
    with pytest.warns(UserWarning, match="order_by=.*has no effect with Memory engine"):
        await create_object_from_value(
            {"date": ["2024-01-03", "2024-01-01", "2024-01-02"], "val": [30, 10, 20]},
            order_by=["date"],
        )


async def test_order_by_mergetree():
    """order_by with MergeTree engine sets correct sorting_key."""
    async with data_context(engine=ENGINE_MERGE_TREE):
        ch = get_ch_client()
        obj = await create_object_from_value(
            {"date": ["2024-01-03", "2024-01-01", "2024-01-02"], "val": [30, 10, 20]},
            order_by=["date"],
        )

        result = await ch.query(f"SELECT engine, sorting_key FROM system.tables WHERE name = '{obj.table}'")
        assert result.result_rows[0][0] == "MergeTree"
        assert result.result_rows[0][1] == "(date)"


@pytest.mark.parametrize(
    "value, kwargs, expected",
    [
        # Unlike a single-column key, ClickHouse flattens a multi-column tuple, so
        # ``sorting_key`` reads back without the surrounding parentheses.
        pytest.param(
            {"category": ["b", "a", "a"], "date": ["2024-01-01", "2024-01-02", "2024-01-01"], "val": [1, 2, 3]},
            {"order_by": ["category", "date"]},
            "category, date",
            id="multi-column",
        ),
        # With an aai_id column and no explicit order_by, MergeTree sorts by aai_id.
        pytest.param({"val": [10, 20, 30]}, {"aai_id": True}, "(aai_id)", id="falls-back-to-aai-id"),
        # Without aai_id and without explicit order_by, MergeTree gets tuple().
        pytest.param({"val": [10, 20, 30]}, {}, "", id="no-aai-id-uses-tuple"),
        # An explicit order_by wins over the aai_id fallback even when aai_id is present.
        pytest.param(
            {"date": ["2024-01-03", "2024-01-01", "2024-01-02"], "val": [30, 10, 20]},
            {"order_by": ["date"], "aai_id": True},
            "(date)",
            id="explicit-order-by-overrides-aai-id-fallback",
        ),
    ],
)
async def test_mergetree_sorting_key(value, kwargs, expected):
    async with data_context(engine=ENGINE_MERGE_TREE):
        ch = get_ch_client()
        obj = await create_object_from_value(value, **kwargs)

        result = await ch.query(f"SELECT sorting_key FROM system.tables WHERE name = '{obj.table}'")
        assert result.result_rows[0][0] == expected


async def test_no_order_by_stays_memory(ctx):
    """Without order_by the default Memory engine is used."""
    ch = get_ch_client()
    obj = await create_object_from_value(
        {"date": ["2024-01-01", "2024-01-02"], "val": [10, 20]},
    )

    result = await ch.query(f"SELECT engine FROM system.tables WHERE name = '{obj.table}'")
    assert result.result_rows[0][0] == "Memory"
