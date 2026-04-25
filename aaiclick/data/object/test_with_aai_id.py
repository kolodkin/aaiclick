"""Tests for the ``with_aai_id`` ergonomic helper on ``create_object_from_value``.

When ``with_aai_id=True``, ``create_object_from_value`` adds an ``aai_id``
column (``UInt64`` with ``DEFAULT generateSnowflakeID()``) so callers can
recover insertion order via ``view(order_by="aai_id")`` and get pair-stable
cross-table arithmetic.

generateSnowflakeID() evaluates per-row in ClickHouse, so each row in a batch
insert gets a distinct, monotonically-increasing 64-bit ID.

Operator propagation
--------------------
When the LEFT operand of a binary operator carries ``aai_id``, the result
table propagates the LEFT side's ``aai_id`` values, so
``(a + b).view(order_by="aai_id")`` recovers the original row order.
"""

import pytest

from aaiclick import FIELDTYPE_ARRAY, create_object_from_value
from aaiclick.data.data_context import get_ch_client


async def test_with_aai_id_true_adds_aai_id_column(ctx):
    obj = await create_object_from_value([1, 2, 3], with_aai_id=True)
    assert "aai_id" in obj.schema.columns
    col = obj.schema.columns["aai_id"]
    assert col.type == "UInt64"
    assert col.default == "generateSnowflakeID()"
    assert col.fieldtype == FIELDTYPE_ARRAY


async def test_with_aai_id_default_off(ctx):
    obj = await create_object_from_value([1, 2, 3])
    assert "aai_id" not in obj.schema.columns


async def test_with_aai_id_unique_per_row(ctx):
    """generateSnowflakeID() is per-row, so each row in a batch gets a unique ID."""
    obj = await create_object_from_value([10, 20, 30, 40, 50], with_aai_id=True)
    rows = await get_ch_client().query(f"SELECT aai_id FROM {obj.table}")
    ids = [row[0] for row in rows.result_rows]
    assert len(ids) == 5
    assert len(set(ids)) == 5, f"aai_ids must be unique per row, got {ids}"


async def test_with_aai_id_recovers_insertion_order(ctx):
    """view(order_by='aai_id').data() returns rows in insertion order."""
    obj = await create_object_from_value([3, 1, 2], with_aai_id=True)
    assert await obj.view(order_by="aai_id").data() == [3, 1, 2]


async def test_aai_id_auto_defaults_order_by(ctx):
    """When the schema has aai_id and no explicit order_by, it becomes the default."""
    obj = await create_object_from_value([3, 1, 2], with_aai_id=True)
    # Object.order_by auto-resolves to "aai_id"
    assert obj.order_by == "aai_id"
    # data() now returns insertion order without an explicit view() wrapper
    assert await obj.data() == [3, 1, 2]


async def test_aai_id_auto_default_skipped_when_column_absent(ctx):
    """Without the aai_id column, order_by stays None (no implicit ordering)."""
    obj = await create_object_from_value([3, 1, 2])
    assert obj.order_by is None


async def test_aai_id_auto_default_overridden_by_explicit_view_order(ctx):
    """Explicit view(order_by=...) wins over the aai_id fallback."""
    obj = await create_object_from_value([3, 1, 2], with_aai_id=True)
    assert await obj.view(order_by="value").data() == [1, 2, 3]


async def test_cross_table_op_works_without_explicit_order_when_aai_id_present(ctx):
    """With aai_id on both sides, the cross-table contract is satisfied implicitly."""
    a = await create_object_from_value([10, 20, 30], with_aai_id=True)
    b = await create_object_from_value([-5, 5, 0], with_aai_id=True)
    result = await (a + b)  # no .view(order_by=...) wrappers needed
    assert "aai_id" in result.schema.columns
    assert await result.data() == [5, 25, 30]


async def test_with_aai_id_dict_of_arrays(ctx):
    """Dict-of-arrays gains the aai_id column; user columns insert as-is."""
    obj = await create_object_from_value(
        {"x": [1, 2, 3], "label": ["a", "b", "c"]},
        with_aai_id=True,
    )
    assert "aai_id" in obj.schema.columns
    data = await obj.data(order_by="aai_id")
    assert data["x"] == [1, 2, 3]
    assert data["label"] == ["a", "b", "c"]


async def test_with_aai_id_collision_raises(ctx):
    """User column named 'aai_id' collides with the helper-managed column."""
    with pytest.raises(ValueError, match="aai_id"):
        await create_object_from_value(
            {"aai_id": [1, 2, 3], "value": [10, 20, 30]},
            with_aai_id=True,
        )


async def test_operator_result_propagates_lhs_aai_id(ctx):
    """When LHS carries aai_id, the result table also carries aai_id (LHS values)."""
    a = await create_object_from_value([10, 20, 30], with_aai_id=True)
    b = await create_object_from_value([-5, 5, 0], with_aai_id=True)

    a_ids = [row[0] for row in (await get_ch_client().query(f"SELECT aai_id FROM {a.table}")).result_rows]

    result = await (a + b)
    assert "aai_id" in result.schema.columns

    result_ids = [row[0] for row in (await get_ch_client().query(f"SELECT aai_id FROM {result.table}")).result_rows]
    assert sorted(result_ids) == sorted(a_ids), "result aai_ids must be the multiset of LHS aai_ids"


async def test_operator_result_recovers_pair_order_via_aai_id(ctx):
    """The result auto-orders by aai_id when read, recovering pair-stable LHS order.

    Inputs are chosen so positional and value-sorted pairing differ; the
    propagated aai_id lets us re-sort the result back into LHS order.
    """
    a = await create_object_from_value([10, 20, 30], with_aai_id=True)
    b = await create_object_from_value([-5, 5, 0], with_aai_id=True)

    result = await (a + b)
    # Positional pairing: (10-5, 20+5, 30+0) = [5, 25, 30] in LHS order
    assert await result.data() == [5, 25, 30]


async def test_operator_no_propagation_when_lhs_lacks_aai_id(ctx):
    """RHS-only aai_id does NOT propagate — only LHS triggers propagation."""
    a = await create_object_from_value([10, 20, 30])  # no aai_id
    b = await create_object_from_value([-5, 5, 0], with_aai_id=True)

    # a has no aai_id — must wrap in view(order_by=...) to satisfy cross-table contract
    result = await (a.view(order_by="value") + b)
    assert "aai_id" not in result.schema.columns


async def test_scalar_broadcast_propagates_aai_id(ctx):
    """array-with-aai_id * scalar propagates aai_id to result."""
    a = await create_object_from_value([10, 20, 30], with_aai_id=True)
    result = await (a * 2)
    assert "aai_id" in result.schema.columns
    assert await result.data() == [20, 40, 60]


async def test_chained_operators_preserve_aai_id(ctx):
    """aai_id propagates through chained operators when all LHSes inherit it."""
    a = await create_object_from_value([10, 20, 30], with_aai_id=True)
    b = await create_object_from_value([-5, 5, 0], with_aai_id=True)
    c = await create_object_from_value([1, 2, 3], with_aai_id=True)

    result = await (await (a + b) + c)
    assert "aai_id" in result.schema.columns
    assert await result.data() == [6, 27, 33]


async def test_same_table_field_op_propagates_aai_id(ctx):
    """Same-table field-vs-field operator (fast path) propagates aai_id."""
    obj = await create_object_from_value(
        {"x": [1, 2, 3], "y": [10, 20, 30]},
        with_aai_id=True,
    )
    result = await (obj["x"] + obj["y"])
    assert "aai_id" in result.schema.columns
    assert await result.data() == [11, 22, 33]


async def test_aai_id_does_not_force_subquery_on_aggregation(ctx):
    """The aai_id auto order_by is order-only, so unconstrained Objects with
    aai_id must NOT have ``has_constraints=True`` — otherwise aggregation
    SQL wraps the source in a redundant ``(SELECT * FROM table ORDER BY aai_id)``
    subquery that ClickHouse may not always optimize away.
    """
    obj = await create_object_from_value([10, 20, 30], with_aai_id=True)
    assert obj.has_constraints is False
    info = obj._get_query_info()
    # Source must be the raw table name, not a wrapped subquery.
    assert info.source == obj.table
    # But operators still see aai_id ordering via order_by + aai_id_info.
    assert info.order_by == "aai_id"
    assert info.aai_id_info is not None
    # Aggregation still computes the correct value.
    assert await (await obj.sum()).data() == 60
