"""Tests for ``with_snowflake_id=True`` ergonomic helper.

When set, ``create_object_from_value`` adds a ``snowflake_id UInt64`` column
with ``DEFAULT generateSnowflakeID()`` so callers can recover insertion
order via ``view(order_by="snowflake_id")``.

generateSnowflakeID() evaluates per-row in ClickHouse, so each row in a
batch insert gets a distinct, monotonically-increasing 64-bit ID.
"""

import pytest

from aaiclick import FIELDTYPE_ARRAY, create_object_from_value
from aaiclick.data.data_context import get_ch_client


async def test_with_snowflake_id_adds_column(ctx):
    obj = await create_object_from_value([1, 2, 3], with_snowflake_id=True)
    assert "snowflake_id" in obj.schema.columns
    col = obj.schema.columns["snowflake_id"]
    assert col.type == "UInt64"
    assert col.default == "generateSnowflakeID()"
    assert col.fieldtype == FIELDTYPE_ARRAY


async def test_with_snowflake_id_default_off(ctx):
    obj = await create_object_from_value([1, 2, 3])
    assert "snowflake_id" not in obj.schema.columns


async def test_with_snowflake_id_unique_per_row(ctx):
    """generateSnowflakeID() is per-row, so each row in a batch gets a unique ID."""
    obj = await create_object_from_value([10, 20, 30, 40, 50], with_snowflake_id=True)
    rows = await get_ch_client().query(f"SELECT snowflake_id FROM {obj.table}")
    ids = [row[0] for row in rows.result_rows]
    assert len(ids) == 5
    assert len(set(ids)) == 5, f"snowflake_ids must be unique per row, got {ids}"


async def test_with_snowflake_id_recovers_insertion_order(ctx):
    """view(order_by='snowflake_id').data() returns rows in insertion order."""
    obj = await create_object_from_value([3, 1, 2], with_snowflake_id=True)
    assert await obj.view(order_by="snowflake_id").data() == [3, 1, 2]


async def test_with_snowflake_id_dict_of_arrays(ctx):
    """Dict-of-arrays gains snowflake_id; user columns insert as-is."""
    obj = await create_object_from_value(
        {"x": [1, 2, 3], "label": ["a", "b", "c"]},
        with_snowflake_id=True,
    )
    assert "snowflake_id" in obj.schema.columns
    data = await obj.data(order_by="snowflake_id")
    assert data["x"] == [1, 2, 3]
    assert data["label"] == ["a", "b", "c"]


async def test_with_snowflake_id_collision_raises(ctx):
    """Passing with_snowflake_id=True and a user column named 'snowflake_id' is rejected."""
    with pytest.raises(ValueError, match="snowflake_id"):
        await create_object_from_value(
            {"snowflake_id": [1, 2, 3], "value": [10, 20, 30]},
            with_snowflake_id=True,
        )


async def test_cross_table_pair_stable_with_snowflake_id(ctx):
    """Cross-table a + b pairs rows by snowflake_id — input position is preserved
    by the JOIN.

    Inputs are chosen so positional pairing and value-sorted pairing give
    different multisets — proves the JOIN actually used snowflake_id.

    The result table does **not** carry ``snowflake_id`` — operator output is
    a plain ``value``-only Object — so callers verify pair-stability by
    checking the result multiset, not row order.
    """
    a = await create_object_from_value([10, 20, 30], with_snowflake_id=True)
    b = await create_object_from_value([-5, 5, 0], with_snowflake_id=True)

    result = await (a.view(order_by="snowflake_id") + b.view(order_by="snowflake_id"))
    # Positional pairing: (10-5, 20+5, 30+0) = [5, 25, 30]   ← multiset {5,25,30}
    # Value-sorted pairing would be: (10-5, 20+0, 30+5) = [5, 20, 35]
    # Multiset proves we got positional pairing.
    assert sorted(await result.data()) == [5, 25, 30]
