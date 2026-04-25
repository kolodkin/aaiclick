"""Tests for the ``with_aai_id`` ergonomic helper on ``create_object_from_value``.

When set, the helper adds an aaiclick-managed row-id column (``UInt64`` with
``DEFAULT generateSnowflakeID()``) so callers can recover insertion order via
``view(order_by=<name>)`` and pair-stable cross-table arithmetic.

API:
- ``with_aai_id=False`` (default): no column.
- ``with_aai_id=True``: column named ``"id"``.
- ``with_aai_id="custom"``: column named ``"custom"``.

generateSnowflakeID() evaluates per-row in ClickHouse, so each row in a batch
insert gets a distinct, monotonically-increasing 64-bit ID.
"""

import pytest

from aaiclick import FIELDTYPE_ARRAY, create_object_from_value
from aaiclick.data.data_context import get_ch_client


async def test_with_aai_id_true_adds_id_column(ctx):
    obj = await create_object_from_value([1, 2, 3], with_aai_id=True)
    assert "id" in obj.schema.columns
    col = obj.schema.columns["id"]
    assert col.type == "UInt64"
    assert col.default == "generateSnowflakeID()"
    assert col.fieldtype == FIELDTYPE_ARRAY


async def test_with_aai_id_default_off(ctx):
    obj = await create_object_from_value([1, 2, 3])
    assert "id" not in obj.schema.columns


async def test_with_aai_id_str_uses_custom_name(ctx):
    obj = await create_object_from_value([1, 2, 3], with_aai_id="row_id")
    assert "row_id" in obj.schema.columns
    assert "id" not in obj.schema.columns
    col = obj.schema.columns["row_id"]
    assert col.type == "UInt64"
    assert col.default == "generateSnowflakeID()"


async def test_with_aai_id_unique_per_row(ctx):
    """generateSnowflakeID() is per-row, so each row in a batch gets a unique ID."""
    obj = await create_object_from_value([10, 20, 30, 40, 50], with_aai_id=True)
    rows = await get_ch_client().query(f"SELECT id FROM {obj.table}")
    ids = [row[0] for row in rows.result_rows]
    assert len(ids) == 5
    assert len(set(ids)) == 5, f"ids must be unique per row, got {ids}"


async def test_with_aai_id_recovers_insertion_order(ctx):
    """view(order_by='id').data() returns rows in insertion order."""
    obj = await create_object_from_value([3, 1, 2], with_aai_id=True)
    assert await obj.view(order_by="id").data() == [3, 1, 2]


async def test_with_aai_id_dict_of_arrays(ctx):
    """Dict-of-arrays gains the id column; user columns insert as-is."""
    obj = await create_object_from_value(
        {"x": [1, 2, 3], "label": ["a", "b", "c"]},
        with_aai_id=True,
    )
    assert "id" in obj.schema.columns
    data = await obj.data(order_by="id")
    assert data["x"] == [1, 2, 3]
    assert data["label"] == ["a", "b", "c"]


async def test_with_aai_id_collision_raises(ctx):
    """User column named 'id' collides with the default-name helper."""
    with pytest.raises(ValueError, match="id"):
        await create_object_from_value(
            {"id": [1, 2, 3], "value": [10, 20, 30]},
            with_aai_id=True,
        )


async def test_with_aai_id_str_collision_raises(ctx):
    """User column named 'row_id' collides with with_aai_id='row_id'."""
    with pytest.raises(ValueError, match="row_id"):
        await create_object_from_value(
            {"row_id": [1, 2, 3], "value": [10, 20, 30]},
            with_aai_id="row_id",
        )


async def test_cross_table_pair_stable_with_aai_id(ctx):
    """Cross-table a + b pairs rows by id — input position is preserved by the JOIN.

    Inputs are chosen so positional pairing and value-sorted pairing give
    different multisets — proves the JOIN actually used the id column.

    The result table does **not** carry the id column — operator output is a
    plain ``value``-only Object — so callers verify pair-stability by checking
    the result multiset, not row order.
    """
    a = await create_object_from_value([10, 20, 30], with_aai_id=True)
    b = await create_object_from_value([-5, 5, 0], with_aai_id=True)

    result = await (a.view(order_by="id") + b.view(order_by="id"))
    # Positional pairing: (10-5, 20+5, 30+0) = [5, 25, 30]   ← multiset {5,25,30}
    # Value-sorted pairing would be: (10-5, 20+0, 30+5) = [5, 20, 35]
    # Multiset proves we got positional pairing.
    assert sorted(await result.data()) == [5, 25, 30]
