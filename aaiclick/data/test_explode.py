"""
Tests for Object.explode() — ARRAY JOIN flattening to rows.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.models import Schema, ColumnInfo, FIELDTYPE_DICT
from aaiclick.data.object import View


# =============================================================================
# Basic explode + data()
# =============================================================================


async def test_explode_single_column_data(ctx):
    """Exploding a single Array column returns each element as its own row."""
    obj = await create_object_from_value([
        {"user": "Alice", "tags": ["python", "rust"]},
        {"user": "Bob",   "tags": ["python", "go"]},
    ])
    flat = obj.explode("tags")
    assert isinstance(flat, View)
    result = await flat.data()
    assert result["user"] == ["Alice", "Alice", "Bob", "Bob"]
    assert result["tags"] == ["python", "rust", "python", "go"]


async def test_explode_returns_view(ctx):
    """explode() is synchronous and returns a View without DB call."""
    obj = await create_object_from_value([
        {"x": [1, 2], "y": "a"},
    ])
    flat = obj.explode("x")
    assert isinstance(flat, View)


async def test_explode_schema_type_change(ctx):
    """Exploded column changes from Array(T) to T in effective_columns."""
    obj = await create_object_from_value([
        {"user": "Alice", "scores": [90, 85]},
    ])
    flat = obj.explode("scores")
    eff = flat._effective_columns
    assert eff["scores"].array == 0
    assert eff["scores"].type == "Int64"
    # Non-exploded column unchanged
    assert eff["user"].array == 0
    assert eff["user"].type == "String"


# =============================================================================
# Explode + aggregation
# =============================================================================


async def test_explode_unique(ctx):
    """Explode + unique() deduplicates array elements across rows."""
    obj = await create_object_from_value([
        {"user": "Alice", "tags": ["python", "rust"]},
        {"user": "Bob",   "tags": ["python", "go"]},
    ])
    flat = obj.explode("tags")
    unique_tags = await flat["tags"].unique()
    result = sorted(await unique_tags.data())
    assert result == ["go", "python", "rust"]


async def test_explode_group_by_count(ctx):
    """Explode + group_by + count aggregates per-element frequency."""
    obj = await create_object_from_value([
        {"user": "Alice", "tags": ["python", "rust"]},
        {"user": "Bob",   "tags": ["python", "go"]},
    ])
    flat = obj.explode("tags")
    tag_counts = await flat.group_by("tags").count()
    result = await tag_counts.data()
    counts = dict(zip(result["tags"], result["_count"]))
    assert counts["python"] == 2
    assert counts["rust"] == 1
    assert counts["go"] == 1


async def test_explode_scalar_max(ctx):
    """Explode + max() returns the max over all array elements."""
    obj = await create_object_from_value([
        {"user": "Alice", "scores": [90, 85]},
        {"user": "Bob",   "scores": [70, 95]},
    ])
    result_obj = await obj.explode("scores")["scores"].max()
    assert await result_obj.data() == 95


async def test_explode_scalar_sum(ctx):
    """Explode + sum() sums all array elements across all rows."""
    obj = await create_object_from_value([
        {"user": "Alice", "vals": [1, 2]},
        {"user": "Bob",   "vals": [3, 4]},
    ])
    result_obj = await obj.explode("vals")["vals"].sum()
    assert await result_obj.data() == 10


# =============================================================================
# Multi-column explode (zip, not cartesian)
# =============================================================================


async def test_explode_multi_column_zip(ctx):
    """Exploding multiple columns zips them (not a cartesian product)."""
    obj = await create_object_from_value([
        {"user": "Alice", "tags": ["python", "rust"], "scores": [90, 85]},
        {"user": "Bob",   "tags": ["python", "go"],   "scores": [70, 95]},
    ])
    flat = obj.explode("tags", "scores")
    result = await flat.data()
    # 2 rows per user = 4 total rows
    assert len(result["user"]) == 4
    assert len(result["tags"]) == 4
    assert len(result["scores"]) == 4
    # Verify zip pairing: find Alice's entries
    pairs = list(zip(result["user"], result["tags"], result["scores"]))
    alice_pairs = [(t, s) for u, t, s in pairs if u == "Alice"]
    assert sorted(alice_pairs) == [("python", 90), ("rust", 85)]


# =============================================================================
# LEFT explode
# =============================================================================


async def test_left_explode_preserves_empty_arrays(ctx):
    """LEFT ARRAY JOIN keeps rows with empty arrays.

    For non-nullable String columns, ClickHouse emits '' (empty string)
    rather than NULL for empty-array rows.
    """
    obj = await create_object_from_value([
        {"user": "Alice", "tags": ["python"]},
        {"user": "Bob",   "tags": []},
    ])
    flat = obj.explode("tags", left=True)
    result = await flat.data()
    assert "Alice" in result["user"]
    assert "Bob" in result["user"]
    # Bob's tags entry is the type default for String: empty string
    bob_idx = result["user"].index("Bob")
    assert result["tags"][bob_idx] == ""


async def test_default_explode_drops_empty_arrays(ctx):
    """Default ARRAY JOIN (non-left) drops rows with empty arrays."""
    obj = await create_object_from_value([
        {"user": "Alice", "tags": ["python"]},
        {"user": "Bob",   "tags": []},
    ])
    flat = obj.explode("tags")
    result = await flat.data()
    assert "Bob" not in result["user"]
    assert "Alice" in result["user"]


# =============================================================================
# Chained operations on exploded view
# =============================================================================


async def test_explode_chained_where(ctx):
    """Explode followed by where() correctly filters exploded rows."""
    obj = await create_object_from_value([
        {"user": "Alice", "tags": ["python", "rust"]},
        {"user": "Bob",   "tags": ["java", "go"]},
    ])
    flat = obj.explode("tags").where("user = 'Alice'")
    result = await flat.data()
    assert sorted(result["tags"]) == ["python", "rust"]
    assert all(u == "Alice" for u in result["user"])


async def test_explode_materialized_copy(ctx):
    """copy() on an exploded view creates a real table with new IDs."""
    obj = await create_object_from_value([
        {"user": "Alice", "tags": ["python", "rust"]},
        {"user": "Bob",   "tags": ["go"]},
    ])
    flat = obj.explode("tags")
    materialized = await flat.copy()
    result = await materialized.data()
    assert len(result["user"]) == 3
    assert sorted(result["tags"]) == ["go", "python", "rust"]


async def test_explode_selected_field_copy_type(ctx):
    """copy() on explode + field selection produces correct scalar type, not Array."""
    obj = await create_object_from_value([
        {"user": "Alice", "tags": ["python", "rust"]},
        {"user": "Bob",   "tags": ["go"]},
    ])
    materialized = await obj.explode("tags")["tags"].copy()
    result = await materialized.data()
    assert sorted(result) == ["go", "python", "rust"]
    # Schema should be String (scalar), not Array(String)
    assert materialized._schema.columns["value"].array == 0


# =============================================================================
# Validation errors
# =============================================================================


async def test_explode_requires_dict_object(ctx):
    """explode() raises ValueError on non-dict (array/scalar) Objects."""
    arr = await create_object_from_value([1, 2, 3])
    with pytest.raises(ValueError, match="dict"):
        arr.explode("value")


async def test_explode_requires_columns(ctx):
    """explode() raises ValueError when no columns are specified."""
    obj = await create_object_from_value([{"x": [1, 2]}])
    with pytest.raises(ValueError, match="at least one column"):
        obj.explode()


async def test_explode_nonexistent_column(ctx):
    """explode() raises ValueError if column doesn't exist."""
    obj = await create_object_from_value([{"x": [1, 2]}])
    with pytest.raises(ValueError, match="does not exist"):
        obj.explode("nonexistent")


async def test_explode_non_array_column(ctx):
    """explode() raises ValueError if column is not an Array type."""
    obj = await create_object_from_value([{"x": [1, 2], "y": "hello"}])
    with pytest.raises(ValueError, match="not an Array type"):
        obj.explode("y")
