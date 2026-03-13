"""
Tests for nested array support - creating Objects from dicts/records
containing nested list-of-dicts, exploded into rows with dot-star column notation.

Example: {a: 2, b: [{c: [1,2,3], d: 5}, {c: [4,5,6], d: 10}]}
Explodes to 2 rows with columns: a (Int64), b.*.c (Array(Int64)), b.*.d (Int64)
"""

import pytest

from aaiclick import ORIENT_DICT, ORIENT_RECORDS, create_object_from_value


# =============================================================================
# Single Record with Nested Arrays
# =============================================================================


async def test_nested_single_record(ctx):
    """Single dict with nested list-of-dicts explodes into multiple rows."""
    obj = await create_object_from_value({
        "a": 2,
        "b": [{"c": [1, 2, 3], "d": 5}, {"c": [4, 5, 6], "d": 10}],
    })

    data = await obj.data(orient=ORIENT_RECORDS)

    assert len(data) == 2
    assert data[0] == {"a": 2, "b.*.c": [1, 2, 3], "b.*.d": 5}
    assert data[1] == {"a": 2, "b.*.c": [4, 5, 6], "b.*.d": 10}


async def test_nested_single_record_schema(ctx):
    """Schema uses dot-star notation with no nested Array wrapping."""
    obj = await create_object_from_value({
        "a": 2,
        "b": [{"c": [1, 2, 3], "d": 5}],
    })

    schema = obj.schema
    assert "a" in schema.columns
    assert "b.*.c" in schema.columns
    assert "b.*.d" in schema.columns
    assert schema.columns["a"].type == "Int64"
    assert schema.columns["b.*.c"].type == "Int64"
    assert schema.columns["b.*.c"].array is True
    assert schema.columns["b.*.d"].type == "Int64"
    assert not schema.columns["b.*.d"].array


async def test_nested_single_record_orient_dict(ctx):
    """Orient dict returns column-oriented output."""
    obj = await create_object_from_value({
        "a": 2,
        "b": [{"c": [1, 2, 3], "d": 5}, {"c": [4, 5, 6], "d": 10}],
    })

    data = await obj.data(orient=ORIENT_DICT)

    assert data["a"] == [2, 2]
    assert data["b.*.c"] == [[1, 2, 3], [4, 5, 6]]
    assert data["b.*.d"] == [5, 10]


# =============================================================================
# Multiple Records with Nested Arrays
# =============================================================================


async def test_nested_multiple_records(ctx):
    """List of dicts with nested list-of-dicts, each exploded into rows."""
    obj = await create_object_from_value([
        {"a": 2, "b": [{"c": [1, 2, 3], "d": 5}, {"c": [4, 5, 6], "d": 10}]},
        {"a": 3, "b": [{"c": [7, 8, 9], "d": 15}]},
    ])

    data = await obj.data(orient=ORIENT_RECORDS)

    assert len(data) == 3
    assert data[0] == {"a": 2, "b.*.c": [1, 2, 3], "b.*.d": 5}
    assert data[1] == {"a": 2, "b.*.c": [4, 5, 6], "b.*.d": 10}
    assert data[2] == {"a": 3, "b.*.c": [7, 8, 9], "b.*.d": 15}


async def test_nested_records_orient_dict(ctx):
    """Orient dict transposes exploded rows into column arrays."""
    obj = await create_object_from_value([
        {"a": 1, "b": [{"x": 10}]},
        {"a": 2, "b": [{"x": 20}]},
    ])

    data = await obj.data(orient=ORIENT_DICT)

    assert data["a"] == [1, 2]
    assert data["b.*.x"] == [10, 20]


# =============================================================================
# Nested with Scalar-Only Sub-Fields
# =============================================================================


async def test_nested_scalar_sub_fields(ctx):
    """Nested objects with only scalar fields become plain columns."""
    obj = await create_object_from_value({
        "name": "test",
        "items": [{"x": 1, "y": 2}, {"x": 3, "y": 4}],
    })

    data = await obj.data(orient=ORIENT_RECORDS)

    assert len(data) == 2
    assert data[0] == {"name": "test", "items.*.x": 1, "items.*.y": 2}
    assert data[1] == {"name": "test", "items.*.x": 3, "items.*.y": 4}


# =============================================================================
# Nested with Array Sub-Fields
# =============================================================================


async def test_nested_array_sub_fields(ctx):
    """Nested objects with array fields stored as Array(T), not Array(Array(T))."""
    obj = await create_object_from_value({
        "id": 1,
        "groups": [
            {"tags": ["a", "b"], "score": 10},
            {"tags": ["c"], "score": 20},
        ],
    })

    data = await obj.data(orient=ORIENT_RECORDS)

    assert len(data) == 2
    assert data[0] == {"id": 1, "groups.*.tags": ["a", "b"], "groups.*.score": 10}
    assert data[1] == {"id": 1, "groups.*.tags": ["c"], "groups.*.score": 20}


# =============================================================================
# Edge Cases
# =============================================================================


async def test_nested_single_element_array(ctx):
    """Nested array with a single element produces one row."""
    obj = await create_object_from_value({
        "a": 1,
        "b": [{"c": 10}],
    })

    data = await obj.data(orient=ORIENT_RECORDS)

    assert len(data) == 1
    assert data[0] == {"a": 1, "b.*.c": 10}


async def test_nested_many_elements(ctx):
    """Nested array with many elements produces many rows."""
    items = [{"val": i, "doubled": i * 2} for i in range(10)]
    obj = await create_object_from_value({"data": items})

    data = await obj.data(orient=ORIENT_RECORDS)

    assert len(data) == 10
    for i, row in enumerate(data):
        assert row["data.*.val"] == i
        assert row["data.*.doubled"] == i * 2


# =============================================================================
# Validation
# =============================================================================


async def test_nested_records_inconsistent_keys_raises(ctx):
    """Records with different keys should raise ValueError."""
    with pytest.raises(ValueError, match="identical keys"):
        await create_object_from_value([
            {"a": 1, "b": [{"x": 10}]},
            {"a": 2, "c": [{"x": 20}]},
        ])


# =============================================================================
# Deep Nesting (Two Levels)
# =============================================================================


async def test_deep_nested_two_levels(ctx):
    """Two levels of nested list-of-dicts explode into cross-product rows."""
    obj = await create_object_from_value({
        "root": 1,
        "level1": [
            {
                "name": "first",
                "level2": [{"val": 10}, {"val": 20}],
            },
            {
                "name": "second",
                "level2": [{"val": 30}],
            },
        ],
    })

    data = await obj.data(orient=ORIENT_RECORDS)

    assert len(data) == 3
    assert data[0] == {"root": 1, "level1.*.name": "first", "level1.*.level2.*.val": 10}
    assert data[1] == {"root": 1, "level1.*.name": "first", "level1.*.level2.*.val": 20}
    assert data[2] == {"root": 1, "level1.*.name": "second", "level1.*.level2.*.val": 30}
