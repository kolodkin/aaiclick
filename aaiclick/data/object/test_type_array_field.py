"""
Tests for array field support - creating Objects from list-of-dicts (records format)
where fields can contain arrays stored as ClickHouse Array(T) columns.
"""

import pytest

from aaiclick import ORIENT_DICT, ORIENT_RECORDS, create_object_from_value

# =============================================================================
# Creation Tests
# =============================================================================


@pytest.mark.parametrize(
    "records, expected",
    [
        # Some fields are arrays, some are scalars.
        pytest.param(
            [{"a": [1, 2, 3], "b": 10}, {"a": [4, 5, 6], "b": 20}],
            {"a": [[1, 2, 3], [4, 5, 6]], "b": [10, 20]},
            id="array-and-scalar-fields",
        ),
        pytest.param(
            [{"x": [1, 2], "y": [3, 4]}, {"x": [5, 6], "y": [7, 8]}],
            {"x": [[1, 2], [5, 6]], "y": [[3, 4], [7, 8]]},
            id="all-array-fields",
        ),
        # No Array columns at all.
        pytest.param(
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
            {"name": ["Alice", "Bob"], "age": [30, 25]},
            id="all-scalar-fields",
        ),
        pytest.param([{"a": [1, 2, 3], "b": 42}], {"a": [[1, 2, 3]], "b": [42]}, id="single-record"),
    ],
)
async def test_records_creation(ctx, records, expected):
    obj = await create_object_from_value(records)

    data = await obj.data()

    assert data == expected


# =============================================================================
# Orient Tests
# =============================================================================


async def test_records_orient_dict(ctx):
    """data() with orient=ORIENT_DICT returns dict of lists."""
    obj = await create_object_from_value(
        [
            {"a": [1, 2], "b": 10},
            {"a": [3, 4], "b": 20},
        ]
    )

    data = await obj.data(orient=ORIENT_DICT)

    assert isinstance(data, dict)
    assert data["a"] == [[1, 2], [3, 4]]
    assert data["b"] == [10, 20]


async def test_records_orient_records(ctx):
    """data() with orient=ORIENT_RECORDS returns list of dicts."""
    obj = await create_object_from_value(
        [
            {"a": [1, 2], "b": 10},
            {"a": [3, 4], "b": 20},
        ]
    )

    data = await obj.data(orient=ORIENT_RECORDS)

    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0] == {"a": [1, 2], "b": 10}
    assert data[1] == {"a": [3, 4], "b": 20}


# =============================================================================
# Variable Length Arrays
# =============================================================================


@pytest.mark.parametrize(
    "records",
    [
        # Different records can have arrays of different lengths.
        pytest.param(
            [{"a": [1, 2, 3], "b": 10}, {"a": [4, 5], "b": 20}, {"a": [6], "b": 30}],
            id="different-array-lengths",
        ),
        pytest.param([{"a": [], "b": 10}, {"a": [1, 2], "b": 20}], id="empty-array-field"),
    ],
)
async def test_records_variable_length_arrays(ctx, records):
    obj = await create_object_from_value(records)

    data = await obj.data(orient=ORIENT_RECORDS)

    assert data == records


# =============================================================================
# Type Inference Tests
# =============================================================================


@pytest.mark.parametrize(
    "column, values, expected_type",
    [
        pytest.param("values", [1, 2, 3], "Int64", id="int"),
        pytest.param("values", [1.5, 2.5, 3.5], "Float64", id="float"),
        pytest.param("tags", ["hello", "world"], "String", id="string"),
    ],
)
async def test_records_array_field_type_inference(ctx, column, values, expected_type):
    """An array field infers Array(<element type>) from its values."""
    obj = await create_object_from_value([{column: values}])

    schema = obj.schema
    assert schema.columns[column].type == expected_type
    assert schema.columns[column].array == 1


# =============================================================================
# Validation Tests
# =============================================================================


async def test_records_inconsistent_keys_raises(ctx):
    """Records with different keys should raise ValueError."""
    with pytest.raises(ValueError, match="identical keys"):
        await create_object_from_value(
            [
                {"a": [1, 2], "b": 10},
                {"a": [3, 4], "c": 20},
            ]
        )


# =============================================================================
# Metadata Tests
# =============================================================================


async def test_records_schema_fieldtype(ctx):
    """schema reports correct fieldtype for records."""
    obj = await create_object_from_value(
        [
            {"a": [1, 2], "b": 10},
            {"a": [3, 4], "b": 20},
        ]
    )

    schema = obj.schema

    assert schema.fieldtype == "d"
    assert "a" in schema.columns
    assert "b" in schema.columns
    assert schema.columns["a"].array == 1
    assert schema.columns["b"].type == "Int64"
    assert schema.columns["b"].array == 0
