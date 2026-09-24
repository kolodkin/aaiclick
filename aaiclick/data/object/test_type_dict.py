"""
Tests for dict data type - creation, data() with orient options.

Dict type stores multiple named columns in a single row.
"""

import pytest

from aaiclick import ORIENT_DICT, ORIENT_RECORDS, create_object_from_value

# =============================================================================
# Dict Creation Tests
# =============================================================================


@pytest.mark.parametrize(
    "value",
    [
        pytest.param({"id": 1, "name": "Alice", "age": 30}, id="simple"),
        pytest.param({"count": 42, "price": 19.99, "name": "item"}, id="mixed-types"),
        pytest.param({"x": 10, "y": 20, "z": 30}, id="all-int"),
        pytest.param({"a": 1.1, "b": 2.2, "c": 3.3}, id="all-float"),
        pytest.param({"first": "hello", "second": "world", "third": "test"}, id="all-string"),
        pytest.param({"only": 42}, id="single-field"),
        pytest.param({"name": "", "value": 123}, id="empty-string"),
        pytest.param({"zero_int": 0, "zero_float": 0.0}, id="zero-values"),
    ],
)
async def test_dict_creation(ctx, value):
    obj = await create_object_from_value(value)

    data = await obj.data()

    assert data == value


# =============================================================================
# Orient Options Tests
# =============================================================================


async def test_dict_orient_dict(ctx):
    """Test data() with orient=ORIENT_DICT returns dict."""
    obj = await create_object_from_value({"x": 10, "y": 20})

    data = await obj.data(orient=ORIENT_DICT)

    assert isinstance(data, dict)
    assert data == {"x": 10, "y": 20}


async def test_dict_orient_records(ctx):
    """Test data() with orient=ORIENT_RECORDS returns list of dicts."""
    obj = await create_object_from_value({"x": 10, "y": 20})

    data = await obj.data(orient=ORIENT_RECORDS)

    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0] == {"x": 10, "y": 20}


async def test_dict_default_orient_is_dict(ctx):
    """Test that default orient is ORIENT_DICT."""
    obj = await create_object_from_value({"a": 1, "b": 2})

    data_default = await obj.data()
    data_explicit = await obj.data(orient=ORIENT_DICT)

    assert data_default == data_explicit


# =============================================================================
# Dict of Arrays Tests
# =============================================================================


@pytest.mark.parametrize(
    "value",
    [
        pytest.param({"id": [1, 2, 3], "value": [10, 20, 30]}, id="int-arrays"),
        pytest.param({"x": [1.5, 2.5, 3.5], "y": [4.5, 5.5, 6.5]}, id="float-arrays"),
        pytest.param({"first": ["John", "Jane"], "last": ["Doe", "Smith"]}, id="string-arrays"),
    ],
)
async def test_dict_of_arrays_creation(ctx, value):
    obj = await create_object_from_value(value)

    data = await obj.data()

    assert data == value


async def test_dict_of_arrays_orient_dict(ctx):
    """Test dict of arrays with orient=ORIENT_DICT returns dict with arrays."""
    obj = await create_object_from_value({"name": ["Alice", "Bob"], "age": [30, 25]})

    data = await obj.data(orient=ORIENT_DICT)

    assert isinstance(data, dict)
    assert data["name"] == ["Alice", "Bob"]
    assert data["age"] == [30, 25]


async def test_dict_of_arrays_orient_records(ctx):
    """Test dict of arrays with orient=ORIENT_RECORDS returns list of row dicts."""
    obj = await create_object_from_value({"name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35]})

    data = await obj.data(orient=ORIENT_RECORDS)

    assert isinstance(data, list)
    assert len(data) == 3
    assert data[0] == {"name": "Alice", "age": 30}
    assert data[1] == {"name": "Bob", "age": 25}
    assert data[2] == {"name": "Charlie", "age": 35}


async def test_dict_of_arrays_records_preserves_order(ctx):
    """Test that orient=ORIENT_RECORDS preserves array order."""
    obj = await create_object_from_value({"letter": ["z", "a", "m"], "number": [3, 1, 2]})

    data = await obj.data(orient=ORIENT_RECORDS)

    # Order should match original array order
    assert data[0] == {"letter": "z", "number": 3}
    assert data[1] == {"letter": "a", "number": 1}
    assert data[2] == {"letter": "m", "number": 2}


# =============================================================================
# Mixed scalar/list dicts — single record with Array(T) columns
# =============================================================================


async def test_dict_mixed_scalar_and_list(ctx):
    """A dict mixing scalars and lists is one record; lists become Array columns."""
    obj = await create_object_from_value({"id": 1, "tags": ["a", "b"]})

    schema = obj.schema
    assert schema.columns["id"].type == "Int64"
    assert int(schema.columns["id"].array) == 0
    assert schema.columns["tags"].type == "String"
    assert int(schema.columns["tags"].array) == 1

    assert await obj.data() == {"id": 1, "tags": ["a", "b"]}


async def test_dict_mixed_scalar_and_empty_list(ctx):
    """An empty list in a mixed dict round-trips as an empty Array."""
    obj = await create_object_from_value({"id": 1, "tags": []})

    assert await obj.data() == {"id": 1, "tags": []}


async def test_dict_mixed_scalar_list_and_nested_list(ctx):
    """Nested scalar lists in a mixed dict become Array(Array(T))."""
    obj = await create_object_from_value({"id": 1, "grid": [[1, 2], [3]]})

    assert int(obj.schema.columns["grid"].array) == 2
    assert await obj.data() == {"id": 1, "grid": [[1, 2], [3]]}
