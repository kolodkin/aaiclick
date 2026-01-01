"""
Tests for dict data type - creation, data() with orient options.

Dict type stores multiple named columns in a single row.
"""

from aaiclick import create_object_from_value, ORIENT_DICT, ORIENT_RECORDS


# =============================================================================
# Dict Creation Tests
# =============================================================================

async def test_dict_creation_simple():
    """Test creating a dict object with simple values."""
    obj = await create_object_from_value({"id": 1, "name": "Alice", "age": 30})

    data = await obj.data()

    assert isinstance(data, dict)
    assert data["id"] == 1
    assert data["name"] == "Alice"
    assert data["age"] == 30

    await obj.delete_table()


async def test_dict_creation_mixed_types():
    """Test creating a dict with mixed value types."""
    obj = await create_object_from_value({
        "count": 42,
        "price": 19.99,
        "name": "item"
    })

    data = await obj.data()

    assert data["count"] == 42
    assert data["price"] == 19.99
    assert data["name"] == "item"

    await obj.delete_table()


async def test_dict_creation_all_int():
    """Test creating a dict with all integer values."""
    obj = await create_object_from_value({"x": 10, "y": 20, "z": 30})

    data = await obj.data()

    assert data == {"x": 10, "y": 20, "z": 30}

    await obj.delete_table()


async def test_dict_creation_all_float():
    """Test creating a dict with all float values."""
    obj = await create_object_from_value({"a": 1.1, "b": 2.2, "c": 3.3})

    data = await obj.data()

    assert data == {"a": 1.1, "b": 2.2, "c": 3.3}

    await obj.delete_table()


async def test_dict_creation_all_string():
    """Test creating a dict with all string values."""
    obj = await create_object_from_value({
        "first": "hello",
        "second": "world",
        "third": "test"
    })

    data = await obj.data()

    assert data["first"] == "hello"
    assert data["second"] == "world"
    assert data["third"] == "test"

    await obj.delete_table()


# =============================================================================
# Orient Options Tests
# =============================================================================

async def test_dict_orient_dict():
    """Test data() with orient=ORIENT_DICT returns dict."""
    obj = await create_object_from_value({"x": 10, "y": 20})

    data = await obj.data(orient=ORIENT_DICT)

    assert isinstance(data, dict)
    assert data == {"x": 10, "y": 20}

    await obj.delete_table()


async def test_dict_orient_records():
    """Test data() with orient=ORIENT_RECORDS returns list of dicts."""
    obj = await create_object_from_value({"x": 10, "y": 20})

    data = await obj.data(orient=ORIENT_RECORDS)

    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0] == {"x": 10, "y": 20}

    await obj.delete_table()


async def test_dict_default_orient_is_dict():
    """Test that default orient is ORIENT_DICT."""
    obj = await create_object_from_value({"a": 1, "b": 2})

    data_default = await obj.data()
    data_explicit = await obj.data(orient=ORIENT_DICT)

    assert data_default == data_explicit

    await obj.delete_table()


# =============================================================================
# Edge Cases
# =============================================================================

async def test_dict_single_field():
    """Test dict with a single field."""
    obj = await create_object_from_value({"only": 42})

    data = await obj.data()

    assert data == {"only": 42}

    await obj.delete_table()


async def test_dict_with_empty_string():
    """Test dict containing empty string value."""
    obj = await create_object_from_value({"name": "", "value": 123})

    data = await obj.data()

    assert data["name"] == ""
    assert data["value"] == 123

    await obj.delete_table()


async def test_dict_with_zero_values():
    """Test dict containing zero values."""
    obj = await create_object_from_value({"zero_int": 0, "zero_float": 0.0})

    data = await obj.data()

    assert data["zero_int"] == 0
    assert data["zero_float"] == 0.0

    await obj.delete_table()


# =============================================================================
# Dict of Arrays Tests
# =============================================================================
# NOTE: Dict of arrays requires implementation in factories.py
# The following tests are placeholders for when the feature is implemented.
# Current implementation stores arrays as string representations.
# TODO: Implement proper array handling in create_object_from_value for dicts
