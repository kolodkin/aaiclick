"""
Tests for Object.data() method with different data types and orient options.
"""

from aaiclick import (
    create_object_from_value,
    ORIENT_DICT,
    ORIENT_RECORDS,
)


async def test_data_scalar():
    """Test data() returns scalar value directly."""
    obj = await create_object_from_value(42.0)

    data = await obj.data()

    assert data == 42.0

    await obj.delete_table()


async def test_data_scalar_integer():
    """Test data() returns integer scalar directly."""
    obj = await create_object_from_value(123)

    data = await obj.data()

    assert data == 123

    await obj.delete_table()


async def test_data_scalar_string():
    """Test data() returns string scalar directly."""
    obj = await create_object_from_value("hello")

    data = await obj.data()

    assert data == "hello"

    await obj.delete_table()


async def test_data_array():
    """Test data() returns list for array."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])

    data = await obj.data()

    assert data == [1, 2, 3, 4, 5]

    await obj.delete_table()


async def test_data_array_floats():
    """Test data() returns list of floats for float array."""
    obj = await create_object_from_value([1.5, 2.5, 3.5])

    data = await obj.data()

    assert data == [1.5, 2.5, 3.5]

    await obj.delete_table()


async def test_data_dict_scalar():
    """Test data() returns dict for dict_scalar (single row)."""
    obj = await create_object_from_value({"id": 1, "name": "Alice", "age": 30})

    # Default orient='dict' returns single dict
    data = await obj.data()

    assert isinstance(data, dict)
    assert data["id"] == 1
    assert data["name"] == "Alice"
    assert data["age"] == 30

    await obj.delete_table()


async def test_data_dict_orient_dict():
    """Test data() with orient=ORIENT_DICT returns dict."""
    obj = await create_object_from_value({"x": 10, "y": 20})

    data = await obj.data(orient=ORIENT_DICT)

    assert isinstance(data, dict)
    assert data == {"x": 10, "y": 20}

    await obj.delete_table()


async def test_data_dict_orient_records():
    """Test data() with orient=ORIENT_RECORDS returns list of dicts."""
    obj = await create_object_from_value({"x": 10, "y": 20})

    data = await obj.data(orient=ORIENT_RECORDS)

    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0] == {"x": 10, "y": 20}

    await obj.delete_table()


async def test_data_after_addition():
    """Test data() returns correct value after scalar addition."""
    a = await create_object_from_value(100.0)
    b = await create_object_from_value(50.0)

    result = await (a + b)
    data = await result.data()

    assert data == 150.0

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()


async def test_data_array_after_addition():
    """Test data() returns correct list after array addition."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])

    result = await (a + b)
    data = await result.data()

    assert data == [11, 22, 33]

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()
