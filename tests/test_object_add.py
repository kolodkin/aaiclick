"""
Tests for Object addition operation.
"""

import pytest
from aaiclick import create_object_from_value, get_client


@pytest.mark.asyncio
async def test_object_add_simple():
    """Test basic element-wise addition of two objects."""
    # Create two objects with simple values
    obj_a = await create_object_from_value([10.0, 20.0, 30.0])
    obj_b = await create_object_from_value([5.0, 10.0, 15.0])

    # Perform addition
    result = await (obj_a + obj_b)

    # Verify result
    client = await get_client()
    query_result = await client.query(f"SELECT value FROM {result.table} ORDER BY value")
    rows = query_result.result_rows

    # Expected: element-wise addition (10+5, 20+10, 30+15) -> (15, 30, 45)
    assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
    assert rows[0][0] == 15.0, f"Expected 15.0, got {rows[0][0]}"
    assert rows[1][0] == 30.0, f"Expected 30.0, got {rows[1][0]}"
    assert rows[2][0] == 45.0, f"Expected 45.0, got {rows[2][0]}"

    # Cleanup
    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()


@pytest.mark.asyncio
async def test_object_add_integers():
    """Test element-wise addition with integer values."""
    obj_a = await create_object_from_value([1, 2, 3])
    obj_b = await create_object_from_value([10, 20, 30])

    result = await (obj_a + obj_b)

    client = await get_client()
    query_result = await client.query(f"SELECT value FROM {result.table} ORDER BY value")
    rows = query_result.result_rows

    # Expected: element-wise (1+10, 2+20, 3+30) -> (11, 22, 33)
    assert len(rows) == 3
    assert rows[0][0] == 11
    assert rows[1][0] == 22
    assert rows[2][0] == 33

    # Cleanup
    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()


@pytest.mark.asyncio
async def test_object_add_single_values():
    """Test addition with single value objects."""
    obj_a = await create_object_from_value([100.0])
    obj_b = await create_object_from_value([50.0])

    result = await (obj_a + obj_b)

    client = await get_client()
    query_result = await client.query(f"SELECT value FROM {result.table}")
    rows = query_result.result_rows

    assert len(rows) == 1
    assert rows[0][0] == 150.0

    # Cleanup
    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()


@pytest.mark.asyncio
async def test_object_add_result_table_name():
    """Test that result table exists and has Snowflake ID."""
    obj_a = await create_object_from_value([1.0])
    obj_b = await create_object_from_value([2.0])

    result = await (obj_a + obj_b)

    # Check that result table name starts with 't' followed by Snowflake ID
    assert result.table.startswith('t'), f"Expected table name to start with 't', got {result.table}"
    assert result.table[1:].isdigit(), f"Expected numeric Snowflake ID after 't', got {result.table}"

    # Check that table exists
    client = await get_client()
    query_result = await client.query(f"EXISTS TABLE {result.table}")
    assert query_result.result_rows[0][0] == 1  # Table exists

    # Cleanup
    await obj_a.delete_table()
    await obj_b.delete_table()
    await result.delete_table()


@pytest.mark.asyncio
async def test_object_add_chain():
    """Test chaining multiple additions."""
    obj_a = await create_object_from_value([1.0])
    obj_b = await create_object_from_value([2.0])
    obj_c = await create_object_from_value([3.0])

    # Chain additions: (a + b) + c
    temp = await (obj_a + obj_b)
    result = await (temp + obj_c)

    client = await get_client()
    query_result = await client.query(f"SELECT value FROM {result.table}")
    rows = query_result.result_rows

    # Result should be 1 + 2 + 3 = 6
    assert len(rows) == 1
    assert rows[0][0] == 6.0

    # Cleanup
    await obj_a.delete_table()
    await obj_b.delete_table()
    await obj_c.delete_table()
    await temp.delete_table()
    await result.delete_table()
