"""
Tests for backend compatibility between chdb and clickhouse-connect.
"""

import pytest
import os
from aaiclick.adapter import ChDBSessionAdapter, ClickHouseConnectAdapter, QueryResult


@pytest.mark.asyncio
async def test_query_result_wrapper():
    """Test that QueryResult wrapper works correctly."""
    # Test data
    test_rows = [(1, "a"), (2, "b"), (3, "c")]

    # Create wrapper
    result = QueryResult(test_rows)

    # Verify result_rows property
    assert result.result_rows == test_rows
    assert len(result.result_rows) == 3
    assert result.result_rows[0] == (1, "a")


@pytest.mark.asyncio
async def test_chdb_adapter_basic():
    """Test chdb adapter basic query functionality."""
    # Create adapter with temporary session
    adapter = ChDBSessionAdapter()

    try:
        # Test simple query
        result = await adapter.query("SELECT 1 as num, 'hello' as text")

        # Verify result format
        assert hasattr(result, "result_rows")
        assert len(result.result_rows) == 1
        assert result.result_rows[0] == (1, "hello")

    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_chdb_adapter_create_table():
    """Test chdb adapter table creation and insertion."""
    adapter = ChDBSessionAdapter()

    try:
        # Create table
        await adapter.command("CREATE TABLE IF NOT EXISTS test_table (id UInt32, name String) ENGINE = Memory")

        # Insert data
        await adapter.command("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")

        # Query data
        result = await adapter.query("SELECT * FROM test_table ORDER BY id")

        # Verify results
        assert len(result.result_rows) == 2
        assert result.result_rows[0] == (1, "Alice")
        assert result.result_rows[1] == (2, "Bob")

        # Cleanup
        await adapter.command("DROP TABLE test_table")

    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_chdb_adapter_numeric_types():
    """Test chdb adapter with various numeric types."""
    adapter = ChDBSessionAdapter()

    try:
        # Test integers
        result = await adapter.query("SELECT 42 as int_val, 3.14 as float_val")
        assert len(result.result_rows) == 1
        assert result.result_rows[0][0] == 42
        assert abs(result.result_rows[0][1] - 3.14) < 0.001

        # Test arrays of numbers
        result = await adapter.query("SELECT number FROM numbers(5)")
        assert len(result.result_rows) == 5
        assert result.result_rows[0] == (0,)
        assert result.result_rows[4] == (4,)

    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_chdb_adapter_multiple_columns():
    """Test chdb adapter with multiple columns."""
    adapter = ChDBSessionAdapter()

    try:
        result = await adapter.query(
            "SELECT number, number * 2, number * 3 FROM numbers(3)"
        )

        assert len(result.result_rows) == 3
        assert result.result_rows[0] == (0, 0, 0)
        assert result.result_rows[1] == (1, 2, 3)
        assert result.result_rows[2] == (2, 4, 6)

    finally:
        await adapter.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.getenv("SKIP_CLICKHOUSE_CONNECT_TESTS") == "1",
    reason="clickhouse-connect tests require running ClickHouse server"
)
async def test_clickhouse_connect_adapter_basic():
    """Test clickhouse-connect adapter (requires running server)."""
    from clickhouse_connect import get_async_client

    # Create client
    client = await get_async_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "default"),
    )

    adapter = ClickHouseConnectAdapter(client)

    try:
        # Test simple query
        result = await adapter.query("SELECT 1 as num, 'hello' as text")

        # Verify result format matches chdb
        assert hasattr(result, "result_rows")
        assert len(result.result_rows) == 1
        assert result.result_rows[0] == (1, "hello")

    finally:
        await adapter.close()


@pytest.mark.asyncio
def test_result_format_consistency():
    """Test that both adapters return the same result format structure."""
    # Both should return QueryResult with result_rows property
    test_data = [(1, 2, 3), (4, 5, 6)]

    result1 = QueryResult(test_data)
    result2 = QueryResult(test_data)

    # Both should have identical interface
    assert hasattr(result1, "result_rows")
    assert hasattr(result2, "result_rows")
    assert result1.result_rows == result2.result_rows
