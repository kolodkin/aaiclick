"""
Tests for ClickHouse adapter.
"""

import pytest
import os
from aaiclick.adapter import ClickHouseConnectAdapter, QueryResult


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

        # Verify result format
        assert hasattr(result, "result_rows")
        assert len(result.result_rows) == 1
        assert result.result_rows[0] == (1, "hello")

    finally:
        await adapter.close()


def test_result_format_consistency():
    """Test that QueryResult returns consistent format structure."""
    # Should return QueryResult with result_rows property
    test_data = [(1, 2, 3), (4, 5, 6)]

    result1 = QueryResult(test_data)
    result2 = QueryResult(test_data)

    # Both should have identical interface
    assert hasattr(result1, "result_rows")
    assert hasattr(result2, "result_rows")
    assert result1.result_rows == result2.result_rows
