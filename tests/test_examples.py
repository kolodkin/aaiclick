"""
Tests for examples to ensure they run without errors.
"""

import pytest
import os


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.getenv("SKIP_CLICKHOUSE_TESTS") == "1",
    reason="Example tests require running ClickHouse server"
)
async def test_basic_operators_example():
    """Test that the basic_operators example runs successfully."""
    from aaiclick.examples.basic_operators import main

    # Run the example - should complete without errors
    await main()
