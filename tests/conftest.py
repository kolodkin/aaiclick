"""
Pytest configuration for aaiclick tests.

This module provides test fixtures. ClickHouse setup is handled by
scripts/setup_and_test.py or manually via docker-compose.
"""

import asyncio

import pytest

from aaiclick import Context, get_ch_client


@pytest.fixture(scope="session")
def event_loop():
    """
    Create an event loop for async tests.
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def cleanup_tables():
    """
    Fixture to track and cleanup test tables after each test.
    """
    tables_to_cleanup = []

    def register_table(table_name):
        tables_to_cleanup.append(table_name)

    yield register_table

    # Cleanup after test
    if tables_to_cleanup:
        ch_client = await get_ch_client()
        for table in tables_to_cleanup:
            try:
                await ch_client.command(f"DROP TABLE IF EXISTS {table}")
            except Exception as e:
                print(f"Warning: Failed to drop table {table}: {e}")


@pytest.fixture
async def ctx():
    """
    Fixture that provides a Context for tests.

    Usage:
        async def test_example(ctx):
            obj = await ctx.create_object_from_value([1, 2, 3])
            # Tables are automatically cleaned up
    """
    async with Context() as context:
        yield context
