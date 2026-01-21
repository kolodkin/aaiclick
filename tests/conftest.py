"""
Pytest configuration for aaiclick tests.

This module provides test fixtures. ClickHouse setup is handled by
scripts/setup_and_test.py or manually via docker-compose.
"""

import asyncio

import pytest

from aaiclick import DataContext, get_context, create_object_from_value, create_object


@pytest.fixture(scope="session")
def event_loop():
    """
    Create an event loop for async tests.
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def ctx():
    """
    Fixture that provides a DataContext for tests.

    Usage:
        async def test_example(ctx):
            obj = await create_object_from_value([1, 2, 3])
            # Tables are automatically cleaned up
    """
    async with DataContext() as context:
        yield context
