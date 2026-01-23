"""
Pytest configuration for aaiclick tests.

This module provides test fixtures. ClickHouse setup is handled by
scripts/setup_and_test.py or manually via docker-compose.
"""

import asyncio

import pytest

from aaiclick import DataContext, create_object, create_object_from_value, get_context
from aaiclick.orchestration.context import OrchContext


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


@pytest.fixture
async def orch_ctx():
    """
    Fixture that provides an OrchContext for orchestration tests.

    The engine is automatically disposed when the context exits.

    Usage:
        async def test_example(orch_ctx):
            job = await create_job("my_job", "mymodule.task1")
            # Engine is automatically cleaned up
    """
    async with OrchContext() as context:
        yield context


@pytest.fixture(autouse=True)
async def cleanup_postgres_pool():
    """
    Reset PostgreSQL connection pool after each test.

    This ensures tests don't interfere with each other through
    lingering connections or operations in the shared asyncpg pool.

    The asyncpg pool is global and persists across tests, so we need
    to explicitly close and reset it to avoid "operation is in progress" errors.
    """
    yield
    # Clean up after test
    from aaiclick.orchestration.context import _reset_postgres_pool

    await _reset_postgres_pool()
