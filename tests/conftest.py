"""
Pytest configuration for aaiclick tests.

This module provides test fixtures. ClickHouse setup is handled by
scripts/setup_and_test.py or manually via docker-compose.
"""

import asyncio

import pytest

from aaiclick import DataContext, create_object, create_object_from_value, get_context


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


@pytest.fixture(autouse=True)
async def reset_postgres_pool():
    """
    Reset PostgreSQL connection pool and SQLAlchemy engine after each test.

    This ensures tests don't interfere with each other through
    shared connection pool state.
    """
    yield
    # Clean up after test using reset methods from pool-related files
    from aaiclick.orchestration.context import reset_orch_context_engine
    from aaiclick.orchestration.database import reset_postgres_pool

    await reset_postgres_pool()
    await reset_orch_context_engine()
