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

    Usage:
        async def test_example(orch_ctx):
            job = await create_job("my_job", "mymodule.task1")
    """
    async with OrchContext() as context:
        yield context


@pytest.fixture(autouse=True)
async def cleanup_engine():
    """
    Reset SQLAlchemy engine after each test to ensure isolation.

    The global engine persists across tests, and asyncpg connections in the pool
    retain protocol state (prepared statements, Futures, transactions) that goes
    beyond what SQLAlchemy's pool_reset_on_return can clean. Disposing the engine
    ensures each test gets fresh connections with clean asyncpg protocol state.

    Without this, tests fail with:
    - "InterfaceError: another operation is in progress" (stale transactions)
    - "RuntimeError: Future attached to different loop" (stale asyncpg Futures)
    """
    yield
    # Clean up after test
    from aaiclick.orchestration.context import _reset_engine

    await _reset_engine()
