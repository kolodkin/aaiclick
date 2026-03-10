"""
Pytest configuration for aaiclick tests.

This module provides test fixtures. ClickHouse setup is handled by
scripts/setup_and_test.py or manually via docker-compose.
"""

import asyncio

import pytest

from aaiclick.data.data_context import data_context
from aaiclick.orchestration.context import orch_context


@pytest.fixture(scope="session")
def event_loop():
    """
    Create an event loop for async tests.
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def ctx():
    """
    Session-scoped data context shared across all tests in a worker.

    Objects are cleaned up via refcounting when they go out of scope,
    so table accumulation is not a concern.
    """
    async with data_context():
        yield


@pytest.fixture
async def orch_ctx():
    """
    Function-scoped orch context for orchestration tests.

    Cannot be session-scoped: SQLAlchemy async sessions don't safely
    share across tests (concurrent operation and event loop issues).
    """
    async with orch_context():
        yield
