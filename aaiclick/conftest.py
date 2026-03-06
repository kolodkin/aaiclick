"""
Pytest configuration for aaiclick tests.

This module provides test fixtures. ClickHouse setup is handled by
scripts/setup_and_test.py or manually via docker-compose.
"""

import asyncio
import os

import pytest

from aaiclick import DataContext, create_object, create_object_from_value, get_data_context
from aaiclick.orchestration.context import OrchContext


def pytest_collection_modifyitems(config, items):
    if os.getenv("AAICLICK_URL_TEST_ENABLE"):
        return
    skip_url = pytest.mark.skip(reason="set AAICLICK_URL_TEST_ENABLE=1 to run")
    for item in items:
        if "url" in item.keywords:
            item.add_marker(skip_url)


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
    Session-scoped DataContext shared across all tests in a worker.

    Objects are cleaned up via refcounting when they go out of scope,
    so table accumulation is not a concern.
    """
    async with DataContext() as context:
        yield context


@pytest.fixture
async def orch_ctx():
    """
    Function-scoped OrchContext for orchestration tests.

    Cannot be session-scoped: SQLAlchemy async sessions don't safely
    share across tests (concurrent operation and event loop issues).
    """
    async with OrchContext() as context:
        yield context
