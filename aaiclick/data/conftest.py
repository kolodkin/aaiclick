"""Pytest fixtures for aaiclick.data tests."""

import pytest

from aaiclick.data.data_context import data_context


@pytest.fixture(scope="session")
async def ctx():
    """Session-scoped data context shared across all data tests.

    Objects are cleaned up via refcounting when they go out of scope,
    so table accumulation is not a concern.
    """
    async with data_context():
        yield
