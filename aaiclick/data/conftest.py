"""Pytest fixtures for aaiclick.data tests."""

import pytest

from aaiclick.data.data_context import data_context
from aaiclick.test_utils import reset_test_state


@pytest.fixture
async def ctx():
    """Function-scoped data context with per-test CH reset.

    Refcount-driven cleanup still runs on Object garbage collection; the
    explicit DROP sweep catches any leaked tables so state never crosses
    test boundaries.
    """
    async with reset_test_state(data_context()):
        yield
