"""Pytest fixtures for aaiclick.data tests.

Session-scoped worker-isolation and chdb pin fixtures register globally
via the ``aaiclick.testing`` plugin (see ``aaiclick/conftest.py``).
This conftest adds the data-specific ``ctx`` fixture that enters
``data_context()`` per function — data tests don't use ``orch_context``.
"""

import pytest

from aaiclick.data.data_context import data_context
from aaiclick.testing import reset_test_state


@pytest.fixture
async def ctx():
    """Function-scoped data context with per-test CH reset.

    Refcount-driven cleanup still runs on Object garbage collection; the
    explicit DROP sweep catches any leaked tables so state never crosses
    test boundaries.
    """
    async with reset_test_state(data_context()):
        yield
