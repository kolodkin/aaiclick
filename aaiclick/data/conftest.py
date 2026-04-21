"""Pytest fixtures for aaiclick.data tests.

Shared fixtures (``ch_worker_setup``, ``sql_worker_setup``,
``pin_chdb_session``, ``orch_ctx`` family) register globally via the
``aaiclick.testing`` plugin (see ``aaiclick/conftest.py``). This
conftest only adds the data-specific ``ctx`` fixture — data tests use
``data_context()`` directly, not ``orch_context``.
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
