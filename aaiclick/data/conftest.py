"""Pytest fixtures for aaiclick.data tests.

Shared fixtures (``ch_worker_setup``, ``sql_worker_setup``,
``pin_chdb_session``, ``orch_ctx`` family) register globally via the
``aaiclick.testing`` plugin (see ``aaiclick/conftest.py``). This
conftest adds the data-specific ``ctx`` fixture — an ``orch_context``
wrapper that gives data tests SQL-session access (required by
``_get_table_schema``'s registry read path).
"""

import pytest

from aaiclick.orchestration.orch_context import orch_context
from aaiclick.testing import reset_test_state


@pytest.fixture
async def ctx():
    """Function-scoped orch context with per-test CH reset.

    Wraps ``orch_context()`` — a superset of ``data_context()`` that also
    configures the SQL engine ``_get_table_schema`` reads from. Refcount-driven
    cleanup still runs on Object garbage collection; the explicit DROP sweep
    catches any leaked tables so state never crosses test boundaries.
    """
    async with reset_test_state(orch_context()):
        yield
