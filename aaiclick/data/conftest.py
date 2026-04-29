"""Pytest fixtures for aaiclick.data tests.

Shared fixtures (``ch_worker_setup``, ``sql_worker_setup``,
``pin_chdb_session``, ``orch_ctx`` family) register globally via the
``aaiclick.testing`` plugin (see ``aaiclick/conftest.py``). This
conftest adds the data-specific ``ctx`` fixture — an ``orch_context``
+ ``task_scope`` wrapper that gives data tests SQL-session access
(required by ``_get_table_schema``'s registry read path) and an
``TaskLifecycleHandler`` that writes ``table_registry.schema_doc``
on every ``create_object``.
"""

import pytest

from aaiclick.data.data_context.data_context import _engine_var
from aaiclick.data.models import ENGINE_MEMORY
from aaiclick.orchestration.orch_context import orch_context, task_scope
from aaiclick.snowflake import get_snowflake_id
from aaiclick.testing import reset_test_state


@pytest.fixture
async def ctx():
    """Function-scoped orch + task context with per-test CH reset.

    Wraps ``orch_context() + task_scope()`` so data tests get:
    - SQL engine (``_get_table_schema`` reads ``table_registry.schema_doc``)
    - ``TaskLifecycleHandler`` (``create_object`` writes ``schema_doc``)

    The orch context defaults the table engine to MergeTree; data tests
    expect the historical Memory default, so we override it here. Synthetic
    task_id/job_id/run_id are minted per test so multiple tests don't share
    lifecycle state. Refcount-driven cleanup still runs on Object garbage
    collection; the explicit DROP sweep catches any leaked tables so state
    never crosses test boundaries.
    """
    async with reset_test_state(orch_context()):
        synthetic_id = get_snowflake_id()
        async with task_scope(task_id=synthetic_id, job_id=synthetic_id, run_id=synthetic_id):
            engine_token = _engine_var.set(ENGINE_MEMORY)
            try:
                yield
            finally:
                _engine_var.reset(engine_token)
