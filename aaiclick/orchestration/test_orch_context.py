"""Tests for ``orch_context`` nesting behavior.

The engine + handler are constructed only at the outermost call;
nested calls reuse them so SQLAlchemy's connection pool actually
gets to amortize handshakes across calls instead of being torn
down per scope.
"""

from __future__ import annotations

import importlib
from unittest.mock import patch

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from aaiclick.data.data_context.ch_client import _ch_client_var
from aaiclick.orchestration.execution.db_handler import _db_handler_var
from aaiclick.orchestration.orch_context import orch_context
from aaiclick.orchestration.sql_context import _sql_engine_var

# `aaiclick.orchestration.__init__` re-exports the function ``orch_context``
# from the submodule, so attribute-walk based patch paths
# (``"aaiclick.orchestration.orch_context.X"``) hit the function not the
# module. Resolve via ``importlib`` to get the module object directly.
_orch_module = importlib.import_module("aaiclick.orchestration.orch_context")


async def test_nested_orch_context_reuses_outer_engine():
    with patch.object(_orch_module, "create_async_engine", wraps=create_async_engine) as spy:
        async with orch_context(with_ch=False):
            outer_engine = _sql_engine_var.get()
            outer_handler = _db_handler_var.get()
            assert spy.call_count == 1

            async with orch_context(with_ch=False):
                assert _sql_engine_var.get() is outer_engine
                assert _db_handler_var.get() is outer_handler
                assert spy.call_count == 1

                async with orch_context(with_ch=False):
                    assert _sql_engine_var.get() is outer_engine
                    assert spy.call_count == 1


async def test_nested_orch_context_does_not_dispose_outer_engine():
    with patch.object(AsyncEngine, "dispose", autospec=True) as dispose_spy:
        async with orch_context(with_ch=False):
            async with orch_context(with_ch=False):
                pass
            assert dispose_spy.call_count == 0
        assert dispose_spy.call_count == 1


async def test_nested_orch_context_reuses_ch_client():
    """ChClient was already nest-aware; this guards the existing behavior."""
    async with orch_context(with_ch=True):
        outer_ch = _ch_client_var.get()
        async with orch_context(with_ch=True):
            assert _ch_client_var.get() is outer_ch


async def test_inner_with_ch_true_reuses_outer_with_ch_false_branch():
    async with orch_context(with_ch=False):
        assert _ch_client_var.get() is None
        async with orch_context(with_ch=True):
            assert _ch_client_var.get() is not None
        assert _ch_client_var.get() is None
