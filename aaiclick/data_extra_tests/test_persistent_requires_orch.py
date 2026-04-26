"""Persistent named objects are an orch-only feature.

``data_context()`` does not provide the SQL session that
``table_registry`` needs, so any attempt to create a named/persistent
object from inside a bare ``data_context()`` block must raise
immediately — not silently fall back to a temp table or fail later
during ``open_object()``.

Lives in ``data_extra_tests/`` because the assertion needs an explicit
``async with data_context():`` block (no shared ``ctx`` orch fixture).
"""

from __future__ import annotations

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context


async def test_named_object_in_data_context_raises():
    """``name=...`` outside orch_context raises RuntimeError."""
    async with data_context():
        with pytest.raises(RuntimeError, match="orch_context"):
            await create_object_from_value([1, 2, 3], name="should_not_persist")


async def test_scope_global_in_data_context_raises():
    """Explicit ``scope='global'`` outside orch_context raises RuntimeError."""
    async with data_context():
        with pytest.raises(RuntimeError, match="orch_context"):
            await create_object_from_value([1, 2, 3], name="should_not_persist", scope="global")


async def test_scope_job_in_data_context_raises():
    """Explicit ``scope='job'`` outside orch_context raises RuntimeError."""
    async with data_context():
        with pytest.raises(RuntimeError, match="orch_context"):
            await create_object_from_value([1, 2, 3], name="should_not_persist", scope="job")


async def test_unnamed_object_in_data_context_works():
    """No ``name=`` → temp table is fine in data_context (regression guard)."""
    async with data_context():
        obj = await create_object_from_value([1, 2, 3])
        assert obj.table.startswith("t_")
        assert obj.persistent is False
        assert sorted(await obj.data()) == [1, 2, 3]
