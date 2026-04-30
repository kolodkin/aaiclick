"""Bare ``data_context()`` accepts temp_named, rejects persistent scopes.

Persistent tables (``scope="global"``/``"job"``, ``open_object()``) live
in orchestration — they need the SQL ``table_registry`` row that only
``OrchLifecycleHandler`` writes. ``scope="temp_named"`` (the default when
``name=`` is set) is a temp table tagged with a name and works anywhere.
The behavioural coverage of persistent tables under ``orch_context()``
lives in ``aaiclick/orchestration/test_persistent.py``.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context


async def test_named_object_in_bare_data_context_is_temp_named():
    async with data_context():
        obj = await create_object_from_value([1, 2, 3], name="staging")
        assert obj.scope == "temp_named"
        assert obj.persistent is False
        assert obj.table.startswith("t_staging_")
        assert await obj.data() == [1, 2, 3]


async def test_scope_temp_named_in_bare_data_context():
    async with data_context():
        obj = await create_object_from_value([1, 2, 3], name="explicit", scope="temp_named")
        assert obj.scope == "temp_named"
        assert obj.table.startswith("t_explicit_")


async def test_temp_named_collisions_allowed_in_same_context():
    """Same ``name`` twice in one context yields two distinct tables."""
    async with data_context():
        a = await create_object_from_value([1], name="dup")
        b = await create_object_from_value([2], name="dup")
        assert a.table != b.table
        assert a.table.startswith("t_dup_")
        assert b.table.startswith("t_dup_")


async def test_scope_global_in_bare_data_context_raises():
    async with data_context():
        with pytest.raises(RuntimeError, match="orch_context"):
            await create_object_from_value([1, 2, 3], name="x", scope="global")


async def test_scope_job_in_bare_data_context_raises():
    async with data_context():
        with pytest.raises(RuntimeError, match="orch_context"):
            await create_object_from_value([1, 2, 3], name="x", scope="job")
