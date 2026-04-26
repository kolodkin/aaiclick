"""Tests for persistent named objects.

Most tests use the ``ctx`` fixture (orch_context + task_scope) — that is
the only environment in which persistent named objects are supported.
A small section at the end opens a bare ``data_context()`` block to
verify that attempting persistence **without** orch_context raises.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.data_context import (
    data_context,
    delete_persistent_object,
    delete_persistent_objects,
    get_data_lifecycle,
    open_object,
)
from aaiclick.data.data_context.data_context import _validate_persistent_name


async def test_create_persistent_object_table_prefix(ctx):
    """``scope='global'`` named objects use the ``p_`` table prefix."""
    obj = await create_object_from_value([10, 20, 30], name="test_persist_create", scope="global")
    try:
        assert obj.table == "p_test_persist_create"
        assert obj.persistent is True
        assert await obj.data() == [10, 20, 30]
    finally:
        await delete_persistent_object("test_persist_create")


async def test_regular_object_not_persistent(ctx):
    """Unnamed objects use ``t_`` and are not persistent."""
    obj = await create_object_from_value([1, 2, 3])
    assert obj.persistent is False
    assert obj.table.startswith("t_")


async def test_open_persistent_object_in_same_context(ctx):
    """``open_object`` round-trips schema and data within one context."""
    await create_object_from_value(
        {"x": [1, 2, 3], "y": [4, 5, 6]},
        name="test_persist_open",
        scope="global",
    )
    try:
        opened = await open_object("test_persist_open")
        assert opened.table == "p_test_persist_open"
        assert opened.persistent is True
        data = await opened.data()
        assert data["x"] == [1, 2, 3]
        assert data["y"] == [4, 5, 6]
    finally:
        await delete_persistent_object("test_persist_open")


async def test_delete_persistent_object_then_open_raises(ctx):
    """After deletion, ``open_object`` raises RuntimeError."""
    await create_object_from_value([1], name="test_persist_delete", scope="global")
    await delete_persistent_object("test_persist_delete")

    with pytest.raises(RuntimeError, match="does not exist"):
        await open_object("test_persist_delete")


async def test_persistent_name_validation(ctx):
    """Invalid names raise ValueError."""
    with pytest.raises(ValueError, match="Invalid persistent name"):
        _validate_persistent_name("123bad")
    with pytest.raises(ValueError, match="Invalid persistent name"):
        _validate_persistent_name("has space")
    with pytest.raises(ValueError, match="Invalid persistent name"):
        _validate_persistent_name("has-dash")
    _validate_persistent_name("valid_name")
    _validate_persistent_name("_underscore")
    _validate_persistent_name("CamelCase")


async def test_open_nonexistent_raises(ctx):
    """Opening a non-existent persistent object raises RuntimeError."""
    with pytest.raises(RuntimeError, match="does not exist"):
        await open_object("this_does_not_exist_xyz")


async def test_delete_persistent_objects_requires_time_filter(ctx):
    """Calling delete_persistent_objects without after or before raises ValueError."""
    with pytest.raises(ValueError, match="At least one of"):
        await delete_persistent_objects()


async def test_scope_job_creates_j_prefix_with_active_job_id(ctx):
    """``scope='job'`` with an active orch job_id yields ``j_<job_id>_<name>``.

    The ``ctx`` fixture wraps ``orch_context`` + ``task_scope`` and sets
    ``current_job_id`` to a synthetic snowflake; we read it back via
    ``get_data_lifecycle().current_job_id()`` and assert the table name.
    """
    lifecycle = get_data_lifecycle()
    assert lifecycle is not None
    job_id = lifecycle.current_job_id()
    assert job_id is not None

    obj = await create_object_from_value(
        [1, 2, 3],
        name="test_scope_job_explicit",
        scope="job",
    )
    assert obj.table == f"j_{job_id}_test_scope_job_explicit"
    assert obj.scope == "job"
    assert obj.persistent is True


async def test_scope_global_explicit_creates_p_prefix(ctx):
    """``scope='global'`` forces the ``p_`` prefix even when an orch job
    is active (which would otherwise default named objects to ``j_<id>_``)."""
    obj = await create_object_from_value(
        [1, 2, 3],
        name="test_scope_global_explicit",
        scope="global",
    )
    try:
        assert obj.table == "p_test_scope_global_explicit"
        assert obj.scope == "global"
        assert obj.persistent is True
    finally:
        await delete_persistent_object("test_scope_global_explicit")


async def test_scope_without_name_raises(ctx):
    """Passing ``scope`` without a name is an API misuse."""
    with pytest.raises(ValueError, match="scope can only be set together with name"):
        await create_object_from_value([1, 2, 3], scope="global")


async def test_object_scope_property_for_temp(ctx):
    """Unnamed objects report ``scope == "temp"``."""
    obj = await create_object_from_value([1, 2, 3])
    assert obj.scope == "temp"
    assert obj.persistent is False


# --- Persistent attempts outside orch_context are rejected ---


async def test_named_object_in_bare_data_context_raises():
    """``name=...`` outside orch_context raises RuntimeError.

    These tests open their own ``async with data_context():`` block
    (no ``ctx`` fixture) so the persistence helpers can verify the
    "no SQL session → can't write table_registry" rejection path.
    """
    async with data_context():
        with pytest.raises(RuntimeError, match="orch_context"):
            await create_object_from_value([1, 2, 3], name="should_not_persist")


async def test_scope_global_in_bare_data_context_raises():
    async with data_context():
        with pytest.raises(RuntimeError, match="orch_context"):
            await create_object_from_value([1, 2, 3], name="x", scope="global")


async def test_scope_job_in_bare_data_context_raises():
    async with data_context():
        with pytest.raises(RuntimeError, match="orch_context"):
            await create_object_from_value([1, 2, 3], name="x", scope="job")


async def test_unnamed_object_in_bare_data_context_works():
    """No ``name=`` → temp table is fine in bare ``data_context()``."""
    async with data_context():
        obj = await create_object_from_value([1, 2, 3])
        assert obj.table.startswith("t_")
        assert obj.persistent is False
        assert sorted(await obj.data()) == [1, 2, 3]
