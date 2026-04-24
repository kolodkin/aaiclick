"""Tests for persistent named objects."""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.data_context import (
    data_context,
    delete_persistent_object,
    delete_persistent_objects,
    list_persistent_objects,
    open_object,
)
from aaiclick.data.data_context.data_context import _validate_persistent_name


async def test_create_persistent_object(ctx):
    """Persistent object has p_ prefix and persistent property."""
    obj = await create_object_from_value([10, 20, 30], name="test_persist_create")
    try:
        assert obj.table == "p_test_persist_create"
        assert obj.persistent is True
        data = await obj.data()
        assert data == [10, 20, 30]
    finally:
        await delete_persistent_object("test_persist_create")


async def test_regular_object_not_persistent(ctx):
    """Regular (unnamed) objects are not persistent."""
    obj = await create_object_from_value([1, 2, 3])
    assert obj.persistent is False
    assert obj.table.startswith("t_")


async def test_open_persistent_object(ctx):
    """Opening an existing persistent object returns correct data."""
    await create_object_from_value(
        {"x": [1, 2, 3], "y": [4, 5, 6]},
        name="test_persist_open",
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


async def test_persistent_survives_context_exit(ctx):
    """Data in a persistent object is accessible from a new context."""
    await create_object_from_value([100, 200], name="test_persist_survive")

    try:
        obj = await open_object("test_persist_survive")
        data = await obj.data()
        assert data == [100, 200]
    finally:
        await delete_persistent_object("test_persist_survive")


async def test_delete_persistent_object(ctx):
    """Deleting a persistent object removes the table."""
    await create_object_from_value([1], name="test_persist_delete")
    await delete_persistent_object("test_persist_delete")

    with pytest.raises(RuntimeError, match="does not exist"):
        await open_object("test_persist_delete")


async def test_list_persistent_objects(ctx):
    """Listing persistent objects returns their names."""
    await create_object_from_value([1], name="test_persist_list_a")
    await create_object_from_value([2], name="test_persist_list_b")
    try:
        names = await list_persistent_objects()
        assert "test_persist_list_a" in names
        assert "test_persist_list_b" in names
    finally:
        await delete_persistent_object("test_persist_list_a")
        await delete_persistent_object("test_persist_list_b")


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


async def test_persistent_append_semantics(ctx):
    """Creating with same name appends data."""
    try:
        await create_object_from_value([1, 2], name="test_persist_append")
        await create_object_from_value([3, 4], name="test_persist_append")

        obj = await open_object("test_persist_append")
        data = await obj.data()
        assert sorted(data) == [1, 2, 3, 4]
    finally:
        await delete_persistent_object("test_persist_append")


async def test_open_nonexistent_raises(ctx):
    """Opening a non-existent persistent object raises RuntimeError."""
    with pytest.raises(RuntimeError, match="does not exist"):
        await open_object("this_does_not_exist_xyz")


async def test_persistent_dict_object(ctx):
    """Persistent objects work with dict values."""
    try:
        obj = await create_object_from_value(
            {"name": ["Alice", "Bob"], "age": [30, 25]},
            name="test_persist_dict",
        )
        assert obj.persistent is True

        opened = await open_object("test_persist_dict")
        data = await opened.data()
        assert len(data) == 2
    finally:
        await delete_persistent_object("test_persist_dict")


async def test_persistent_scalar_object(ctx):
    """Persistent objects work with scalar values."""
    try:
        obj = await create_object_from_value(42, name="test_persist_scalar")
        assert obj.persistent is True
        data = await obj.data()
        assert data == 42
    finally:
        await delete_persistent_object("test_persist_scalar")


async def test_delete_persistent_objects_requires_time_filter(ctx):
    """Calling delete_persistent_objects without after or before raises ValueError."""
    with pytest.raises(ValueError, match="At least one of"):
        await delete_persistent_objects()


async def test_scope_global_explicit_creates_p_prefix(ctx):
    """scope='global' forces the ``p_`` prefix even outside orch."""
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


async def test_scope_job_outside_orch_raises(ctx):
    """scope='job' without an orch job_id raises a helpful error."""
    with pytest.raises(ValueError, match="scope='job' requires a job_id"):
        await create_object_from_value(
            [1, 2, 3],
            name="needs_orch",
            scope="job",
        )


async def test_scope_without_name_raises(ctx):
    """Passing scope without name is an API misuse."""
    with pytest.raises(ValueError, match="scope can only be set together with name"):
        await create_object_from_value([1, 2, 3], scope="global")


async def test_object_scope_property_for_temp(ctx):
    obj = await create_object_from_value([1, 2, 3])
    assert obj.scope == "temp"
    assert obj.persistent is False
