"""Tests for persistent named objects."""

import pytest

from aaiclick import create_object, create_object_from_value, Schema, FIELDTYPE_ARRAY
from aaiclick.data.data_context import (
    data_context,
    delete_persistent_object,
    get_ch_client,
    list_persistent_objects,
    open_object,
    _validate_persistent_name,
)


async def test_create_persistent_object():
    """Persistent object has p_ prefix and persistent property."""
    async with data_context():
        obj = await create_object_from_value([10, 20, 30], name="test_persist_create")
        try:
            assert obj.table == "p_test_persist_create"
            assert obj.persistent is True
            data = await obj.data()
            assert data == [10, 20, 30]
        finally:
            await delete_persistent_object("test_persist_create")


async def test_regular_object_not_persistent():
    """Regular (unnamed) objects are not persistent."""
    async with data_context():
        obj = await create_object_from_value([1, 2, 3])
        assert obj.persistent is False
        assert obj.table.startswith("t_")


async def test_open_persistent_object():
    """Opening an existing persistent object returns correct data."""
    async with data_context():
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


async def test_persistent_survives_context_exit():
    """Data in a persistent object is accessible from a new context."""
    async with data_context():
        await create_object_from_value([100, 200], name="test_persist_survive")

    async with data_context():
        try:
            obj = await open_object("test_persist_survive")
            data = await obj.data()
            assert data == [100, 200]
        finally:
            await delete_persistent_object("test_persist_survive")


async def test_delete_persistent_object():
    """Deleting a persistent object removes the table."""
    async with data_context():
        await create_object_from_value([1], name="test_persist_delete")
        await delete_persistent_object("test_persist_delete")

        with pytest.raises(RuntimeError, match="does not exist"):
            await open_object("test_persist_delete")


async def test_list_persistent_objects():
    """Listing persistent objects returns their names."""
    async with data_context():
        await create_object_from_value([1], name="test_persist_list_a")
        await create_object_from_value([2], name="test_persist_list_b")
        try:
            names = await list_persistent_objects()
            assert "test_persist_list_a" in names
            assert "test_persist_list_b" in names
        finally:
            await delete_persistent_object("test_persist_list_a")
            await delete_persistent_object("test_persist_list_b")


async def test_persistent_name_validation():
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


async def test_persistent_append_semantics():
    """Creating with same name appends data."""
    async with data_context():
        try:
            await create_object_from_value([1, 2], name="test_persist_append")
            await create_object_from_value([3, 4], name="test_persist_append")

            obj = await open_object("test_persist_append")
            data = await obj.data()
            assert sorted(data) == [1, 2, 3, 4]
        finally:
            await delete_persistent_object("test_persist_append")


async def test_open_nonexistent_raises():
    """Opening a non-existent persistent object raises RuntimeError."""
    async with data_context():
        with pytest.raises(RuntimeError, match="does not exist"):
            await open_object("this_does_not_exist_xyz")


async def test_persistent_dict_object():
    """Persistent objects work with dict values."""
    async with data_context():
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


async def test_persistent_scalar_object():
    """Persistent objects work with scalar values."""
    async with data_context():
        try:
            obj = await create_object_from_value(42, name="test_persist_scalar")
            assert obj.persistent is True
            data = await obj.data()
            assert data == 42
        finally:
            await delete_persistent_object("test_persist_scalar")
