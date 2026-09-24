"""Tests for the data_context manager: DDL/registry persistence,
automatic Object cleanup, and stale-object guarantees after context exit.
"""

from __future__ import annotations

import pytest

from aaiclick import (
    create_object_from_value,
)
from aaiclick.data.data_context import delete_object, get_ch_client, open_object
from aaiclick.data.data_context.lifecycle import get_data_lifecycle
from aaiclick.data.models import FIELDTYPE_ARRAY

# DDL and registry persistence


async def test_create_object_emits_no_aai_id_column(ctx):
    obj = await create_object_from_value([1.0, 2.0, 3.0])
    ch_client = get_ch_client()
    result = await ch_client.query(f"SELECT name FROM system.columns WHERE table = '{obj.table}' ORDER BY position")
    names = [r[0] for r in result.result_rows]
    assert "aai_id" not in names
    assert names == ["value"]


async def test_create_object_emits_no_comment_clauses(ctx):
    obj = await create_object_from_value([1, 2, 3])
    ch_client = get_ch_client()
    result = await ch_client.query(f"SELECT name, comment FROM system.columns WHERE table = '{obj.table}'")
    for name, comment in result.result_rows:
        assert comment == "", f"column {name} has unexpected comment {comment!r}"


async def test_create_object_schema_survives_reopen(ctx):
    """The registry's ``schema_doc`` is what ``open_object`` rebuilds the schema from."""
    await create_object_from_value([1, 2, 3], name="reopened", scope="global")
    # Registry write goes through the DBLifecycleHandler queue; flush so the
    # INSERT has committed before we read.
    lifecycle = get_data_lifecycle()
    assert lifecycle is not None
    await lifecycle.flush()

    schema = (await open_object("reopened", scope="global")).schema

    assert schema.fieldtype == FIELDTYPE_ARRAY
    assert list(schema.columns) == ["value"]
    assert schema.columns["value"].fieldtype == FIELDTYPE_ARRAY


async def test_create_object_allows_user_column_named_aai_id(ctx):
    """aai_id is no longer reserved — users can define a column with that name."""
    obj = await create_object_from_value({"aai_id": [1, 2], "label": ["a", "b"]})
    ch_client = get_ch_client()
    result = await ch_client.query(f"SELECT name FROM system.columns WHERE table = '{obj.table}'")
    names = {r[0] for r in result.result_rows}
    assert "aai_id" in names
    assert "label" in names


# Context-manager lifecycle (objects become stale after context exits)


async def test_context_object_stale_flag(ctx):
    """Test that stale flag is set correctly."""
    obj = await create_object_from_value([1, 2, 3])
    assert not obj.stale

    await delete_object(obj)
    assert obj.stale


# Stale-object guards


async def test_stale_object_prevents_data_access(ctx):
    """Test that stale objects prevent database access."""
    obj = await create_object_from_value([1, 2, 3])
    await delete_object(obj)

    # Object is now stale, should raise RuntimeError
    assert obj.stale

    with pytest.raises(RuntimeError, match="Cannot use stale Object"):
        await obj.data()


async def test_stale_object_prevents_operators(ctx):
    """Test that stale objects prevent operator usage."""
    obj1 = await create_object_from_value([1, 2, 3])
    obj2 = await create_object_from_value([4, 5, 6])

    await delete_object(obj1)
    assert obj1.stale

    # Attempting to use operators on stale object should raise
    with pytest.raises(RuntimeError, match="Cannot use stale Object"):
        await (obj1 + obj2)


async def test_stale_object_prevents_aggregates(ctx):
    """Test that stale objects prevent aggregate methods."""
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    await delete_object(obj)

    assert obj.stale

    # Test various aggregate methods
    with pytest.raises(RuntimeError, match="Cannot use stale Object"):
        await obj.min()

    with pytest.raises(RuntimeError, match="Cannot use stale Object"):
        await obj.max()

    with pytest.raises(RuntimeError, match="Cannot use stale Object"):
        await obj.sum()

    with pytest.raises(RuntimeError, match="Cannot use stale Object"):
        await obj.mean()

    with pytest.raises(RuntimeError, match="Cannot use stale Object"):
        await obj.std()


async def test_stale_object_prevents_copy(ctx):
    obj = await create_object_from_value([1, 2, 3])
    await delete_object(obj)

    with pytest.raises(RuntimeError, match="Cannot use stale Object"):
        await obj.copy()


async def test_stale_object_prevents_concat(ctx):
    obj1 = await create_object_from_value([1, 2, 3])
    obj2 = await create_object_from_value([4, 5, 6])
    await delete_object(obj1)

    with pytest.raises(RuntimeError, match="Cannot use stale Object"):
        await obj1.concat(obj2)


async def test_stale_object_prevents_insert(ctx):
    obj1 = await create_object_from_value([1, 2, 3])
    obj2 = await create_object_from_value([4, 5, 6])
    await delete_object(obj1)

    with pytest.raises(RuntimeError, match="Cannot use stale Object"):
        await obj1.insert(obj2)


async def test_stale_object_allows_property_access(ctx):
    """Test that stale objects still allow property access."""
    obj = await create_object_from_value([1, 2, 3])
    table_name = obj.table

    await delete_object(obj)

    # Properties should still be accessible
    assert obj.stale
    assert obj.table == table_name
    assert repr(obj) == f"Object(table='{table_name}')"
