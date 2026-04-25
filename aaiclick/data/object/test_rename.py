"""
Tests for Object.rename() and tolerant insert with extra source columns.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.data_context import create_object
from aaiclick.data.models import FIELDTYPE_ARRAY, ColumnInfo, Computed, Schema


async def test_rename_basic(ctx):
    """Renamed columns appear under new names in data()."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "old_name": ColumnInfo("String"),
            "keep_me": ColumnInfo("Int32"),
        },
    )
    obj = await create_object(schema)
    ch = obj.ch_client
    await ch.command(f"INSERT INTO {obj.table} (old_name, keep_me) VALUES ('hello', 42)")

    view = obj.rename({"old_name": "new_name"})
    data = await view.data()
    assert "new_name" in data
    assert "old_name" not in data
    assert "keep_me" in data
    assert data["new_name"] == ["hello"]
    assert data["keep_me"] == [42]


async def test_rename_insert_into_target(ctx):
    """Renamed view can be inserted into target with matching column names."""
    # Source has "src_col", target expects "dst_col"
    src_schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "src_col": ColumnInfo("String"),
            "shared": ColumnInfo("Int32"),
        },
    )
    src = await create_object(src_schema)
    ch = src.ch_client
    await ch.command(f"INSERT INTO {src.table} (src_col, shared) VALUES ('alpha', 10)")

    tgt_schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "dst_col": ColumnInfo("String"),
            "shared": ColumnInfo("Int32"),
        },
    )
    tgt = await create_object(tgt_schema)

    renamed = src.rename({"src_col": "dst_col"})
    await tgt.insert(renamed)

    data = await tgt.data()
    assert data["dst_col"] == ["alpha"]
    assert data["shared"] == [10]


async def test_rename_with_computed_columns(ctx):
    """rename() + with_columns() can be chained."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "old_col": ColumnInfo("Int32"),
        },
    )
    obj = await create_object(schema)
    ch = obj.ch_client
    await ch.command(f"INSERT INTO {obj.table} (old_col) VALUES (5)")

    view = obj.rename({"old_col": "val"}).with_columns(
        {
            "doubled": Computed("Int32", "old_col * 2"),
        }
    )
    data = await view.data()
    assert data["val"] == [5]
    assert data["doubled"] == [10]


async def test_rename_collision_raises(ctx):
    """Renaming to an existing non-renamed column name raises."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "col_a": ColumnInfo("String"),
            "col_b": ColumnInfo("String"),
        },
    )
    obj = await create_object(schema)
    with pytest.raises(ValueError, match="collides with existing"):
        obj.rename({"col_a": "col_b"})


async def test_rename_nonexistent_raises(ctx):
    """Renaming a column that doesn't exist raises."""
    obj = await create_object_from_value([1, 2, 3])
    with pytest.raises(ValueError, match="does not exist"):
        obj.rename({"nonexistent": "new_name"})


async def test_rename_aai_id_raises(ctx):
    """Cannot rename aai_id."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "col": ColumnInfo("String"),
        },
    )
    obj = await create_object(schema)
    with pytest.raises(ValueError, match="Cannot rename 'aai_id'"):
        obj.rename({"aai_id": "id"})


async def test_insert_skips_extra_source_columns(ctx):
    """insert() silently skips source columns not present in target."""
    src_schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "shared": ColumnInfo("Int32"),
            "extra_col": ColumnInfo("String"),
        },
    )
    src = await create_object(src_schema)
    ch = src.ch_client
    await ch.command(f"INSERT INTO {src.table} (shared, extra_col) VALUES (99, 'ignored')")

    tgt_schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "shared": ColumnInfo("Int32"),
        },
    )
    tgt = await create_object(tgt_schema)
    await tgt.insert(src)

    data = await tgt.data()
    assert data["shared"] == [99]


async def test_rename_empty_raises(ctx):
    """Empty rename mapping raises."""
    obj = await create_object_from_value([1, 2, 3])
    with pytest.raises(ValueError, match="non-empty"):
        obj.rename({})


async def test_rename_duplicate_new_names_raises(ctx):
    """Duplicate new names in rename mapping raises."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "col_a": ColumnInfo("String"),
            "col_b": ColumnInfo("String"),
        },
    )
    obj = await create_object(schema)
    with pytest.raises(ValueError, match="Duplicate"):
        obj.rename({"col_a": "same", "col_b": "same"})
