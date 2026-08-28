"""
Tests for Object.rename() and tolerant insert with extra source columns.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.data_context import create_object
from aaiclick.data.models import FIELDTYPE_ARRAY, FIELDTYPE_DICT, ColumnInfo, Computed, Schema


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
        fieldtype=FIELDTYPE_DICT,
        columns={
            "src_col": ColumnInfo("String", fieldtype=FIELDTYPE_ARRAY),
            "shared": ColumnInfo("Int32", fieldtype=FIELDTYPE_ARRAY),
        },
    )
    src = await create_object(src_schema)
    ch = src.ch_client
    await ch.command(f"INSERT INTO {src.table} (src_col, shared) VALUES ('alpha', 10)")

    tgt_schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "dst_col": ColumnInfo("String", fieldtype=FIELDTYPE_ARRAY),
            "shared": ColumnInfo("Int32", fieldtype=FIELDTYPE_ARRAY),
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
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    with pytest.raises(ValueError, match="does not exist"):
        obj.rename({"nonexistent": "new_name"})


async def test_insert_skips_extra_source_columns(ctx):
    """insert() silently skips source columns not present in target."""
    src_schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "shared": ColumnInfo("Int32", fieldtype=FIELDTYPE_ARRAY),
            "extra_col": ColumnInfo("String", fieldtype=FIELDTYPE_ARRAY),
        },
    )
    src = await create_object(src_schema)
    ch = src.ch_client
    await ch.command(f"INSERT INTO {src.table} (shared, extra_col) VALUES (99, 'ignored')")

    tgt_schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "shared": ColumnInfo("Int32", fieldtype=FIELDTYPE_ARRAY),
        },
    )
    tgt = await create_object(tgt_schema)
    await tgt.insert(src)

    data = await tgt.data()
    assert data["shared"] == [99]


async def test_rename_empty_raises(ctx):
    """Empty rename mapping raises."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
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


async def test_rename_swap_raises(ctx):
    """Regression: a rename whose target reuses an existing column name
    being renamed away (e.g. ``{"genres": "genres_str", "genres_array":
    "genres"}``) must raise rather than silently produce wrong values.

    Without this check, ClickHouse's alias resolution makes ``SELECT
    genres AS genres_str, genres_array AS genres FROM t`` ambiguous —
    the alias ``genres`` shadows the original column, so both items
    resolve to the same expression. Users who genuinely want to swap
    names should rename in two steps via an intermediate name."""
    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "title": ColumnInfo("String", fieldtype=FIELDTYPE_ARRAY),
            "genres": ColumnInfo("String", fieldtype=FIELDTYPE_ARRAY),
            "genres_array": ColumnInfo("String", array=1, fieldtype=FIELDTYPE_ARRAY),
        },
    )
    obj = await create_object(schema)
    with pytest.raises(ValueError, match="reuses a name being renamed away"):
        obj.rename({"genres": "genres_str", "genres_array": "genres"})


async def test_copy_after_rename_uses_new_names(ctx):
    """Regression: copy() on a renamed View must use post-rename names in
    both the destination schema and the INSERT/SELECT.

    Before the fix, ``_get_copy_info`` built ``columns`` from the original
    schema while the inner SELECT already aliased ``old AS new`` — so
    ``copy_db`` produced ``INSERT INTO new (orig) SELECT orig FROM (...
    orig AS new ...)``, which fails with ClickHouse Code 47 because the
    inner subquery no longer exposes ``orig``."""
    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "title": ColumnInfo("String", fieldtype=FIELDTYPE_ARRAY),
            "genres_array": ColumnInfo("String", array=1, fieldtype=FIELDTYPE_ARRAY),
        },
    )
    obj = await create_object(schema)
    ch = obj.ch_client
    await ch.command(f"INSERT INTO {obj.table} (title, genres_array) VALUES ('Movie', ['Action', 'Drama'])")

    renamed = obj.rename({"genres_array": "genres"})
    copied = await renamed.copy()

    assert set(copied._schema.columns.keys()) == {"title", "genres"}
    data = await copied.data()
    assert data["title"] == ["Movie"]
    assert data["genres"] == [["Action", "Drama"]]


async def test_select_columns_after_rename(ctx):
    """Regression: ``View.__getitem__`` on a renamed View must accept the
    post-rename names.

    Before the fix, ``_effective_columns`` looked up selected_fields in
    the original ``_schema.columns`` (KeyError on the new name) and
    ``_build_select`` emitted the new name as a bare column ref against
    the underlying table (ClickHouse Code 47). The fix inverts the rename
    mapping at lookup/emission so post-rename names project from their
    source columns via aliases."""
    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "title": ColumnInfo("String", fieldtype=FIELDTYPE_ARRAY),
            "genres_array": ColumnInfo("String", array=1, fieldtype=FIELDTYPE_ARRAY),
        },
    )
    obj = await create_object(schema)
    ch = obj.ch_client
    await ch.command(f"INSERT INTO {obj.table} (title, genres_array) VALUES ('Movie', ['Action', 'Drama'])")

    renamed = obj.rename({"genres_array": "genres"})
    sub = renamed[["title", "genres"]]
    data = await sub.data()

    assert set(data.keys()) == {"title", "genres"}
    assert data["title"] == ["Movie"]
    assert data["genres"] == [["Action", "Drama"]]


async def test_rename_after_column_selection(ctx):
    """Regression: rename() chained after a field selection must apply.

    ``selected_fields`` hold post-rename names, but a rename applied *after*
    a selection left them at their pre-rename spelling — so the rename was
    silently ignored and ``data()`` still returned the old column name."""
    obj = await create_object_from_value({"a": [1, 2, 3], "b": [10, 20, 30]})

    data = await obj[["a", "b"]].rename({"b": "c"}).data()

    assert sorted(data) == ["a", "c"]
    assert data["c"] == [10, 20, 30]


async def test_copy_after_rename_following_selection(ctx):
    """Regression: the same pre-rename ``selected_fields`` made ``copy()``
    raise ``KeyError`` on the old name — ``_get_copy_info`` keys its columns
    by the post-rename name while the selection still asked for the old one."""
    obj = await create_object_from_value({"a": [1, 2, 3], "b": [10, 20, 30]})

    copied = await obj[["a", "b"]].rename({"b": "c"}).copy()

    assert sorted(copied._schema.columns) == ["a", "c"]
    data = await copied.data()
    assert data["c"] == [10, 20, 30]


async def test_rename_after_selection_keeps_earlier_rename(ctx):
    """A rename chained after a selection must compose with the View's
    existing renames rather than replacing them."""
    obj = await create_object_from_value({"a": [1, 2], "b": [10, 20], "c": [100, 200]})

    view = obj.rename({"a": "x"})[["x", "b"]].rename({"b": "y"})
    data = await view.data()

    assert sorted(data) == ["x", "y"]
    assert data["x"] == [1, 2]
    assert data["y"] == [10, 20]


async def test_rename_rejects_alias_already_used_by_an_earlier_rename(ctx):
    """Composed renames must not alias two columns to the same name — that
    emits ``a AS x, c AS x``, which ClickHouse rejects as an ambiguous alias."""
    obj = await create_object_from_value({"a": [1, 2], "b": [10, 20], "c": [100, 200]})

    with pytest.raises(ValueError, match="collides with a rename already applied"):
        obj.rename({"a": "x"}).rename({"c": "x"})
