"""Tests for nullable column support."""

import pytest

from aaiclick import (
    ColumnInfo,
    Schema,
    create_object,
    create_object_from_value,
    FIELDTYPE_ARRAY,
    FIELDTYPE_SCALAR,
)
from aaiclick.data.data_context import get_ch_client
from aaiclick.data.models import parse_ch_type


# --- ColumnInfo and parse_ch_type utility tests ---


def test_column_def_ch_type_non_nullable():
    cd = ColumnInfo("Int64")
    assert cd.ch_type() == "Int64"
    assert cd.nullable is False


def test_column_def_ch_type_nullable():
    cd = ColumnInfo("Int64", nullable=True)
    assert cd.ch_type() == "Nullable(Int64)"
    assert cd.nullable is True


def test_column_def_frozen():
    cd = ColumnInfo("Int64")
    with pytest.raises(AttributeError):
        cd.type = "String"


def test_parse_ch_type_plain():
    cd = parse_ch_type("Int64")
    assert cd.type == "Int64"
    assert cd.nullable is False


def test_parse_ch_type_nullable():
    cd = parse_ch_type("Nullable(Int64)")
    assert cd.type == "Int64"
    assert cd.nullable is True


def test_parse_ch_type_string():
    cd = parse_ch_type("String")
    assert cd.type == "String"
    assert cd.nullable is False


def test_parse_ch_type_nullable_string():
    cd = parse_ch_type("Nullable(String)")
    assert cd.type == "String"
    assert cd.nullable is True


# --- LowCardinality support ---


def test_column_def_low_cardinality():
    cd = ColumnInfo("String", low_cardinality=True)
    assert cd.ch_type() == "LowCardinality(String)"
    assert cd.low_cardinality is True


def test_column_def_low_cardinality_nullable():
    cd = ColumnInfo("String", nullable=True, low_cardinality=True)
    assert cd.ch_type() == "LowCardinality(Nullable(String))"


def test_parse_ch_type_low_cardinality():
    cd = parse_ch_type("LowCardinality(String)")
    assert cd.type == "String"
    assert cd.nullable is False
    assert cd.low_cardinality is True


def test_parse_ch_type_low_cardinality_nullable():
    cd = parse_ch_type("LowCardinality(Nullable(String))")
    assert cd.type == "String"
    assert cd.nullable is True
    assert cd.low_cardinality is True


# --- Table creation with nullable columns ---


async def test_create_nullable_column(ctx):
    """Create a table with a nullable column and insert NULL."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()

    # Insert rows including NULL
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (1), (NULL), (3)")

    data = await obj.data()
    assert len(data) == 3
    assert None in data


async def test_create_nullable_scalar(ctx):
    """Create a scalar nullable column with NULL value."""
    schema = Schema(
        fieldtype=FIELDTYPE_SCALAR,
        col_fieldtype=FIELDTYPE_SCALAR,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("String", nullable=True),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()

    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (NULL)")

    data = await obj.data()
    assert data is None


async def test_aai_id_nullable_raises(ctx):
    """aai_id column must never be nullable."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64", nullable=True),
            "value": ColumnInfo("Int64"),
        },
    )
    with pytest.raises(ValueError, match="aai_id column cannot be nullable"):
        await create_object(schema)


async def test_nullable_dict_columns(ctx):
    """Create dict object with mixed nullable and non-nullable columns."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "name": ColumnInfo("String"),
            "score": ColumnInfo("Float64", nullable=True),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()

    await ch.command(
        f"INSERT INTO {obj.table} (name, score) VALUES "
        f"('alice', 95.0), ('bob', NULL), ('carol', 88.5)"
    )

    data = await obj.data()
    assert data["name"] == ["alice", "bob", "carol"]
    assert None in data["score"]


# --- Metadata ---


async def test_schema_shows_nullable(ctx):
    """metadata() should report nullable=True for nullable columns."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)

    schema = obj.schema
    assert schema.columns["value"].nullable is True
    assert schema.columns["value"].type == "Int64"
    assert schema.columns["aai_id"].nullable is False


# --- Operator NULL propagation ---


async def test_nullable_add_propagates_null(ctx):
    """Binary operators propagate NULL: NULL + 5 = NULL."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (1), (NULL), (3)")

    result = await (obj + 10)
    data = await result.data()
    assert data[0] == 11
    assert data[1] is None
    assert data[2] == 13


async def test_nullable_comparison_returns_nullable(ctx):
    """Comparison with NULL returns NULL, not True/False."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (1), (NULL), (3)")

    result = await (obj > 2)
    data = await result.data()
    assert data[0] == 0  # 1 > 2 is false
    assert data[1] is None  # NULL > 2 is NULL
    assert data[2] == 1  # 3 > 2 is true


# --- Aggregation skips NULLs ---


async def test_nullable_sum_skips_nulls(ctx):
    """sum() skips NULL values."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (1), (NULL), (3)")

    total = await obj.sum()
    assert await total.data() == 4


async def test_nullable_count_counts_all_rows(ctx):
    """count() counts all rows (including NULLs) per ClickHouse semantics."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (1), (NULL), (3)")

    cnt = await obj.count()
    # count() uses count() without args, so it counts all rows
    assert await cnt.data() == 3


async def test_nullable_mean_skips_nulls(ctx):
    """mean() computes average of non-NULL values."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (2), (NULL), (4)")

    avg = await obj.mean()
    assert await avg.data() == 3.0


# --- Null-specific operations ---


async def test_is_null(ctx):
    """is_null() returns 1 for NULL, 0 otherwise."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (1), (NULL), (3)")

    mask = await obj.is_null()
    assert await mask.data() == [0, 1, 0]


async def test_is_not_null(ctx):
    """is_not_null() returns 1 for non-NULL, 0 otherwise."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (1), (NULL), (3)")

    mask = await obj.is_not_null()
    assert await mask.data() == [1, 0, 1]


async def test_coalesce_with_scalar(ctx):
    """coalesce() replaces NULLs with a scalar fallback."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (1), (NULL), (3)")

    filled = await obj.coalesce(0)
    data = await filled.data()
    assert data == [1, 0, 3]


async def test_coalesce_with_object(ctx):
    """coalesce() uses values from another Object as fallback."""
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)
    ch = get_ch_client()
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (1), (NULL), (3)")

    fallback = await create_object_from_value([10, 20, 30])
    filled = await obj.coalesce(fallback)
    data = await filled.data()
    assert data == [1, 20, 3]


# --- Concat with nullable ---


async def test_concat_nullable_with_nonnullable(ctx):
    """Concat of nullable and non-nullable promotes result to nullable."""
    from aaiclick.data.object import Object

    schema_nullable = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj_a = await create_object(schema_nullable)
    ch = get_ch_client()
    await ch.command(f"INSERT INTO {obj_a.table} (value) VALUES (1), (NULL)")

    obj_b = await create_object_from_value([3, 4])

    result = await obj_a.concat(obj_b)
    schema = result.schema
    assert schema.columns["value"].nullable is True
