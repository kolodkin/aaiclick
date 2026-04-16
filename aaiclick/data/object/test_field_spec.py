"""Tests for FieldSpec support in create_object_from_value."""

import pytest

from aaiclick import FieldSpec, create_object_from_value
from aaiclick.data.data_context import get_ch_client

# --- Dict of arrays ---


async def test_field_spec_nullable_dict_arrays(ctx):
    """FieldSpec(nullable=True) creates a Nullable column from dict of arrays."""
    obj = await create_object_from_value(
        {"name": ["alice", "bob"], "score": [95.5, 88.0]},
        fields={"score": FieldSpec(nullable=True)},
    )
    schema = obj.schema
    assert schema.columns["score"].nullable is True
    assert schema.columns["name"].nullable is False


async def test_field_spec_low_cardinality_dict_arrays(ctx):
    """FieldSpec(low_cardinality=True) creates a LowCardinality column."""
    obj = await create_object_from_value(
        {"category": ["a", "b", "a"], "value": [1, 2, 3]},
        fields={"category": FieldSpec(low_cardinality=True)},
    )
    schema = obj.schema
    assert schema.columns["category"].low_cardinality is True
    assert schema.columns["category"].type == "String"
    data = await obj.data()
    assert data["category"] == ["a", "b", "a"]


async def test_field_spec_combined_nullable_low_cardinality(ctx):
    """FieldSpec can set both nullable and low_cardinality."""
    obj = await create_object_from_value(
        {"tag": ["x", "y"], "val": [1, 2]},
        fields={"tag": FieldSpec(nullable=True, low_cardinality=True)},
    )
    schema = obj.schema
    col = schema.columns["tag"]
    assert col.nullable is True
    assert col.low_cardinality is True
    assert col.ch_type() == "LowCardinality(Nullable(String))"


async def test_field_spec_type_override(ctx):
    """FieldSpec(type=...) overrides the inferred base type."""
    obj = await create_object_from_value(
        {"price": [1, 2, 3]},
        fields={"price": FieldSpec(type="Float32")},
    )
    schema = obj.schema
    assert schema.columns["price"].type == "Float32"


# --- Dict of scalars ---


async def test_field_spec_dict_scalars(ctx):
    """FieldSpec works with dict-of-scalars input."""
    obj = await create_object_from_value(
        {"name": "alice", "age": 30},
        fields={"name": FieldSpec(low_cardinality=True)},
    )
    schema = obj.schema
    assert schema.columns["name"].low_cardinality is True


# --- List of dicts (records) ---


async def test_field_spec_records(ctx):
    """FieldSpec works with records (list of dicts) input."""
    obj = await create_object_from_value(
        [{"city": "NYC", "pop": 8000000}, {"city": "LA", "pop": 4000000}],
        fields={"city": FieldSpec(low_cardinality=True), "pop": FieldSpec(nullable=True)},
    )
    schema = obj.schema
    assert schema.columns["city"].low_cardinality is True
    assert schema.columns["pop"].nullable is True
    data = await obj.data()
    assert data["city"] == ["NYC", "LA"]


# --- List of scalars ---


async def test_field_spec_list_scalars(ctx):
    """FieldSpec works with list-of-scalars via the 'value' column."""
    obj = await create_object_from_value(
        [10, 20, 30],
        fields={"value": FieldSpec(nullable=True)},
    )
    schema = obj.schema
    assert schema.columns["value"].nullable is True


# --- Scalar ---


async def test_field_spec_scalar(ctx):
    """FieldSpec works with scalar input via the 'value' column."""
    obj = await create_object_from_value(
        42,
        fields={"value": FieldSpec(nullable=True)},
    )
    schema = obj.schema
    assert schema.columns["value"].nullable is True


# --- Validation ---


async def test_field_spec_unknown_column_raises(ctx):
    """FieldSpec referencing unknown column raises ValueError."""
    with pytest.raises(ValueError, match="unknown columns"):
        await create_object_from_value(
            {"a": [1, 2]},
            fields={"nonexistent": FieldSpec(nullable=True)},
        )


async def test_field_spec_aai_id_raises(ctx):
    """FieldSpec targeting aai_id raises ValueError."""
    with pytest.raises(ValueError, match="aai_id"):
        await create_object_from_value(
            {"a": [1, 2]},
            fields={"aai_id": FieldSpec(nullable=True)},
        )


# --- Multiple fields ---


async def test_field_spec_multiple_columns(ctx):
    """Multiple FieldSpec entries apply to different columns."""
    obj = await create_object_from_value(
        {"name": ["a", "b"], "tag": ["x", "y"], "score": [1.0, 2.0]},
        fields={
            "name": FieldSpec(low_cardinality=True),
            "tag": FieldSpec(nullable=True, low_cardinality=True),
            "score": FieldSpec(nullable=True),
        },
    )
    schema = obj.schema
    assert schema.columns["name"].low_cardinality is True
    assert schema.columns["name"].nullable is False
    assert schema.columns["tag"].low_cardinality is True
    assert schema.columns["tag"].nullable is True
    assert schema.columns["score"].nullable is True
    assert schema.columns["score"].low_cardinality is False


# --- Data insertion and retrieval with FieldSpec ---


async def test_field_spec_nullable_insert_and_read_null(ctx):
    """Nullable column created via FieldSpec accepts NULL values."""
    obj = await create_object_from_value(
        {"name": ["alice", "bob"], "score": [95.5, 88.0]},
        fields={"score": FieldSpec(nullable=True)},
    )
    ch = get_ch_client()
    await ch.command(
        f"INSERT INTO {obj.table} (name, score) VALUES ('carol', NULL)"
    )
    data = await obj.data()
    assert data["name"] == ["alice", "bob", "carol"]
    assert None in data["score"]


async def test_field_spec_low_cardinality_roundtrip(ctx):
    """LowCardinality column created via FieldSpec stores and retrieves correctly."""
    obj = await create_object_from_value(
        {"tag": ["x", "x", "y", "x", "y"]},
        fields={"tag": FieldSpec(low_cardinality=True)},
    )
    data = await obj.data()
    assert data["tag"] == ["x", "x", "y", "x", "y"]
    assert obj.schema.columns["tag"].ch_type() == "LowCardinality(String)"


async def test_field_spec_nullable_arithmetic(ctx):
    """Arithmetic on nullable column created via FieldSpec propagates NULLs."""
    obj = await create_object_from_value(
        [10, 20, 30],
        fields={"value": FieldSpec(nullable=True)},
    )
    ch = get_ch_client()
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (NULL)")

    result = await (obj + 5)
    data = await result.data()
    assert data == [15, 25, 35, None]
