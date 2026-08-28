"""
Tests for singleton nested dict support - creating Objects from dicts
containing plain dict values, stored with dot column notation.

Example: {a: 2, x: {y: {z: 1}}}
Flattens to columns: a (Int64), x.y.z (Int64)
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.data_context import create_object
from aaiclick.data.models import FIELDTYPE_DICT, ORIENT_DICT, ORIENT_RECORDS, ColumnInfo, Schema

# =============================================================================
# Schema — dot notation column names
# =============================================================================


async def test_singleton_dict_schema(ctx):
    """Plain nested dicts flatten to dot-notation columns with no Array wrapper."""
    obj = await create_object_from_value({"a": 2, "x": {"y": {"z": 1}}})

    schema = obj.schema
    assert "a" in schema.columns
    assert "x.y.z" in schema.columns
    assert schema.columns["x.y.z"].type == "Int64"
    assert int(schema.columns["x.y.z"].array) == 0


async def test_dict_inside_array_items_schema(ctx):
    """A dict inside list-of-dicts items extends the name after the star."""
    obj = await create_object_from_value({"b": [{"c": {"d": 5}}, {"c": {"d": 10}}]})

    schema = obj.schema
    assert "b.*.c.d" in schema.columns
    assert schema.columns["b.*.c.d"].type == "Int64"
    assert int(schema.columns["b.*.c.d"].array) == 1


async def test_array_of_objects_inside_dict_schema(ctx):
    """A list-of-dicts inside a dict gets the star after the dot prefix."""
    obj = await create_object_from_value({"x": {"y": [{"z": 1}, {"z": 2}]}})

    schema = obj.schema
    assert "x.y.*.z" in schema.columns
    assert schema.columns["x.y.*.z"].type == "Int64"
    assert int(schema.columns["x.y.*.z"].array) == 1


# =============================================================================
# Round-trip — data() reconstructs the original nesting
# =============================================================================


async def test_singleton_dict_round_trip(ctx):
    """Deep plain nesting reconstructs exactly."""
    obj = await create_object_from_value({"a": 2, "x": {"y": {"z": 1}}})

    data = await obj.data()

    assert data == {"a": 2, "x": {"y": {"z": 1}}}


async def test_records_with_dict_field_round_trip(ctx):
    """Records list with a dict field, both orients."""
    records = [
        {"a": 1, "meta": {"source": "s1", "score": 0.5}},
        {"a": 2, "meta": {"source": "s2", "score": 0.75}},
    ]
    obj = await create_object_from_value(records)

    as_records = await obj.data(orient=ORIENT_RECORDS)
    assert as_records == records

    as_dict = await obj.data(orient=ORIENT_DICT)
    assert as_dict["a"] == [1, 2]
    assert as_dict["meta"] == [
        {"source": "s1", "score": 0.5},
        {"source": "s2", "score": 0.75},
    ]


async def test_array_of_objects_inside_dict_round_trip(ctx):
    """Mixed notation x.y.*.z reconstructs dict-of-list-of-dicts."""
    obj = await create_object_from_value({"x": {"y": [{"z": 1}, {"z": 2}]}})

    data = await obj.data()

    assert data == {"x": {"y": [{"z": 1}, {"z": 2}]}}


async def test_dict_inside_array_items_round_trip(ctx):
    """Mixed notation b.*.c.d reconstructs list-of-dicts-of-dicts."""
    obj = await create_object_from_value({"b": [{"c": {"d": 5}}, {"c": {"d": 10}}]})

    data = await obj.data()

    assert data == {"b": [{"c": {"d": 5}}, {"c": {"d": 10}}]}


# =============================================================================
# Validation
# =============================================================================


@pytest.mark.parametrize(
    "value, match",
    [
        # Flat dict keys containing '.' are rejected — data() would reshape them.
        pytest.param({"a.b": 1}, "must not contain", id="dotted-key"),
        # Dotted keys are rejected at any nesting level.
        pytest.param({"x": {"a.b": 1}}, "must not contain", id="nested-dotted-key"),
        pytest.param([{"b": [{"c.d": 1}]}], "must not contain", id="dotted-key-inside-array-items"),
        # Empty dicts have no representable columns.
        pytest.param({"x": {}}, "[Ee]mpty dict", id="empty-dict-value"),
        # Nested dict fields must have identical key sets across records.
        pytest.param([{"m": {"a": 1}}, {"m": {"b": 2}}], "identical keys", id="mismatched-nested-keys"),
    ],
)
async def test_invalid_nested_value_raises(ctx, value, match):
    with pytest.raises(ValueError, match=match):
        await create_object_from_value(value)


# =============================================================================
# Composition — overlapping prefixes, explicit Schema
# =============================================================================


async def test_star_group_and_plain_column_share_prefix_round_trip(ctx):
    """A plain-dot sibling and a star group under the same prefix merge correctly."""
    obj = await create_object_from_value({"x": {"w": 1, "y": [{"z": 1}, {"z": 2}]}})

    data = await obj.data()

    assert data == {"x": {"w": 1, "y": [{"z": 1}, {"z": 2}]}}


async def test_explicit_schema_dotted_column_round_trip(ctx):
    """A column literally named 'a.b' via explicit Schema unflattens on data()."""
    schema = Schema(
        fieldtype=FIELDTYPE_DICT,
        columns={
            "a.b": ColumnInfo("Int64"),
        },
    )
    obj = await create_object(schema)
    ch = obj.ch_client
    await ch.command(f"INSERT INTO {obj.table} (`a.b`) VALUES (1)")

    data = await obj.data()

    assert data == {"a": {"b": 1}}


# =============================================================================
# Arrow-based inference — whole-dataset strictness
# =============================================================================


@pytest.mark.parametrize(
    "value, match",
    [
        # Keys present only in later records are rejected, not silently dropped.
        pytest.param([{"c": 1}, {"c": 2, "d": 3}], "identical keys", id="extra-key-in-later-record"),
        pytest.param({"b": [{"c": 1}, {"c": 2, "d": 3}]}, "identical keys", id="extra-key-in-later-list-item"),
        # A None mixed into a list of dicts raises instead of fabricating a record.
        pytest.param({"b": [{"c": 1}, None]}, "identical keys", id="none-item-in-list-of-dicts"),
        pytest.param({"b": [{"c": 1}, 5]}, "uniform schema", id="non-dict-item-in-list-of-dicts"),
        # A field that changes type across records raises a clear ValueError.
        pytest.param([{"a": 1}, {"a": "s"}], "uniform schema", id="cross-record-type-conflict"),
    ],
)
async def test_whole_dataset_strictness_raises(ctx, value, match):
    with pytest.raises(ValueError, match=match):
        await create_object_from_value(value)


async def test_all_none_value_round_trips_as_none(ctx):
    """All-None values infer as Nullable(String) and round-trip as None."""
    obj = await create_object_from_value({"a": None, "b": 1})

    data = await obj.data()

    assert data == {"a": None, "b": 1}
    assert obj.schema.columns["a"].nullable is True


@pytest.mark.parametrize(
    "columns, insert",
    [
        # A column named 'x' next to 'x.y' cannot be reconstructed — raise, don't drop.
        pytest.param(
            {"x": ColumnInfo("Int64"), "x.y": ColumnInfo("Int64")},
            "(`x`, `x.y`) VALUES (1, 2)",
            id="dot-prefix",
        ),
        # A column named 'x' next to 'x.*.y' is the same collision through the star path.
        pytest.param(
            {"x": ColumnInfo("Int64"), "x.*.y": ColumnInfo("Int64", array=True)},
            "(`x`, `x.*.y`) VALUES (1, [2])",
            id="star-prefix",
        ),
    ],
)
async def test_plain_column_colliding_with_prefix_raises(ctx, columns, insert):
    schema = Schema(fieldtype=FIELDTYPE_DICT, columns=columns)
    obj = await create_object(schema)
    await obj.ch_client.command(f"INSERT INTO {obj.table} {insert}")

    with pytest.raises(ValueError, match="collide"):
        await obj.data()
