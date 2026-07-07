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


async def test_dotted_key_raises(ctx):
    """Flat dict keys containing '.' are rejected — data() would reshape them."""
    with pytest.raises(ValueError, match="must not contain"):
        await create_object_from_value({"a.b": 1})


async def test_nested_dotted_key_raises(ctx):
    """Dotted keys are rejected at any nesting level."""
    with pytest.raises(ValueError, match="must not contain"):
        await create_object_from_value({"x": {"a.b": 1}})


async def test_dotted_key_inside_array_items_raises(ctx):
    """Dotted keys inside list-of-dicts items are rejected."""
    with pytest.raises(ValueError, match="must not contain"):
        await create_object_from_value([{"b": [{"c.d": 1}]}])


async def test_empty_dict_value_raises(ctx):
    """Empty dicts have no representable columns."""
    with pytest.raises(ValueError, match="[Ee]mpty dict"):
        await create_object_from_value({"x": {}})


async def test_mismatched_nested_keys_raises(ctx):
    """Nested dict fields must have identical key sets across records."""
    with pytest.raises(ValueError, match="identical keys"):
        await create_object_from_value(
            [
                {"m": {"a": 1}},
                {"m": {"b": 2}},
            ]
        )


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


async def test_extra_key_in_later_record_raises(ctx):
    """Keys present only in later records are rejected, not silently dropped."""
    with pytest.raises(ValueError, match="identical keys"):
        await create_object_from_value([{"c": 1}, {"c": 2, "d": 3}])


async def test_extra_key_in_later_list_item_raises(ctx):
    """Keys present only in later list-of-dicts items are rejected (silent-drop fix)."""
    with pytest.raises(ValueError, match="identical keys"):
        await create_object_from_value({"b": [{"c": 1}, {"c": 2, "d": 3}]})


async def test_non_dict_item_in_list_of_dicts_raises(ctx):
    """A non-dict mixed into a list of dicts raises a clear ValueError."""
    with pytest.raises(ValueError, match="uniform schema"):
        await create_object_from_value({"b": [{"c": 1}, 5]})


async def test_cross_record_type_conflict_raises(ctx):
    """A field that changes type across records raises a clear ValueError."""
    with pytest.raises(ValueError, match="uniform schema"):
        await create_object_from_value([{"a": 1}, {"a": "s"}])


async def test_none_item_in_list_of_dicts_raises(ctx):
    """A None mixed into a list of dicts raises instead of fabricating a record."""
    with pytest.raises(ValueError, match="identical keys"):
        await create_object_from_value({"b": [{"c": 1}, None]})


async def test_all_none_value_round_trips_as_none(ctx):
    """All-None values infer as Nullable(String) and round-trip as None."""
    obj = await create_object_from_value({"a": None, "b": 1})

    data = await obj.data()

    assert data == {"a": None, "b": 1}
    assert obj.schema.columns["a"].nullable is True
