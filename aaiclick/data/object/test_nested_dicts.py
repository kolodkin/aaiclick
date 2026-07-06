"""
Tests for singleton nested dict support - creating Objects from dicts
containing plain dict values, stored with dot column notation.

Example: {a: 2, x: {y: {z: 1}}}
Flattens to columns: a (Int64), x.y.z (Int64)
"""

from aaiclick import create_object_from_value

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
