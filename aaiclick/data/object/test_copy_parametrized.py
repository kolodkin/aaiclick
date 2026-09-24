"""
Parametrized tests for copy operations across different data types.

Tests scalar and array copying with verification that new tables are created.
"""

import pytest

from aaiclick import View, create_object_from_value, delete_persistent_object

THRESHOLD = 1e-5


# =============================================================================
# Scalar Copy Tests
# =============================================================================


@pytest.mark.parametrize(
    "input_value,expected_output",
    [
        # Integer scalars
        pytest.param(42, 42, id="int-positive"),
        pytest.param(0, 0, id="int-zero"),
        pytest.param(-100, -100, id="int-negative"),
        pytest.param(1000000, 1000000, id="int-large"),
        # Float scalars
        pytest.param(3.14159, 3.14159, id="float-pi"),
        pytest.param(0.0, 0.0, id="float-zero"),
        pytest.param(-10.5, -10.5, id="float-negative"),
        pytest.param(1.5, 1.5, id="float-small"),
        # Boolean scalars (stored as UInt8)
        pytest.param(True, 1, id="bool-true"),
        pytest.param(False, 0, id="bool-false"),
        # String scalars
        pytest.param("hello", "hello", id="str-simple"),
        pytest.param("", "", id="str-empty"),
        pytest.param("hello world", "hello world", id="str-spaces"),
        pytest.param("こんにちは", "こんにちは", id="str-unicode"),
    ],
)
async def test_scalar_copy(ctx, input_value, expected_output):
    """Test copying scalar objects across all data types."""
    obj = await create_object_from_value(input_value)

    copy = await obj.copy()
    data = await copy.data()

    assert data == pytest.approx(expected_output, abs=THRESHOLD)

    # Verify tables are different
    assert copy.table != obj.table


# =============================================================================
# Array Copy Tests
# =============================================================================


@pytest.mark.parametrize(
    "input_value,expected_output",
    [
        # Integer arrays
        pytest.param([1, 2, 3], [1, 2, 3], id="int-array"),
        pytest.param([0, 0, 0], [0, 0, 0], id="int-zeros"),
        pytest.param([-5, -10, -15], [-5, -10, -15], id="int-negative"),
        pytest.param([42], [42], id="int-single"),
        pytest.param([1, 2, 3, 4, 5], [1, 2, 3, 4, 5], id="int-longer"),
        # Float arrays
        pytest.param([1.5, 2.5, 3.5], [1.5, 2.5, 3.5], id="float-array"),
        pytest.param([0.0, 0.0], [0.0, 0.0], id="float-zeros"),
        pytest.param([-5.5, -10.5], [-5.5, -10.5], id="float-negative"),
        pytest.param([3.14159], [3.14159], id="float-single"),
        # Boolean arrays (stored as UInt8)
        pytest.param([True, False, True], [1, 0, 1], id="bool-mixed"),
        pytest.param([True, True, True], [1, 1, 1], id="bool-all-true"),
        pytest.param([False, False, False], [0, 0, 0], id="bool-all-false"),
        # String arrays
        pytest.param(["apple", "banana", "cherry"], ["apple", "banana", "cherry"], id="str-array"),
        pytest.param(["single"], ["single"], id="str-single"),
        pytest.param(["hello", "world"], ["hello", "world"], id="str-pair"),
        pytest.param(["a", "", "b"], ["a", "", "b"], id="str-with-empty"),
        # Unsorted input: copy preserves the original array order
        pytest.param([5, 1, 9, 3, 7], [5, 1, 9, 3, 7], id="int-unsorted"),
        pytest.param([5.5, 1.1, 9.9, 3.3], [5.5, 1.1, 9.9, 3.3], id="float-unsorted"),
        pytest.param(["z", "a", "m", "b", "y"], ["z", "a", "m", "b", "y"], id="str-unsorted"),
    ],
)
async def test_array_copy(ctx, input_value, expected_output):
    """Test copying array objects across all data types."""
    obj = await create_object_from_value(input_value)

    copy = await obj.copy()
    data = await copy.data()

    assert data == pytest.approx(expected_output, abs=THRESHOLD)

    # Verify tables are different
    assert copy.table != obj.table


# =============================================================================
# Multiple Copies Tests
# =============================================================================


@pytest.mark.parametrize(
    "input_value",
    [
        # Various types
        pytest.param(42, id="int-scalar"),
        pytest.param([1, 2, 3], id="int-array"),
        pytest.param(3.14159, id="float-scalar"),
        pytest.param([1.5, 2.5, 3.5], id="float-array"),
        pytest.param("hello", id="str-scalar"),
        pytest.param(["a", "b", "c"], id="str-array"),
    ],
)
async def test_multiple_copies_create_different_tables(ctx, input_value):
    """Test that multiple copies create different tables."""
    obj = await create_object_from_value(input_value)

    copy1 = await obj.copy()
    copy2 = await obj.copy()
    copy3 = await obj.copy()

    # All tables should be different
    assert copy1.table != obj.table
    assert copy2.table != obj.table
    assert copy3.table != obj.table
    assert copy1.table != copy2.table
    assert copy2.table != copy3.table
    assert copy1.table != copy3.table


# =============================================================================
# Named Copy Tests
# =============================================================================


async def test_copy_with_name_global_scope(ctx):
    """copy(name=..., scope='global') routes through the named-table path."""
    obj = await create_object_from_value([1, 2, 3])

    copy = await obj.copy(name="copy_named_global", scope="global")
    try:
        assert copy.table == "p_copy_named_global"
        assert await copy.data() == [1, 2, 3]
    finally:
        await delete_persistent_object("copy_named_global", scope="global")


async def test_copy_with_name_temp_scope(ctx):
    """copy(name=...) defaults to temp_named scope (t_<name>_<id>)."""
    obj = await create_object_from_value([10, 20])

    copy = await obj.copy(name="copy_named_temp")

    assert copy.table.startswith("t_copy_named_temp_")
    assert await copy.data() == [10, 20]


# =============================================================================
# Ordered + Limited Copy Tests
# =============================================================================


@pytest.mark.parametrize(
    "offset,expected",
    [
        pytest.param(None, [9, 8, 7], id="limit"),
        pytest.param(2, [7, 6, 5], id="limit-offset"),
    ],
)
async def test_copy_applies_order_by_before_slicing(ctx, offset, expected):
    """Regression: copy() of an ordered + sliced View must materialize the
    top-N rows, not an arbitrary N re-sorted. See ``_build_select``."""
    obj = await create_object_from_value([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

    copied = await obj.view(order_by="value DESC", limit=3, offset=offset).copy()

    assert sorted(await copied.data(), reverse=True) == expected


async def test_copy_selected_fields_applies_order_by_before_limit(ctx):
    """The field-selection copy path needs the same inner ORDER BY —
    this is the shape that silently truncates a "top-N by score" corpus."""
    obj = await create_object_from_value({"title": ["a", "b", "c", "d", "e"], "votes": [1, 5, 3, 2, 4]})

    view = obj[["title", "votes"]].view(order_by="votes DESC", limit=2)
    data = await (await view.copy()).data()

    assert sorted(data["votes"], reverse=True) == [5, 4]
    assert sorted(data["title"]) == ["b", "e"]


# =============================================================================
# Copying column-selection Views
# =============================================================================


async def test_dict_selector_copy(ctx):
    """Test that copy() materializes a view as a new array Object."""
    obj = await create_object_from_value({"param1": [1, 2, 3], "param2": [4, 5, 6]}, aai_id=True)

    view = obj["param1"]
    arr = await view.copy()

    # Should be a new Object, not a View
    assert not isinstance(arr, View)
    # Should have a different table (copy creates new table)
    assert arr.table != obj.table
    assert await arr.data() == [1, 2, 3]


@pytest.mark.parametrize(
    "value, field, expected",
    [
        pytest.param({"param1": [10, 20], "param2": [30, 40]}, "param2", [30, 40], id="second-field"),
        pytest.param({"floats": [1.5, 2.5, 3.5], "ints": [1, 2, 3]}, "floats", [1.5, 2.5, 3.5], id="float"),
        pytest.param({"names": ["Alice", "Bob"], "ages": [30, 25]}, "names", ["Alice", "Bob"], id="string"),
    ],
)
async def test_dict_selector_copy_field(ctx, value, field, expected):
    """Test copying a selected field of various types."""
    obj = await create_object_from_value(value, aai_id=True)

    arr = await obj[field].copy()

    assert await arr.data() == expected


async def test_multi_field_selector_copy(ctx):
    """Test copying a multi-field view creates dict Object."""
    obj = await create_object_from_value({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]}, aai_id=True)

    view = obj[["x", "y"]]
    cloned = await view.copy()

    data = await cloned.data()
    assert data == {"x": [1, 2, 3], "y": [4, 5, 6]}

    # Verify cloned is a dict Object
    schema = cloned.schema
    assert "x" in schema.columns
    assert "y" in schema.columns
    assert "z" not in schema.columns
