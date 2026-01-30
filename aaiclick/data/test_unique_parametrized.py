"""
Parametrized tests for unique() operation across data types.

Tests unique() method which returns unique values using GROUP BY.
"""

import pytest

from aaiclick import create_object_from_value


# =============================================================================
# Unique Tests - Integer Arrays
# =============================================================================


@pytest.mark.parametrize(
    "values,expected_unique",
    [
        pytest.param([1, 2, 2, 3, 3, 3, 4], {1, 2, 3, 4}, id="int-duplicates"),
        pytest.param([1, 1, 1, 1], {1}, id="int-all-same"),
        pytest.param([1, 2, 3, 4, 5], {1, 2, 3, 4, 5}, id="int-all-unique"),
        pytest.param([42], {42}, id="int-single"),
        pytest.param([0, 0, 1, 1, 0], {0, 1}, id="int-zeros-ones"),
        pytest.param([-5, -5, 5, 5], {-5, 5}, id="int-negative"),
        pytest.param([10, 20, 10, 30, 20, 10], {10, 20, 30}, id="int-repeated-pattern"),
    ],
)
async def test_unique_int(ctx, values, expected_unique):
    """Test unique() on integer arrays. Returns array Object with unique values."""
    obj = await create_object_from_value(values)

    result_obj = await obj.unique()
    result = await result_obj.data()

    # Convert to set for comparison (order is not guaranteed)
    assert set(result) == expected_unique


# =============================================================================
# Unique Tests - Float Arrays
# =============================================================================


@pytest.mark.parametrize(
    "values,expected_unique",
    [
        pytest.param([1.1, 2.2, 2.2, 3.3], {1.1, 2.2, 3.3}, id="float-duplicates"),
        pytest.param([3.14, 3.14, 3.14], {3.14}, id="float-all-same"),
        pytest.param([1.0, 2.0, 3.0], {1.0, 2.0, 3.0}, id="float-all-unique"),
        pytest.param([42.5], {42.5}, id="float-single"),
        pytest.param([-1.5, 1.5, -1.5], {-1.5, 1.5}, id="float-negative"),
        pytest.param([0.0, 0.0, 0.0, 1.0], {0.0, 1.0}, id="float-zeros"),
    ],
)
async def test_unique_float(ctx, values, expected_unique):
    """Test unique() on float arrays. Returns array Object with unique values."""
    obj = await create_object_from_value(values)

    result_obj = await obj.unique()
    result = await result_obj.data()

    # Convert to set for comparison (order is not guaranteed)
    assert set(result) == expected_unique


# =============================================================================
# Unique Tests - Boolean Arrays
# =============================================================================


@pytest.mark.parametrize(
    "values,expected_unique",
    [
        pytest.param([True, False, True, False], {0, 1}, id="bool-mixed"),
        pytest.param([True, True, True], {1}, id="bool-all-true"),
        pytest.param([False, False, False], {0}, id="bool-all-false"),
        pytest.param([True], {1}, id="bool-single-true"),
        pytest.param([False], {0}, id="bool-single-false"),
    ],
)
async def test_unique_bool(ctx, values, expected_unique):
    """Test unique() on boolean arrays. Returns array Object with unique values (as UInt8)."""
    obj = await create_object_from_value(values)

    result_obj = await obj.unique()
    result = await result_obj.data()

    # Convert to set for comparison (order is not guaranteed)
    assert set(result) == expected_unique


# =============================================================================
# Unique Tests - String Arrays
# =============================================================================


@pytest.mark.parametrize(
    "values,expected_unique",
    [
        pytest.param(["a", "b", "a", "c", "b"], {"a", "b", "c"}, id="str-duplicates"),
        pytest.param(["hello", "hello", "hello"], {"hello"}, id="str-all-same"),
        pytest.param(["x", "y", "z"], {"x", "y", "z"}, id="str-all-unique"),
        pytest.param(["single"], {"single"}, id="str-single"),
        pytest.param(["", "", "a"], {"", "a"}, id="str-with-empty"),
    ],
)
async def test_unique_str(ctx, values, expected_unique):
    """Test unique() on string arrays. Returns array Object with unique values."""
    obj = await create_object_from_value(values)

    result_obj = await obj.unique()
    result = await result_obj.data()

    # Convert to set for comparison (order is not guaranteed)
    assert set(result) == expected_unique


# =============================================================================
# Unique After Operations Tests
# =============================================================================


async def test_unique_after_concat(ctx):
    """Test unique() on concatenated arrays."""
    obj_a = await create_object_from_value([1, 2, 3])
    obj_b = await create_object_from_value([2, 3, 4])

    result = await obj_a.concat(obj_b)
    unique_obj = await result.unique()
    unique_values = await unique_obj.data()

    assert set(unique_values) == {1, 2, 3, 4}


async def test_unique_preserves_type(ctx):
    """Test that unique() preserves the data type."""
    obj = await create_object_from_value([1.5, 2.5, 1.5, 3.5])

    unique_obj = await obj.unique()
    unique_values = await unique_obj.data()

    # All values should be floats
    assert all(isinstance(v, float) for v in unique_values)
    assert set(unique_values) == {1.5, 2.5, 3.5}


async def test_unique_on_view(ctx):
    """Test unique() on a View with constraints."""
    obj = await create_object_from_value([1, 2, 2, 3, 3, 4, 4, 5])

    # Create view with WHERE constraint
    view = obj.view(where="value > 2")
    unique_obj = await view.unique()
    unique_values = await unique_obj.data()

    # Only values > 2: {3, 4, 5}
    assert set(unique_values) == {3, 4, 5}


async def test_unique_chained_with_aggregation(ctx):
    """Test that unique() result can be used with aggregation methods."""
    obj = await create_object_from_value([1, 2, 2, 3, 3, 3, 4])

    unique_obj = await obj.unique()  # {1, 2, 3, 4}

    # Sum of unique values: 1 + 2 + 3 + 4 = 10
    sum_obj = await unique_obj.sum()
    assert await sum_obj.data() == 10

    # Mean of unique values: 10 / 4 = 2.5
    mean_obj = await unique_obj.mean()
    assert await mean_obj.data() == 2.5


async def test_unique_large_dataset(ctx):
    """Test unique() on a larger dataset."""
    # Create array with many duplicates
    values = list(range(100)) * 10  # 1000 elements, 100 unique

    obj = await create_object_from_value(values)
    unique_obj = await obj.unique()
    unique_values = await unique_obj.data()

    assert len(unique_values) == 100
    assert set(unique_values) == set(range(100))
