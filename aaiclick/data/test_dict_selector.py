"""
Tests for dict selector feature - selecting fields from dict Objects.

This module tests the __getitem__ syntax for selecting columns from dict Objects
and the copy() method for materializing Views as array Objects.
"""

from aaiclick import create_object_from_value, View


# =============================================================================
# Basic Dict Selector Tests
# =============================================================================

async def test_dict_selector_basic(ctx):
    """Test basic dict field selection with __getitem__."""
    obj = await create_object_from_value({'param1': [1, 2, 3], 'param2': [4, 5, 6]})

    view = obj['param1']

    assert isinstance(view, View)
    assert view.selected_fields == ['param1']
    assert view.is_single_field


async def test_dict_selector_data(ctx):
    """Test that selecting a field returns the correct data."""
    obj = await create_object_from_value({'param1': [10, 20, 30], 'param2': [40, 50, 60]})

    view = obj['param1']
    data = await view.data()

    assert data == [10, 20, 30]


async def test_dict_selector_second_field(ctx):
    """Test selecting the second field."""
    obj = await create_object_from_value({'param1': [1, 2, 3], 'param2': [4, 5, 6]})

    view = obj['param2']
    data = await view.data()

    assert data == [4, 5, 6]


async def test_dict_selector_multiple_fields(ctx):
    """Test selecting different fields from the same dict."""
    obj = await create_object_from_value({
        'x': [1, 2, 3],
        'y': [10, 20, 30],
        'z': [100, 200, 300]
    })

    view_x = obj['x']
    view_y = obj['y']
    view_z = obj['z']

    assert await view_x.data() == [1, 2, 3]
    assert await view_y.data() == [10, 20, 30]
    assert await view_z.data() == [100, 200, 300]


# =============================================================================
# Copy Tests
# =============================================================================

async def test_dict_selector_copy(ctx):
    """Test that copy() materializes a view as a new array Object."""
    obj = await create_object_from_value({'param1': [1, 2, 3], 'param2': [4, 5, 6]})

    view = obj['param1']
    arr = await view.copy()

    # Should be a new Object, not a View
    assert not isinstance(arr, View)
    # Should have a different table (copy creates new table)
    assert arr.table != obj.table
    assert await arr.data() == [1, 2, 3]


async def test_dict_selector_copy_second_field(ctx):
    """Test copying the second field."""
    obj = await create_object_from_value({'param1': [10, 20], 'param2': [30, 40]})

    view = obj['param2']
    arr = await view.copy()

    assert await arr.data() == [30, 40]


async def test_dict_selector_copy_float(ctx):
    """Test copying a float field."""
    obj = await create_object_from_value({'floats': [1.5, 2.5, 3.5], 'ints': [1, 2, 3]})

    view = obj['floats']
    arr = await view.copy()

    assert await arr.data() == [1.5, 2.5, 3.5]


async def test_dict_selector_copy_string(ctx):
    """Test copying a string field."""
    obj = await create_object_from_value({'names': ['Alice', 'Bob'], 'ages': [30, 25]})

    view = obj['names']
    arr = await view.copy()

    assert await arr.data() == ['Alice', 'Bob']


# =============================================================================
# Operator Tests with Dict Selector
# =============================================================================

async def test_dict_selector_add(ctx):
    """Test addition operator with dict selector views."""
    obj_a = await create_object_from_value({'x': [1, 2, 3], 'y': [10, 20, 30]})
    obj_b = await create_object_from_value({'x': [100, 200, 300], 'y': [1000, 2000, 3000]})

    view_a = obj_a['x']
    view_b = obj_b['x']

    result = await (view_a + view_b)
    data = await result.data()

    assert data == [101, 202, 303]


async def test_dict_selector_multiply(ctx):
    """Test multiplication operator with dict selector views."""
    obj = await create_object_from_value({'prices': [10, 20, 30], 'qty': [2, 3, 4]})

    prices = obj['prices']
    qty = obj['qty']

    result = await (prices * qty)
    data = await result.data()

    assert data == [20, 60, 120]


async def test_dict_selector_with_array_object(ctx):
    """Test operation between dict selector view and array object."""
    dict_obj = await create_object_from_value({'values': [10, 20, 30]})
    array_obj = await create_object_from_value([1, 2, 3])

    view = dict_obj['values']
    result = await (view + array_obj)
    data = await result.data()

    assert data == [11, 22, 33]


# =============================================================================
# Aggregation Tests with Dict Selector
# =============================================================================

async def test_dict_selector_sum(ctx):
    """Test sum aggregation on dict selector view."""
    obj = await create_object_from_value({'numbers': [1, 2, 3, 4, 5]})

    view = obj['numbers']
    result = await view.sum()

    assert await result.data() == 15


async def test_dict_selector_mean(ctx):
    """Test mean aggregation on dict selector view."""
    obj = await create_object_from_value({'numbers': [10, 20, 30, 40]})

    view = obj['numbers']
    result = await view.mean()

    assert await result.data() == 25.0


async def test_dict_selector_min_max(ctx):
    """Test min/max aggregation on dict selector view."""
    obj = await create_object_from_value({'values': [5, 2, 8, 1, 9]})

    view = obj['values']

    min_result = await view.min()
    max_result = await view.max()

    assert await min_result.data() == 1
    assert await max_result.data() == 9


# =============================================================================
# View Repr Tests
# =============================================================================

async def test_dict_selector_repr(ctx):
    """Test that View repr includes selected_fields."""
    obj = await create_object_from_value({'param1': [1, 2, 3]})

    view = obj['param1']

    repr_str = repr(view)
    assert "selected_fields=['param1']" in repr_str


# =============================================================================
# Edge Cases
# =============================================================================

async def test_dict_selector_single_element(ctx):
    """Test dict selector with single element arrays."""
    obj = await create_object_from_value({'a': [42], 'b': [100]})

    view = obj['a']
    data = await view.data()

    assert data == [42]


async def test_dict_selector_preserves_order(ctx):
    """Test that dict selector preserves original array order."""
    obj = await create_object_from_value({
        'letters': ['z', 'a', 'm', 'b'],
        'numbers': [4, 1, 3, 2]
    })

    view = obj['letters']
    data = await view.data()

    # Order should match original array order
    assert data == ['z', 'a', 'm', 'b']


# =============================================================================
# Multi-Field Selector Tests
# =============================================================================

async def test_multi_field_selector_basic(ctx):
    """Test multi-field selection with list syntax."""
    obj = await create_object_from_value({'x': [1, 2, 3], 'y': [4, 5, 6], 'z': [7, 8, 9]})

    view = obj[['x', 'y']]

    assert isinstance(view, View)
    assert view.selected_fields == ['x', 'y']
    assert not view.is_single_field


async def test_multi_field_selector_data(ctx):
    """Test that multi-field selector returns dict with selected columns."""
    obj = await create_object_from_value({'x': [1, 2, 3], 'y': [4, 5, 6], 'z': [7, 8, 9]})

    view = obj[['x', 'z']]
    data = await view.data()

    assert data == {'x': [1, 2, 3], 'z': [7, 8, 9]}


async def test_multi_field_selector_copy(ctx):
    """Test copying a multi-field view creates dict Object."""
    obj = await create_object_from_value({'x': [1, 2, 3], 'y': [4, 5, 6], 'z': [7, 8, 9]})

    view = obj[['x', 'y']]
    cloned = await view.copy()

    data = await cloned.data()
    assert data == {'x': [1, 2, 3], 'y': [4, 5, 6]}

    # Verify cloned is a dict Object
    meta = await cloned.metadata()
    assert 'x' in meta.columns
    assert 'y' in meta.columns
    assert 'z' not in meta.columns


async def test_multi_field_selector_repr(ctx):
    """Test that View repr includes selected_fields."""
    obj = await create_object_from_value({'x': [1], 'y': [2]})

    view = obj[['x', 'y']]

    repr_str = repr(view)
    assert "selected_fields=" in repr_str
    assert "'x'" in repr_str
    assert "'y'" in repr_str


async def test_multi_field_selector_metadata(ctx):
    """Test that ViewMetadata includes selected_fields."""
    obj = await create_object_from_value({'x': [1, 2], 'y': [3, 4], 'z': [5, 6]})

    view = obj[['x', 'z']]
    meta = await view.metadata()

    assert meta.selected_fields == ['x', 'z']
