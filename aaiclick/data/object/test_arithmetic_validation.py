"""Tests for operand length validation in array-array binary operations."""

import pytest

from aaiclick import create_object_from_value


async def test_array_array_same_length(ctx):
    """Same-length arrays produce correct results."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])
    result = await (a.view(order_by="value") + b.view(order_by="value"))
    assert sorted(await result.view(order_by="value").data()) == [11, 22, 33]


async def test_array_array_length_mismatch_raises(ctx):
    """Different-length arrays raise ValueError."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30, 40])
    with pytest.raises(ValueError, match="Operand length mismatch"):
        await (a.view(order_by="value") + b.view(order_by="value"))


async def test_array_array_length_mismatch_left_longer(ctx):
    """Left operand longer than right raises ValueError."""
    a = await create_object_from_value([1, 2, 3, 4, 5])
    b = await create_object_from_value([10, 20])
    with pytest.raises(ValueError, match="left has 5 .* right has 2"):
        await (a.view(order_by="value") - b.view(order_by="value"))


async def test_view_length_mismatch_raises(ctx):
    """Views with different filtered lengths raise ValueError."""
    a = await create_object_from_value([1, 2, 3, 4, 5])
    b = await create_object_from_value([10, 20, 30, 40, 50])
    view_a = a.view(where="value <= 3", order_by="value")  # 3 elements
    view_b = b.view(where="value <= 20", order_by="value")  # 2 elements
    with pytest.raises(ValueError, match="Operand length mismatch"):
        await (view_a + view_b)


async def test_view_same_length_works(ctx):
    """Views with same filtered length produce correct results."""
    a = await create_object_from_value([1, 2, 3, 4, 5])
    b = await create_object_from_value([10, 20, 30, 40, 50])
    view_a = a.view(where="value >= 3", order_by="value")  # [3, 4, 5]
    view_b = b.view(limit=3, order_by="value")  # [10, 20, 30]
    result = await (view_a + view_b)
    assert sorted(await result.view(order_by="value").data()) == [13, 24, 35]


async def test_coalesce_length_mismatch_raises(ctx):
    """Coalesce with different-length arrays raises ValueError."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20])
    with pytest.raises(ValueError, match="Operand length mismatch"):
        await a.view(order_by="value").coalesce(b.view(order_by="value"))


async def test_object_view_length_mismatch_raises(ctx):
    """View + View with different lengths raises ValueError."""
    a = await create_object_from_value([1, 2, 3, 4, 5])
    b = await create_object_from_value([10, 20, 30, 40, 50])
    view_a = a.view(order_by="value")
    view_b = b.view(where="value <= 30", order_by="value")  # [10, 20, 30]
    with pytest.raises(ValueError, match="left has 5 .* right has 3"):
        await (view_a + view_b)


async def test_view_object_length_mismatch_raises(ctx):
    """Two Views with different filtered lengths raise ValueError."""
    a = await create_object_from_value([1, 2, 3, 4, 5])
    b = await create_object_from_value([10, 20])
    view_a = a.view(where="value >= 3", order_by="value")  # [3, 4, 5]
    view_b = b.view(order_by="value")
    with pytest.raises(ValueError, match="left has 3 .* right has 2"):
        await (view_a + view_b)


async def test_object_view_same_length_works(ctx):
    """View + View with same length produces correct results."""
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30, 40, 50])
    view_a = a.view(order_by="value")
    view_b = b.view(limit=3, order_by="value")  # [10, 20, 30]
    result = await (view_a + view_b)
    assert sorted(await result.view(order_by="value").data()) == [11, 22, 33]
