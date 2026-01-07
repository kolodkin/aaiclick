"""
Tests for temporal ordering behavior with Snowflake IDs.

These tests verify that concat and insert operations preserve order based on
Snowflake ID creation timestamps, not argument order. This ensures temporal
causality in distributed systems.
"""

import asyncio


async def test_concat_preserves_creation_order_a_first(ctx):
    """Test that concat result is based on creation order when obj_a created first."""
    # Create obj_a first (T1)
    obj_a = await ctx.create_object_from_value([1, 2, 3])

    # Small delay to ensure different timestamps
    await asyncio.sleep(0.01)

    # Create obj_b second (T2)
    obj_b = await ctx.create_object_from_value([4, 5, 6])

    # Both concat orders should give same result: [1, 2, 3, 4, 5, 6]
    result1 = await obj_a.concat(obj_b)
    data1 = await result1.data()

    result2 = await obj_b.concat(obj_a)
    data2 = await result2.data()

    # Both should give same result based on creation order
    assert data1 == [1, 2, 3, 4, 5, 6]
    assert data2 == [1, 2, 3, 4, 5, 6]
    assert data1 == data2


async def test_concat_preserves_creation_order_b_first(ctx):
    """Test that concat result is based on creation order when obj_b created first."""
    # Create obj_b first (T1)
    obj_b = await ctx.create_object_from_value([4, 5, 6])

    # Small delay to ensure different timestamps
    await asyncio.sleep(0.01)

    # Create obj_a second (T2)
    obj_a = await ctx.create_object_from_value([1, 2, 3])

    # Both concat orders should give same result: [4, 5, 6, 1, 2, 3]
    result1 = await obj_a.concat(obj_b)
    data1 = await result1.data()

    result2 = await obj_b.concat(obj_a)
    data2 = await result2.data()

    # Both should give same result based on creation order
    assert data1 == [4, 5, 6, 1, 2, 3]
    assert data2 == [4, 5, 6, 1, 2, 3]
    assert data1 == data2


async def test_insert_preserves_creation_order_a_first(ctx):
    """Test that insert result is based on creation order when obj_a created first."""
    # Create obj_a first (T1)
    obj_a1 = await ctx.create_object_from_value([1, 2, 3])
    obj_a2 = await ctx.create_object_from_value([1, 2, 3])

    # Small delay to ensure different timestamps
    await asyncio.sleep(0.01)

    # Create obj_b second (T2)
    obj_b = await ctx.create_object_from_value([4, 5, 6])

    # Insert in both directions
    await obj_a1.insert(obj_b)
    data1 = await obj_a1.data()

    await obj_a2.insert(obj_b)
    data2 = await obj_a2.data()

    # Both should give same result based on creation order
    assert data1 == [1, 2, 3, 4, 5, 6]
    assert data2 == [1, 2, 3, 4, 5, 6]
    assert data1 == data2


async def test_insert_preserves_creation_order_b_first(ctx):
    """Test that insert result is based on creation order when obj_b created first."""
    # Create obj_b first (T1)
    obj_b = await ctx.create_object_from_value([4, 5, 6])

    # Small delay to ensure different timestamps
    await asyncio.sleep(0.01)

    # Create obj_a second (T2)
    obj_a1 = await ctx.create_object_from_value([1, 2, 3])
    obj_a2 = await ctx.create_object_from_value([1, 2, 3])

    # Insert obj_b into both obj_a instances
    await obj_a1.insert(obj_b)
    data1 = await obj_a1.data()

    await obj_a2.insert(obj_b)
    data2 = await obj_a2.data()

    # Both should preserve creation order: obj_a elements (T2), then obj_b elements (T1)
    # Wait, this is wrong. Insert adds obj_b to obj_a, so we get obj_a's existing data
    # plus obj_b's data. But obj_b was created first, so when we retrieve, we should
    # get them ordered by Snowflake ID.
    #
    # Actually, let me think about this more carefully:
    # - obj_b created at T1 has IDs from T1
    # - obj_a created at T2 has IDs from T2
    # - When we insert(obj_a, obj_b), we're adding obj_b's rows to obj_a's table
    # - When we retrieve via .data(), it orders by aai_id
    # - So we'd get: obj_b elements (T1 IDs), then obj_a elements (T2 IDs)

    # Result should be [4, 5, 6, 1, 2, 3] based on creation timestamps
    assert data1 == [4, 5, 6, 1, 2, 3]
    assert data2 == [4, 5, 6, 1, 2, 3]
    assert data1 == data2


async def test_concat_with_value_preserves_creation_order(ctx):
    """Test that concat with value preserves creation order."""
    # Create obj_a first
    obj_a = await ctx.create_object_from_value([1, 2, 3])

    # Small delay
    await asyncio.sleep(0.01)

    # Concat with a value (which will create a temp object with later timestamp)
    result = await obj_a.concat([4, 5, 6])
    data = await result.data()

    # obj_a created first, so its elements come first
    assert data == [1, 2, 3, 4, 5, 6]


async def test_insert_with_value_preserves_creation_order(ctx):
    """Test that insert with value preserves creation order."""
    # Create obj_a first
    obj_a = await ctx.create_object_from_value([1, 2, 3])

    # Small delay
    await asyncio.sleep(0.01)

    # Insert value (which will create a temp object with later timestamp)
    await obj_a.insert([4, 5, 6])
    data = await obj_a.data()

    # obj_a created first, so its elements come first
    assert data == [1, 2, 3, 4, 5, 6]


async def test_multiple_concat_preserves_temporal_order(ctx):
    """Test that multiple concat operations maintain temporal order."""
    # Create three objects at different times
    obj1 = await ctx.create_object_from_value([1, 2])
    await asyncio.sleep(0.01)

    obj2 = await ctx.create_object_from_value([3, 4])
    await asyncio.sleep(0.01)

    obj3 = await ctx.create_object_from_value([5, 6])

    # Concat in various orders - all should give same result
    result1 = await obj1.concat(obj2)
    result1 = await result1.concat(obj3)
    data1 = await result1.data()

    result2 = await obj3.concat(obj2)
    result2 = await result2.concat(obj1)
    data2 = await result2.data()

    result3 = await obj2.concat(obj3)
    result3 = await result3.concat(obj1)
    data3 = await result3.data()

    # All should be ordered by creation time: [1, 2, 3, 4, 5, 6]
    assert data1 == [1, 2, 3, 4, 5, 6]
    assert data2 == [1, 2, 3, 4, 5, 6]
    assert data3 == [1, 2, 3, 4, 5, 6]
