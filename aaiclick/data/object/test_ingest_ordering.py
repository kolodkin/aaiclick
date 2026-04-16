"""
Tests for ordering behavior with fresh Snowflake IDs in insert/concat.

With the fix to generate fresh IDs (not preserve source aai_id), order
follows argument order: self first, then args left-to-right.
"""

from aaiclick import create_object_from_value
from aaiclick.data.data_context import get_ch_client


async def test_concat_follows_argument_order(ctx):
    """Concat result follows argument order: self, then args left-to-right."""
    obj_a = await create_object_from_value([1, 2, 3])
    obj_b = await create_object_from_value([4, 5, 6])

    result = await obj_a.concat(obj_b)
    data = await result.data()
    assert data == [1, 2, 3, 4, 5, 6]

    result = await obj_b.concat(obj_a)
    data = await result.data()
    assert data == [4, 5, 6, 1, 2, 3]


async def test_insert_follows_argument_order(ctx):
    """Insert appends args in argument order after existing data."""
    obj_a = await create_object_from_value([1, 2, 3])
    obj_b = await create_object_from_value([4, 5, 6])

    await obj_a.insert(obj_b)
    data = await obj_a.data()
    assert data == [1, 2, 3, 4, 5, 6]


async def test_insert_reverse_argument_order(ctx):
    """Insert preserves target data first, then appends source."""
    obj_a = await create_object_from_value([4, 5, 6])
    obj_b = await create_object_from_value([1, 2, 3])

    await obj_a.insert(obj_b)
    data = await obj_a.data()
    assert data == [4, 5, 6, 1, 2, 3]


async def test_concat_with_value_follows_argument_order(ctx):
    """Concat with inline value: self first, then value."""
    obj_a = await create_object_from_value([1, 2, 3])

    result = await obj_a.concat([4, 5, 6])
    data = await result.data()
    assert data == [1, 2, 3, 4, 5, 6]


async def test_insert_with_value_follows_argument_order(ctx):
    """Insert with inline value: existing data first, then value."""
    obj_a = await create_object_from_value([1, 2, 3])

    await obj_a.insert([4, 5, 6])
    data = await obj_a.data()
    assert data == [1, 2, 3, 4, 5, 6]


async def test_multiple_concat_preserves_argument_order(ctx):
    """Chained concat preserves argument order at each step."""
    obj1 = await create_object_from_value([1, 2])
    obj2 = await create_object_from_value([3, 4])
    obj3 = await create_object_from_value([5, 6])

    result = await obj1.concat(obj2)
    result = await result.concat(obj3)
    data = await result.data()
    assert data == [1, 2, 3, 4, 5, 6]


async def test_concat_multi_arg_order(ctx):
    """Multi-arg concat: self, then each arg in order."""
    obj1 = await create_object_from_value([1, 2])
    obj2 = await create_object_from_value([3, 4])
    obj3 = await create_object_from_value([5, 6])

    result = await obj1.concat(obj2, obj3)
    data = await result.data()
    assert data == [1, 2, 3, 4, 5, 6]

    result = await obj3.concat(obj1, obj2)
    data = await result.data()
    assert data == [5, 6, 1, 2, 3, 4]


async def test_insert_same_source_twice_no_duplicate_ids(ctx):
    """Inserting the same source twice produces unique aai_id values."""
    obj_a = await create_object_from_value([1, 2])
    obj_b = await create_object_from_value([3, 4])

    await obj_a.insert(obj_b)
    await obj_a.insert(obj_b)

    data = await obj_a.data()
    assert data == [1, 2, 3, 4, 3, 4]

    result = await get_ch_client().query(f"SELECT aai_id FROM {obj_a.table} ORDER BY aai_id")
    ids = [row[0] for row in result.result_rows]
    assert len(ids) == len(set(ids)), f"Duplicate aai_id values: {ids}"


async def test_concat_same_source_twice_no_duplicate_ids(ctx):
    """Concatenating the same source twice produces unique aai_id values."""
    obj = await create_object_from_value([1, 2])

    result = await obj.concat(obj)
    data = await result.data()
    assert data == [1, 2, 1, 2]

    query_result = await get_ch_client().query(f"SELECT aai_id FROM {result.table} ORDER BY aai_id")
    ids = [row[0] for row in query_result.result_rows]
    assert len(ids) == len(set(ids)), f"Duplicate aai_id values: {ids}"
