"""Tests for ChClient.insert_arrow — arrow-native inserts on the active backend."""

import pyarrow as pa

from aaiclick import create_object_from_value
from aaiclick.data.data_context import get_ch_client


async def test_insert_arrow_round_trip(ctx):
    obj = await create_object_from_value({"a": [1, 2], "s": ["x", "y"]})
    ch = get_ch_client()
    tbl = pa.table({"a": pa.array([3], type=pa.int64()), "s": pa.array(["z"])})
    await ch.insert_arrow(obj.table, tbl)
    result = await ch.query(f"SELECT a, s FROM {obj.table} ORDER BY a")
    assert result.result_rows == [(1, "x"), (2, "y"), (3, "z")]


async def test_insert_arrow_empty_table_is_noop(ctx):
    obj = await create_object_from_value({"a": [1]})
    ch = get_ch_client()
    tbl = pa.table({"a": pa.array([], type=pa.int64())})
    await ch.insert_arrow(obj.table, tbl)
    result = await ch.query(f"SELECT count() FROM {obj.table}")
    assert result.result_rows == [(1,)]


async def test_insert_arrow_fills_default_columns(ctx):
    """Columns absent from the arrow table (e.g. aai_id) get their defaults."""
    obj = await create_object_from_value({"a": [1, 2]}, aai_id=True)
    ch = get_ch_client()
    await ch.insert_arrow(obj.table, pa.table({"a": pa.array([3], type=pa.int64())}))
    result = await ch.query(f"SELECT a FROM {obj.table} WHERE aai_id > 0 ORDER BY a")
    assert result.result_rows == [(1,), (2,), (3,)]


async def test_insert_arrow_dotted_column_names(ctx):
    """Flattened dot / dot-star column names insert correctly."""
    obj = await create_object_from_value([{"x": {"y": 1}, "b": [{"c": 10}]}])
    ch = get_ch_client()
    tbl = pa.table(
        {
            "x.y": pa.array([2], type=pa.int64()),
            "b.*.c": pa.array([[20, 30]], type=pa.list_(pa.int64())),
        }
    )
    await ch.insert_arrow(obj.table, tbl)
    result = await ch.query(f"SELECT `x.y`, `b.*.c` FROM {obj.table} ORDER BY `x.y`")
    assert result.result_rows == [(1, [10]), (2, [20, 30])]
