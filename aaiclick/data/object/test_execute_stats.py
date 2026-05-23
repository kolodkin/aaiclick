"""Tests for the discard terminal ``Object.execute()`` and ``Object.stats``.

Covers the discard terminal (run a query, keep nothing, return ``QueryStats``)
and ``.stats`` population on query-born objects (``.copy()`` and materialized
``LazyOperator`` results).

Backend-agnostic: the default backend (chdb) fills ``read_rows`` / ``read_bytes``
/ ``elapsed_s`` and leaves ``result_rows`` / ``written_rows`` / ``written_bytes``
``None``; the distributed HTTP backend fills all six. Assertions check the
fields a backend supports and tolerate ``None`` elsewhere (see the availability
matrix in ``docs/object.md``).
"""

from aaiclick import QueryStats, create_object_from_value
from aaiclick.data.data_context import get_ch_client


def _assert_scanned(stats: QueryStats, expected_rows: int) -> None:
    """Assert the scan-side fields both backends populate look sane."""
    assert isinstance(stats, QueryStats)
    assert stats.read_rows == expected_rows
    assert stats.read_bytes is not None and stats.read_bytes > 0
    assert stats.elapsed_s is not None and stats.elapsed_s >= 0


async def _table_names() -> set[str]:
    result = await get_ch_client().query("SELECT name FROM system.tables WHERE database = currentDatabase()")
    return {row[0] for row in result.result_rows}


async def test_execute_returns_stats_with_read_rows(ctx):
    obj = await create_object_from_value(list(range(100)))
    stats = await obj.execute()
    _assert_scanned(stats, 100)


async def test_execute_creates_no_table_and_returns_no_rows(ctx):
    obj = await create_object_from_value(list(range(50)))
    before = await _table_names()
    stats = await obj.execute()
    after = await _table_names()
    assert after == before  # FORMAT Null materializes nothing
    assert stats.result_rows in (None, 0)  # no rows produced / transported
    # The source table is untouched and still readable.
    assert sorted(await obj.data()) == list(range(50))


async def test_execute_honors_where(ctx):
    obj = await create_object_from_value(list(range(100)))
    view = obj.view(where="value < 10")
    stats = await view.execute()
    assert isinstance(stats, QueryStats)
    assert stats.elapsed_s is not None
    # WHERE runs without error and leaves the source intact.
    assert sorted(await obj.data()) == list(range(100))


async def test_execute_honors_limit(ctx):
    obj = await create_object_from_value(list(range(1000)))
    stats = await obj.execute(limit=10)
    assert isinstance(stats, QueryStats)
    assert stats.elapsed_s is not None


async def test_execute_on_view_measures_projection(ctx):
    obj = await create_object_from_value({"x": [1, 2, 3, 4], "y": [5, 6, 7, 8]})
    stats = await obj["x"].execute()
    _assert_scanned(stats, 4)


async def test_copy_stats_populated(ctx):
    obj = await create_object_from_value(list(range(40)))
    copied = await obj.copy()
    cs = await copied.stats()
    assert cs is not None
    _assert_scanned(cs, 40)
    # written_rows is HTTP-only; chdb leaves it None.
    assert cs.written_rows is None or cs.written_rows == 40


async def test_copy_selected_fields_stats_populated(ctx):
    obj = await create_object_from_value({"x": [1, 2, 3], "y": [4, 5, 6]})
    copied = await obj["x"].copy()
    cs = await copied.stats()
    assert cs is not None
    _assert_scanned(cs, 3)


async def test_stats_none_on_plain_object(ctx):
    obj = await create_object_from_value([1, 2, 3])
    assert await obj.stats() is None


async def test_stats_none_on_unexecuted_view(ctx):
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    view = obj.view(where="value > 2")
    assert await view.stats() is None


async def test_lazy_binary_operator_stats(ctx):
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    b = await create_object_from_value([4, 5, 6], aai_id=True)
    result = await (a + b)
    assert await result.data() == [5, 7, 9]
    rs = await result.stats()
    assert rs is not None
    assert rs.read_rows is not None and rs.read_rows > 0


async def test_lazy_stats_triggers_materialization(ctx):
    """await lazy.stats() materializes the un-awaited LazyOperator, mirroring .data()."""
    a = await create_object_from_value([1, 2, 3], aai_id=True)
    b = await create_object_from_value([4, 5, 6], aai_id=True)
    lazy = a + b
    rs = await lazy.stats()
    assert rs is not None
    assert rs.read_rows is not None and rs.read_rows > 0


async def test_lazy_aggregation_stats(ctx):
    obj = await create_object_from_value(list(range(10)))
    result = await obj.sum()
    assert await result.data() == 45
    rs = await result.stats()
    assert rs is not None
    _assert_scanned(rs, 10)


async def test_lazy_unary_transform_stats(ctx):
    obj = await create_object_from_value(["Hello", "World"])
    result = await obj.lower()
    assert await result.data() == ["hello", "world"]
    rs = await result.stats()
    assert rs is not None
    assert rs.read_rows is not None and rs.read_rows > 0
