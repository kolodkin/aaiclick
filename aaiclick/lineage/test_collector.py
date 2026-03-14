"""
Tests for LineageCollector — verify operations produce correct log entries.
"""

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context
from aaiclick.lineage.collector import get_lineage_collector


async def test_lineage_disabled_by_default():
    """When lineage is not enabled, no collector is active."""
    async with data_context():
        assert get_lineage_collector() is None


async def test_lineage_enabled():
    """When lineage=True, a collector is active."""
    async with data_context(lineage=True):
        collector = get_lineage_collector()
        assert collector is not None
        assert collector.operations == []


async def test_lineage_collector_reset_on_exit():
    """Collector is reset when context exits."""
    async with data_context(lineage=True):
        assert get_lineage_collector() is not None
    assert get_lineage_collector() is None


async def test_create_from_value_records_lineage():
    """create_object_from_value records a 'create_from_value' operation."""
    async with data_context(lineage=True):
        obj = await create_object_from_value([1, 2, 3])
        collector = get_lineage_collector()
        ops = collector.operations
        assert len(ops) == 1
        assert ops[0].operation == "create_from_value"
        assert ops[0].result_table == obj.table
        assert ops[0].source_tables == []


async def test_operator_records_lineage():
    """Binary operators record lineage with both source tables."""
    async with data_context(lineage=True):
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([10, 20, 30])
        result = await (a + b)

        collector = get_lineage_collector()
        ops = collector.operations

        # 2 create_from_value + 1 operator
        assert len(ops) == 3
        add_op = ops[2]
        assert add_op.operation == "+"
        assert add_op.result_table == result.table
        assert set(add_op.source_tables) == {a.table, b.table}


async def test_aggregation_records_lineage():
    """Aggregation operations record lineage with source table."""
    async with data_context(lineage=True):
        a = await create_object_from_value([1, 2, 3, 4, 5])
        total = await a.sum()

        collector = get_lineage_collector()
        ops = collector.operations

        # 1 create_from_value + 1 sum
        assert len(ops) == 2
        sum_op = ops[1]
        assert sum_op.operation == "sum"
        assert sum_op.result_table == total.table
        assert sum_op.source_tables == [a.table]


async def test_concat_records_lineage():
    """concat records lineage with all source tables."""
    async with data_context(lineage=True):
        a = await create_object_from_value([1, 2])
        b = await create_object_from_value([3, 4])
        result = await a.concat(b)

        collector = get_lineage_collector()
        ops = collector.operations

        # 2 create_from_value + 1 concat
        assert len(ops) == 3
        concat_op = ops[2]
        assert concat_op.operation == "concat"
        assert concat_op.result_table == result.table
        assert set(concat_op.source_tables) == {a.table, b.table}


async def test_copy_records_lineage():
    """copy records lineage with source table."""
    async with data_context(lineage=True):
        a = await create_object_from_value([1, 2, 3])
        b = await a.copy()

        collector = get_lineage_collector()
        ops = collector.operations

        # 1 create_from_value + 1 copy
        assert len(ops) == 2
        copy_op = ops[1]
        assert copy_op.operation == "copy"
        assert copy_op.result_table == b.table
        assert a.table in copy_op.source_tables


async def test_no_lineage_when_disabled():
    """Operations produce no lineage entries when lineage is disabled."""
    async with data_context():
        a = await create_object_from_value([1, 2, 3])
        b = await create_object_from_value([4, 5, 6])
        await (a + b)

        assert get_lineage_collector() is None


async def test_multi_step_pipeline():
    """A multi-step pipeline records all operations in order."""
    async with data_context(lineage=True):
        prices = await create_object_from_value([100, 200, 300])
        tax_rate = await create_object_from_value(0.1)
        tax = await (prices * tax_rate)
        total = await (prices + tax)
        avg_total = await total.mean()

        collector = get_lineage_collector()
        ops = collector.operations

        op_names = [op.operation for op in ops]
        assert op_names == [
            "create_from_value",  # prices
            "create_from_value",  # tax_rate
            "*",                   # tax = prices * tax_rate
            "+",                   # total = prices + tax
            "mean",                # avg_total = total.mean()
        ]

        # Verify the chain
        assert ops[2].source_tables == [prices.table, tax_rate.table]
        assert ops[3].source_tables == [prices.table, tax.table]
        assert ops[4].source_tables == [total.table]


async def test_collector_clear():
    """Collector.clear() empties the buffer."""
    async with data_context(lineage=True):
        await create_object_from_value([1, 2, 3])
        collector = get_lineage_collector()
        assert len(collector.operations) == 1
        collector.clear()
        assert len(collector.operations) == 0
