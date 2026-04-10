"""
Order By example for aaiclick.

This example demonstrates how to use the ``order_by`` parameter when creating
Objects so the underlying ClickHouse table is physically sorted.

``aai_id`` is always appended as the last ORDER BY column.
Requires a MergeTree-family engine (Memory ignores ORDER BY).
"""

import asyncio

from aaiclick import ORIENT_RECORDS, create_object_from_value
from aaiclick.data import ENGINE_MERGE_TREE
from aaiclick.data.data_context import data_context, get_ch_client


async def example():
    """Run all order_by examples."""
    ch = get_ch_client()

    # Example 1: Single column order_by
    print("Example 1: Single column order_by")
    print("-" * 50)

    sales = await create_object_from_value(
        {
            "date": ["2024-01-03", "2024-01-01", "2024-01-02"],
            "amount": [300, 100, 200],
        },
        order_by=["date"],
    )
    print(f"Data: {await sales.data(orient=ORIENT_RECORDS)}")

    result = await ch.query(
        f"SELECT engine, sorting_key FROM system.tables WHERE name = '{sales.table}'"
    )
    engine, sorting_key = result.result_rows[0]
    print(f"Engine: {engine}, ORDER BY: {sorting_key}")  # → Engine: MergeTree, ORDER BY: date, aai_id

    # Example 2: Multiple column order_by
    print("\n" + "=" * 50)
    print("Example 2: Multiple column order_by")
    print("-" * 50)

    events = await create_object_from_value(
        {
            "category": ["b", "a", "a", "b"],
            "date": ["2024-01-02", "2024-01-02", "2024-01-01", "2024-01-01"],
            "value": [4, 2, 1, 3],
        },
        order_by=["category", "date"],
    )
    print(f"Data: {await events.data(orient=ORIENT_RECORDS)}")

    result = await ch.query(
        f"SELECT sorting_key FROM system.tables WHERE name = '{events.table}'"
    )
    print(f"ORDER BY: {result.result_rows[0][0]}")  # → ORDER BY: category, date, aai_id

    # Example 3: No order_by (default behaviour)
    print("\n" + "=" * 50)
    print("Example 3: No order_by (default)")
    print("-" * 50)

    plain = await create_object_from_value(
        {"x": [3, 1, 2], "y": [30, 10, 20]},
    )
    print(f"Data: {await plain.data(orient=ORIENT_RECORDS)}")

    result = await ch.query(
        f"SELECT engine FROM system.tables WHERE name = '{plain.table}'"
    )
    print(f"Engine: {result.result_rows[0][0]} (no ORDER BY)")


async def amain():
    """Main entry point that creates data_context() and calls example."""
    async with data_context(engine=ENGINE_MERGE_TREE):
        await example()


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Order By Example")
    print("=" * 50)
    print()
    asyncio.run(amain())
