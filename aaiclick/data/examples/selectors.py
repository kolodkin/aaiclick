"""
Dict selector examples for aaiclick.

This example demonstrates how to use dict selectors and metadata:
- Selecting fields from dict Objects using __getitem__ syntax
- Getting metadata from Objects and Views
- Copying Views to create new array Objects
- Using selected fields in operations
"""

import asyncio

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context


async def example():
    """Run all dict selector examples."""
    # Example 1: Basic dict selector
    print("Example 1: Basic dict selector")
    print("-" * 50)

    obj = await create_object_from_value({
        'param1': [123, 234, 345],
        'param2': [456, 567, 678]
    })
    print(f"Created dict Object with columns: param1, param2")
    print(f"Full data: {await obj.data()}\n")  # → {'param1': [123, 234, 345], 'param2': [456, 567, 678]}

    # Select param1 using __getitem__
    view_param1 = obj['param1']
    print(f"obj['param1'] creates a View: {view_param1}")
    print(f"View data: {await view_param1.data()}")  # → [123, 234, 345]

    # Select param2
    view_param2 = obj['param2']
    print(f"obj['param2'] data: {await view_param2.data()}")  # → [456, 567, 678]

    # Example 2: Object metadata
    print("\n" + "=" * 50)
    print("Example 2: Object metadata")
    print("-" * 50)

    schema = obj.schema
    print(f"Schema for dict Object:")
    print(f"  table: {schema.table}")
    print(f"  fieldtype: '{schema.fieldtype}' (d = dict)")
    print(f"  columns:")
    for name, col in schema.columns.items():
        print(f"    {name}: type={col.ch_type()}")

    # Example 3: View metadata with selected_fields
    print("\n" + "=" * 50)
    print("Example 3: View metadata with selected_fields")
    print("-" * 50)

    view = obj['param1']
    view_schema = view.schema
    print(f"ViewSchema for obj['param1']:")
    print(f"  table: {view_schema.table}")
    print(f"  fieldtype: '{view_schema.fieldtype}' (source table type)")
    print(f"  selected_fields: {view_schema.selected_fields}")
    print(f"  where: {view_schema.where}")
    print(f"  limit: {view_schema.limit}")
    print(f"  offset: {view_schema.offset}")
    print(f"  order_by: {view_schema.order_by}")

    # Example 4: Copy view to array Object
    print("\n" + "=" * 50)
    print("Example 4: Copy view to array Object")
    print("-" * 50)

    view = obj['param1']
    print(f"View data: {await view.data()}")  # → [123, 234, 345]

    # copy() materializes the view as a new array Object
    arr = await view.copy()
    print(f"Copied to new Object: {arr}")
    print(f"Copied data: {await arr.data()}")  # → [123, 234, 345]

    # Check copied object's metadata
    arr_schema = arr.schema
    print(f"\nCopied Schema:")
    print(f"  table: {arr_schema.table} (new table)")
    print(f"  fieldtype: '{arr_schema.fieldtype}' (a = array)")
    print(f"  columns:")
    for name, col in arr_schema.columns.items():
        print(f"    {name}: type={col.ch_type()}")

    # Example 5: Operations with selected fields
    print("\n" + "=" * 50)
    print("Example 5: Operations with selected fields")
    print("-" * 50)

    data = await create_object_from_value({
        'prices': [10, 20, 30, 40, 50],
        'quantities': [2, 3, 1, 4, 2]
    })
    print(f"Data: {await data.data()}\n")

    prices = data['prices']
    quantities = data['quantities']

    # Multiply selected columns
    totals = await (prices * quantities)
    print(f"prices: {await prices.data()}")  # → [10, 20, 30, 40, 50]
    print(f"quantities: {await quantities.data()}")  # → [2, 3, 1, 4, 2]
    print(f"prices * quantities = {await totals.data()}")  # → [20, 60, 30, 160, 100]

    # Sum of totals
    total_sum = await totals.sum()
    print(f"Sum of totals: {await total_sum.data()}")  # → 370

    # Example 6: Aggregations on selected fields
    print("\n" + "=" * 50)
    print("Example 6: Aggregations on selected fields")
    print("-" * 50)

    metrics = await create_object_from_value({
        'values': [15, 8, 42, 23, 4, 16, 35, 12, 28, 50],
        'weights': [1.0, 0.5, 2.0, 1.5, 0.8, 1.2, 1.8, 0.9, 1.4, 2.5]
    })
    print(f"Metrics data: {await metrics.data()}\n")

    values = metrics['values']
    print(f"values: {await values.data()}")  # → [15, 8, 42, 23, 4, 16, 35, 12, 28, 50]

    # Aggregations on selected field
    min_val = await values.min()
    max_val = await values.max()
    sum_val = await values.sum()
    mean_val = await values.mean()
    count_val = await values.count()

    print(f"  min: {await min_val.data()}")  # → 4
    print(f"  max: {await max_val.data()}")  # → 50
    print(f"  sum: {await sum_val.data()}")  # → 233
    print(f"  mean: {await mean_val.data()}")  # → 23.3
    print(f"  count: {await count_val.data()}")  # → 10

    print("\n" + "=" * 50)
    print("Cleanup: All context-created objects will be cleaned up automatically")
    print("-" * 50)


async def amain():
    """Main entry point that creates data_context() and calls example."""
    async with data_context():
        await example()


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Dict Selectors Example")
    print("=" * 50)
    print("\nNote: This example requires a running ClickHouse server")
    print("      on localhost:8123\n")
    asyncio.run(amain())
