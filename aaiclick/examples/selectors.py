"""
Dict selector examples for aaiclick.

This example demonstrates how to use dict selectors and metadata:
- Selecting fields from dict Objects using __getitem__ syntax
- Getting metadata from Objects and Views
- Copying Views to create new array Objects
- Using selected fields in operations
"""

import asyncio

from aaiclick import DataContext, create_object_from_value


async def example(context):
    """Run all dict selector examples using the provided context."""
    # Example 1: Basic dict selector
    print("Example 1: Basic dict selector")
    print("-" * 50)

    obj = await create_object_from_value({
        'param1': [123, 234, 345],
        'param2': [456, 567, 678]
    })
    print(f"Created dict Object with columns: param1, param2")
    print(f"Full data: {await obj.data()}\n")

    # Select param1 using __getitem__
    view_param1 = obj['param1']
    print(f"obj['param1'] creates a View: {view_param1}")
    print(f"View data: {await view_param1.data()}")

    # Select param2
    view_param2 = obj['param2']
    print(f"obj['param2'] data: {await view_param2.data()}")

    # Example 2: Object metadata
    print("\n" + "=" * 50)
    print("Example 2: Object metadata")
    print("-" * 50)

    meta = await obj.metadata()
    print(f"ObjectMetadata for dict Object:")
    print(f"  table: {meta.table}")
    print(f"  fieldtype: '{meta.fieldtype}' (d = dict)")
    print(f"  columns:")
    for name, col in meta.columns.items():
        print(f"    {name}: type={col.type}, fieldtype={col.fieldtype}")

    # Example 3: View metadata with selected_fields
    print("\n" + "=" * 50)
    print("Example 3: View metadata with selected_fields")
    print("-" * 50)

    view = obj['param1']
    view_meta = await view.metadata()
    print(f"ViewMetadata for obj['param1']:")
    print(f"  table: {view_meta.table}")
    print(f"  fieldtype: '{view_meta.fieldtype}' (source table type)")
    print(f"  selected_fields: {view_meta.selected_fields}")
    print(f"  where: {view_meta.where}")
    print(f"  limit: {view_meta.limit}")
    print(f"  offset: {view_meta.offset}")
    print(f"  order_by: {view_meta.order_by}")

    # Example 4: Copy view to array Object
    print("\n" + "=" * 50)
    print("Example 4: Copy view to array Object")
    print("-" * 50)

    view = obj['param1']
    print(f"View data: {await view.data()}")

    # copy() materializes the view as a new array Object
    arr = await view.copy()
    print(f"Copied to new Object: {arr}")
    print(f"Copied data: {await arr.data()}")

    # Check copied object's metadata
    arr_meta = await arr.metadata()
    print(f"\nCopied ObjectMetadata:")
    print(f"  table: {arr_meta.table} (new table)")
    print(f"  fieldtype: '{arr_meta.fieldtype}' (a = array)")
    print(f"  columns:")
    for name, col in arr_meta.columns.items():
        print(f"    {name}: type={col.type}, fieldtype={col.fieldtype}")

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
    print(f"prices: {await prices.data()}")
    print(f"quantities: {await quantities.data()}")
    print(f"prices * quantities = {await totals.data()}")

    # Sum of totals
    total_sum = await totals.sum()
    print(f"Sum of totals: {await total_sum.data()}")

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
    print(f"values: {await values.data()}")

    # Aggregations on selected field
    min_val = await values.min()
    max_val = await values.max()
    sum_val = await values.sum()
    mean_val = await values.mean()
    count_val = await values.count()

    print(f"  min: {await min_val.data()}")
    print(f"  max: {await max_val.data()}")
    print(f"  sum: {await sum_val.data()}")
    print(f"  mean: {await mean_val.data()}")
    print(f"  count: {await count_val.data()}")

    # Example 7: View metadata with constraints
    print("\n" + "=" * 50)
    print("Example 7: View metadata with constraints")
    print("-" * 50)

    arr_obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    print(f"Array data: {await arr_obj.data()}\n")

    # Create view with multiple constraints
    filtered_view = arr_obj.view(
        where="value > 3",
        order_by="value DESC",
        limit=5,
        offset=1
    )
    print(f"View with constraints:")
    print(f"  data: {await filtered_view.data()}")

    view_meta = await filtered_view.metadata()
    print(f"\nViewMetadata:")
    print(f"  where: '{view_meta.where}'")
    print(f"  order_by: '{view_meta.order_by}'")
    print(f"  limit: {view_meta.limit}")
    print(f"  offset: {view_meta.offset}")
    print(f"  selected_fields: {view_meta.selected_fields}")

    # Example 8: Array metadata
    print("\n" + "=" * 50)
    print("Example 8: Array and scalar metadata")
    print("-" * 50)

    arr = await create_object_from_value([1.5, 2.5, 3.5])
    arr_meta = await arr.metadata()
    print(f"Array [1.5, 2.5, 3.5]:")
    print(f"  fieldtype: '{arr_meta.fieldtype}' (a = array)")
    print(f"  value column type: {arr_meta.columns['value'].type}")

    scalar = await create_object_from_value(42)
    scalar_meta = await scalar.metadata()
    print(f"\nScalar 42:")
    print(f"  fieldtype: '{scalar_meta.fieldtype}' (s = scalar)")
    print(f"  value column type: {scalar_meta.columns['value'].type}")

    print("\n" + "=" * 50)
    print("Cleanup: All context-created objects will be cleaned up automatically")
    print("-" * 50)


async def main():
    """Main entry point that creates context and calls example."""
    async with DataContext() as context:
        await example(context)


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Dict Selectors Example")
    print("=" * 50)
    print("\nNote: This example requires a running ClickHouse server")
    print("      on localhost:8123\n")
    asyncio.run(main())
