"""
Data manipulation example for aaiclick.

This example demonstrates how to use data manipulation methods:
- copy() - Create a copy of an object
- concat() - Concatenate data (creates new object)
- insert() - Insert data in place (modifies existing object)
"""

import asyncio

from aaiclick import Context, create_object_from_value


async def example(context):
    """Run all data manipulation examples using the provided context."""
    # Example 1: Copying objects
    print("Example 1: Copying objects")
    print("-" * 50)

    original = await create_object_from_value([1, 2, 3])
    print(f"Original object: {original}")
    print(f"Original data: {await original.data()}")
    print(f"Original table: {original.table}\n")

    # Copy the object
    copied = await original.copy()
    print(f"Copied object: {copied}")
    print(f"Copied data: {await copied.data()}")
    print(f"Copied table: {copied.table}")
    print(f"Tables are different: {original.table != copied.table}")

    # Example 2: Concatenate - Creates new object (non-mutating)
    print("\n" + "=" * 50)
    print("Example 2: Concatenate (non-mutating)")
    print("-" * 50)

    obj_a = await create_object_from_value([1, 2, 3])
    obj_b = await create_object_from_value([4, 5, 6])

    print(f"Object A: {await obj_a.data()}")
    print(f"Object B: {await obj_b.data()}\n")

    # Concat creates a new object
    result = await obj_a.concat(obj_b)
    print(f"After concat: {await result.data()}")
    print(f"Original A unchanged: {await obj_a.data()}")
    print(f"Original B unchanged: {await obj_b.data()}")
    print(f"Result is new object: {result.table != obj_a.table}")

    # Example 3: Concatenate with scalar value
    print("\n" + "=" * 50)
    print("Example 3: Concatenate with scalar value")
    print("-" * 50)

    obj = await create_object_from_value([10, 20, 30])
    print(f"Original array: {await obj.data()}\n")

    # Concat with scalar
    result_scalar = await obj.concat(40)
    print(f"After concat(40): {await result_scalar.data()}")
    print(f"Original unchanged: {await obj.data()}")

    # Example 4: Concatenate with list value
    print("\n" + "=" * 50)
    print("Example 4: Concatenate with list value")
    print("-" * 50)

    obj = await create_object_from_value([1, 2])
    print(f"Original array: {await obj.data()}\n")

    # Concat with list
    result_list = await obj.concat([3, 4, 5])
    print(f"After concat([3, 4, 5]): {await result_list.data()}")
    print(f"Original unchanged: {await obj.data()}")

    # Example 5: Insert - Modifies in place (mutating)
    print("\n" + "=" * 50)
    print("Example 5: Insert (mutating - modifies in place)")
    print("-" * 50)

    obj_x = await create_object_from_value([100, 200, 300])
    obj_y = await create_object_from_value([400, 500, 600])

    print(f"Object X before: {await obj_x.data()}")
    print(f"Object Y: {await obj_y.data()}")
    print(f"Table X before: {obj_x.table}\n")

    # Insert modifies obj_x in place
    await obj_x.insert(obj_y)
    print(f"Object X after insert: {await obj_x.data()}")
    print(f"Table X after (same): {obj_x.table}")
    print(f"Object Y unchanged: {await obj_y.data()}")

    # Example 6: Insert with scalar value
    print("\n" + "=" * 50)
    print("Example 6: Insert with scalar value")
    print("-" * 50)

    obj = await create_object_from_value([1, 2, 3])
    print(f"Before insert: {await obj.data()}")
    print(f"Table: {obj.table}\n")

    await obj.insert(4)
    print(f"After insert(4): {await obj.data()}")
    print(f"Table (same): {obj.table}")

    # Example 7: Insert with list value
    print("\n" + "=" * 50)
    print("Example 7: Insert with list value")
    print("-" * 50)

    obj = await create_object_from_value([10, 20])
    print(f"Before insert: {await obj.data()}")
    print(f"Table: {obj.table}\n")

    await obj.insert([30, 40, 50])
    print(f"After insert([30, 40, 50]): {await obj.data()}")
    print(f"Table (same): {obj.table}")

    # Example 8: Multiple inserts
    print("\n" + "=" * 50)
    print("Example 8: Multiple consecutive inserts")
    print("-" * 50)

    obj = await create_object_from_value([1, 2])
    print(f"Initial: {await obj.data()}")

    await obj.insert(3)
    print(f"After insert(3): {await obj.data()}")

    await obj.insert([4, 5])
    print(f"After insert([4, 5]): {await obj.data()}")

    await obj.insert(6)
    print(f"After insert(6): {await obj.data()}")

    # Example 9: Comparing concat vs insert
    print("\n" + "=" * 50)
    print("Example 9: Comparing concat vs insert")
    print("-" * 50)

    # Using concat (non-mutating)
    a1 = await create_object_from_value([1, 2, 3])
    print(f"concat approach:")
    print(f"  Original a1: {await a1.data()}, table: {a1.table}")
    a1_new = await a1.concat([4, 5])
    print(f"  After concat: {await a1_new.data()}, table: {a1_new.table}")
    print(f"  Original a1 unchanged: {await a1.data()}, table: {a1.table}\n")

    # Using insert (mutating)
    a2 = await create_object_from_value([1, 2, 3])
    print(f"insert approach:")
    print(f"  Original a2: {await a2.data()}, table: {a2.table}")
    await a2.insert([4, 5])
    print(f"  After insert: {await a2.data()}, table: {a2.table} (same)")

    # Example 10: Real-world scenario - Building a dataset
    print("\n" + "=" * 50)
    print("Example 10: Real-world scenario - Building a dataset")
    print("-" * 50)

    # Start with initial data
    dataset = await create_object_from_value([10.5, 20.3, 30.7])
    print(f"Initial dataset: {await dataset.data()}")

    # Add more measurements in place
    await dataset.insert([15.2, 25.8])
    print(f"Added batch 1: {await dataset.data()}")

    await dataset.insert(35.9)
    print(f"Added single value: {await dataset.data()}")

    await dataset.insert([12.1, 22.4, 32.6])
    print(f"Added batch 2: {await dataset.data()}")

    # Calculate statistics on final dataset
    print(f"\nFinal dataset statistics:")
    print(f"  Count: {len(await dataset.data())}")
    print(f"  Min: {await dataset.min():.2f}")
    print(f"  Max: {await dataset.max():.2f}")
    print(f"  Mean: {await dataset.mean():.2f}")
    print(f"  Std: {await dataset.std():.2f}")

    # Note: All objects created via context are automatically cleaned up when context exits
    print("\n" + "=" * 50)
    print("Cleanup: All context-created objects will be cleaned up automatically")
    print("-" * 50)


async def main():
    """Main entry point that creates context and calls example."""
    async with Context() as context:
        await example(context)


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Data Manipulation Example")
    print("=" * 50)
    print("\nNote: This example requires a running ClickHouse server")
    print("      on localhost:8123\n")
    asyncio.run(main())
