"""
Statistics example for aaiclick.

This example demonstrates how to use the statistical methods (min, max, sum, mean, std)
on Objects containing numeric data within a data_context().
"""

import asyncio

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context


async def example():
    """Run all statistics examples."""
    # Example 1: Basic statistics on a simple dataset
    print("Example 1: Basic statistics on a simple dataset")
    print("-" * 50)

    data = [10.0, 20.0, 30.0, 40.0, 50.0]
    obj = await create_object_from_value(data)
    print(f"Created object: {obj}")
    print(f"Data: {data}\n")

    min_val = await (await obj.min()).data()
    max_val = await (await obj.max()).data()
    sum_val = await (await obj.sum()).data()
    mean_val = await (await obj.mean()).data()
    std_val = await (await obj.std()).data()

    print(f"Minimum:          {min_val}")  # → 10.0
    print(f"Maximum:          {max_val}")  # → 50.0
    print(f"Sum:              {sum_val}")  # → 150.0
    print(f"Mean:             {mean_val}")  # → 30.0
    print(f"Std Deviation:    {std_val}")  # → 14.142135623730951

    # Example 2: Statistics on integer data
    print("\n" + "=" * 50)
    print("Example 2: Statistics on integer data")
    print("-" * 50)

    int_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    obj_int = await create_object_from_value(int_data)
    print(f"Created object: {obj_int}")
    print(f"Data: {int_data}\n")

    print(f"Minimum:          {await (await obj_int.min()).data()}")  # → 1
    print(f"Maximum:          {await (await obj_int.max()).data()}")  # → 10
    print(f"Sum:              {await (await obj_int.sum()).data()}")  # → 55
    print(f"Mean:             {await (await obj_int.mean()).data()}")  # → 5.5
    print(f"Std Deviation:    {await (await obj_int.std()).data()}")  # → 2.8722813232690143

    # Example 3: Statistics on operation results
    print("\n" + "=" * 50)
    print("Example 3: Statistics on operation results")
    print("-" * 50)

    data_a = [100.0, 200.0, 300.0]
    data_b = [50.0, 100.0, 150.0]

    obj_a = await create_object_from_value(data_a)
    obj_b = await create_object_from_value(data_b)

    print(f"Data A: {data_a}")
    print(f"Data B: {data_b}\n")

    # Add the objects
    obj_sum = await (obj_a + obj_b)
    sum_data = await obj_sum.data()
    print(f"After addition: {sum_data}")  # → [150.0, 300.0, 450.0]
    print(f"Mean of sum:      {await (await obj_sum.mean()).data()}")  # → 300.0
    print(f"Std of sum:       {await (await obj_sum.std()).data()}\n")  # → 122.47...

    # Subtract the objects
    obj_diff = await (obj_a - obj_b)
    diff_data = await obj_diff.data()
    print(f"After subtraction: {diff_data}")  # → [50.0, 100.0, 150.0]
    print(f"Mean of diff:     {await (await obj_diff.mean()).data()}")  # → 100.0
    print(f"Std of diff:      {await (await obj_diff.std()).data()}")  # → 40.82...

    # Clean up operation results (obj_a and obj_b cleaned by context)

    # Example 4: Real-world scenario - Temperature analysis
    print("\n" + "=" * 50)
    print("Example 4: Real-world scenario - Temperature analysis")
    print("-" * 50)

    temperatures = [72.5, 75.0, 68.3, 71.2, 74.8, 69.5, 73.1, 76.2]
    obj_temp = await create_object_from_value(temperatures)

    print(f"Daily temperatures (°F): {temperatures}\n")

    min_temp = await (await obj_temp.min()).data()
    max_temp = await (await obj_temp.max()).data()
    avg_temp = await (await obj_temp.mean()).data()
    std_temp = await (await obj_temp.std()).data()

    print(f"Temperature Analysis:")
    print(f"  Minimum:          {min_temp:.1f}°F")  # → 68.3°F
    print(f"  Maximum:          {max_temp:.1f}°F")  # → 76.2°F
    print(f"  Average:          {avg_temp:.1f}°F")  # → 72.6°F
    print(f"  Std Deviation:    {std_temp:.2f}°F")  # → 2.60°F
    print(f"  Temperature Range: {max_temp - min_temp:.1f}°F")

    # Example 5: Single value edge case
    print("\n" + "=" * 50)
    print("Example 5: Single value edge case")
    print("-" * 50)

    single_value = [42.0]
    obj_single = await create_object_from_value(single_value)
    print(f"Single value data: {single_value}\n")

    print(f"Minimum:          {await (await obj_single.min()).data()}")  # → 42.0
    print(f"Maximum:          {await (await obj_single.max()).data()}")  # → 42.0
    print(f"Sum:              {await (await obj_single.sum()).data()}")  # → 42.0
    print(f"Mean:             {await (await obj_single.mean()).data()}")  # → 42.0
    print(f"Std Deviation:    {await (await obj_single.std()).data()} (no variation)")  # → 0.0

    # Note: All objects created via context are automatically cleaned up when context exits
    print("\n" + "=" * 50)
    print("Cleanup: All context-created objects will be cleaned up automatically")
    print("-" * 50)


async def amain():
    """Main entry point that creates data_context() and calls example."""
    async with data_context():
        await example()


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Statistics Example")
    print("=" * 50)
    print("\nNote: This example requires a running ClickHouse server")
    print("      on localhost:8123\n")
    asyncio.run(amain())
