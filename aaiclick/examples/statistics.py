"""
Statistics example for aaiclick.

This example demonstrates how to use the statistical methods (min, max, sum, mean, std)
on Objects containing numeric data within a DataContext.
"""

import asyncio

from aaiclick import DataContext, create_object_from_value


async def example(context):
    """Run all statistics examples using the provided context."""
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

    print(f"Minimum:          {min_val}")
    print(f"Maximum:          {max_val}")
    print(f"Sum:              {sum_val}")
    print(f"Mean:             {mean_val}")
    print(f"Std Deviation:    {std_val}")

    # Example 2: Statistics on integer data
    print("\n" + "=" * 50)
    print("Example 2: Statistics on integer data")
    print("-" * 50)

    int_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    obj_int = await create_object_from_value(int_data)
    print(f"Created object: {obj_int}")
    print(f"Data: {int_data}\n")

    print(f"Minimum:          {await (await obj_int.min()).data()}")
    print(f"Maximum:          {await (await obj_int.max()).data()}")
    print(f"Sum:              {await (await obj_int.sum()).data()}")
    print(f"Mean:             {await (await obj_int.mean()).data()}")
    print(f"Std Deviation:    {await (await obj_int.std()).data()}")

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
    print(f"After addition: {sum_data}")
    print(f"Mean of sum:      {await (await obj_sum.mean()).data()}")
    print(f"Std of sum:       {await (await obj_sum.std()).data()}\n")

    # Subtract the objects
    obj_diff = await (obj_a - obj_b)
    diff_data = await obj_diff.data()
    print(f"After subtraction: {diff_data}")
    print(f"Mean of diff:     {await (await obj_diff.mean()).data()}")
    print(f"Std of diff:      {await (await obj_diff.std()).data()}")

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
    print(f"  Minimum:          {min_temp:.1f}°F")
    print(f"  Maximum:          {max_temp:.1f}°F")
    print(f"  Average:          {avg_temp:.1f}°F")
    print(f"  Std Deviation:    {std_temp:.2f}°F")
    print(f"  Temperature Range: {max_temp - min_temp:.1f}°F")

    # Example 5: Single value edge case
    print("\n" + "=" * 50)
    print("Example 5: Single value edge case")
    print("-" * 50)

    single_value = [42.0]
    obj_single = await create_object_from_value(single_value)
    print(f"Single value data: {single_value}\n")

    print(f"Minimum:          {await (await obj_single.min()).data()}")
    print(f"Maximum:          {await (await obj_single.max()).data()}")
    print(f"Sum:              {await (await obj_single.sum()).data()}")
    print(f"Mean:             {await (await obj_single.mean()).data()}")
    print(f"Std Deviation:    {await (await obj_single.std()).data()} (no variation)")

    # Note: All objects created via context are automatically cleaned up when context exits
    print("\n" + "=" * 50)
    print("Cleanup: All context-created objects will be cleaned up automatically")
    print("-" * 50)


async def amain():
    """Main entry point that creates context and calls example."""
    async with DataContext() as context:
        await example(context)


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Statistics Example")
    print("=" * 50)
    print("\nNote: This example requires a running ClickHouse server")
    print("      on localhost:8123\n")
    asyncio.run(main())
