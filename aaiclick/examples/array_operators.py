"""
Array operators example for aaiclick.

This example demonstrates element-wise operations on array Objects,
including binary operators, scalar broadcast, aggregations, array_map,
and concat/insert.
"""

import asyncio

from aaiclick import FIELDTYPE_ARRAY, Object, create_object_from_value
from aaiclick.data.data_context import data_context


async def example():
    """Run all array operator examples."""
    # Example 1: Basic array arithmetic
    print("Example 1: Array arithmetic operators")
    print("-" * 50)

    a = await create_object_from_value([10, 20, 30, 40, 50])
    b = await create_object_from_value([2, 4, 5, 8, 10])

    print(f"a: {await a.data()}")
    print(f"b: {await b.data()}\n")

    result = await (a + b)
    print(f"a + b = {await result.data()}")

    result = await (a - b)
    print(f"a - b = {await result.data()}")

    result = await (a * b)
    print(f"a * b = {await result.data()}")

    result = await (a / b)
    print(f"a / b = {await result.data()}")

    result = await (a // b)
    print(f"a // b = {await result.data()}")

    result = await (a % b)
    print(f"a %% b = {await result.data()}")

    result = await (a ** b)
    print(f"a ** b = {await result.data()}")

    # Example 2: Scalar broadcast
    print("\n" + "=" * 50)
    print("Example 2: Scalar broadcast")
    print("-" * 50)

    a = await create_object_from_value([1, 2, 3, 4, 5])
    print(f"a: {await a.data()}\n")

    result = await (a * 10)
    print(f"a * 10 = {await result.data()}")

    result = await (a + 100)
    print(f"a + 100 = {await result.data()}")

    result = await (100 - a)
    print(f"100 - a = {await result.data()}")

    result = await (2 ** a)
    print(f"2 ** a = {await result.data()}")

    # Example 3: Comparison operators
    print("\n" + "=" * 50)
    print("Example 3: Array comparison operators")
    print("-" * 50)

    x = await create_object_from_value([1, 5, 10, 15, 20])
    y = await create_object_from_value([5, 5, 8, 20, 20])

    print(f"x: {await x.data()}")
    print(f"y: {await y.data()}\n")

    result = await (x == y)
    print(f"x == y: {await result.data()}")

    result = await (x < y)
    print(f"x < y:  {await result.data()}")

    result = await (x >= y)
    print(f"x >= y: {await result.data()}")

    # Example 4: Aggregations on arrays
    print("\n" + "=" * 50)
    print("Example 4: Array aggregations")
    print("-" * 50)

    a = await create_object_from_value([10, 20, 30, 40, 50])
    print(f"a: {await a.data()}\n")

    total = await a.sum()
    print(f"sum:  {await total.data()}")

    avg = await a.mean()
    print(f"mean: {await avg.data()}")

    mn = await a.min()
    print(f"min:  {await mn.data()}")

    mx = await a.max()
    print(f"max:  {await mx.data()}")

    sd = await a.std()
    print(f"std:  {await sd.data()}")

    # Example 5: Chained operations (all computation in ClickHouse)
    print("\n" + "=" * 50)
    print("Example 5: Chained operations")
    print("-" * 50)

    a = await create_object_from_value([1, 2, 3, 4, 5])
    print(f"a: {await a.data()}\n")

    # Normalize: divide each element by the sum
    total = await a.sum()
    normalized = await (a / total)
    print(f"normalized (a / sum(a)): {await normalized.data()}")

    # Mean difference between two arrays
    b = await create_object_from_value([10, 20, 30, 40, 50])
    diff = await (b - a)
    mean_diff = await diff.mean()
    print(f"mean(b - a): {await mean_diff.data()}")

    # Example 6: array_map (ClickHouse arrayMap)
    print("\n" + "=" * 50)
    print("Example 6: array_map operator")
    print("-" * 50)

    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])

    print(f"a: {await a.data()}")
    print(f"b: {await b.data()}\n")

    result = await a.array_map(b, "+")
    print(f"array_map(a, b, '+'): {await result.data()}")

    result = await a.array_map(5, "*")
    print(f"array_map(a, 5, '*'): {await result.data()}")

    # Example 7: Concat and insert
    print("\n" + "=" * 50)
    print("Example 7: Concat and insert")
    print("-" * 50)

    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([4, 5, 6])

    print(f"a: {await a.data()}")
    print(f"b: {await b.data()}\n")

    result = await a.concat(b)
    print(f"concat(a, b): {await result.data()}")

    result = await a.concat(b, [7, 8, 9])
    print(f"concat(a, b, [7,8,9]): {await result.data()}")

    # Example 8: Size mismatch raises error
    print("\n" + "=" * 50)
    print("Example 8: Size mismatch raises ValueError")
    print("-" * 50)

    a = await create_object_from_value([1, 2, 3])
    c = await create_object_from_value([10, 20])

    try:
        await (a + c)
        print("ERROR: Should have raised!")
    except ValueError as e:
        print(f"a + c with mismatched sizes: {e}")

    # Example 9: Bitwise operators on arrays
    print("\n" + "=" * 50)
    print("Example 9: Bitwise operators on arrays")
    print("-" * 50)

    m = await create_object_from_value([12, 10, 8])
    n = await create_object_from_value([10, 12, 4])

    print(f"m: {await m.data()}")
    print(f"n: {await n.data()}\n")

    result = await (m & n)
    print(f"m & n: {await result.data()}")

    result = await (m | n)
    print(f"m | n: {await result.data()}")

    result = await (m ^ n)
    print(f"m ^ n: {await result.data()}")


async def amain():
    """Main entry point that creates data_context() and calls example."""
    async with data_context():
        await example()


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Array Operators Example")
    print("=" * 50)
    print("\nNote: This example requires a running ClickHouse server")
    print("      on localhost:8123\n")
    asyncio.run(amain())
