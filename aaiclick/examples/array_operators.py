"""
Array operators example for aaiclick.

Demonstrates two ways to do element-wise operations on array Objects:

1. **Normal operators** (e.g. `a + b`) — use row-JOIN internally.
   Validates array lengths and raises ValueError on mismatch.

2. **array_map** (e.g. `a.array_map(b, '+')`) — uses ClickHouse arrayMap.
   Also raises an error on size mismatch (from ClickHouse).
"""

import asyncio

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context


async def example():
    """Run all array operator examples."""
    # ── Normal Operators on Arrays ──────────────────────────────
    print("=" * 60)
    print("PART 1: Normal operators on array Objects")
    print("=" * 60)

    # Arithmetic
    print("\nArithmetic operators")
    print("-" * 60)

    a = await create_object_from_value([10, 20, 30, 40, 50])
    b = await create_object_from_value([2, 4, 5, 8, 10])

    print(f"a: {await a.data()}")
    print(f"b: {await b.data()}\n")

    result = await (a + b)
    print(f"a + b  = {await result.data()}")

    result = await (a - b)
    print(f"a - b  = {await result.data()}")

    result = await (a * b)
    print(f"a * b  = {await result.data()}")

    result = await (a / b)
    print(f"a / b  = {await result.data()}")

    result = await (a // b)
    print(f"a // b = {await result.data()}")

    result = await (a % b)
    print(f"a %% b = {await result.data()}")

    result = await (a ** b)
    print(f"a ** b = {await result.data()}")

    # Scalar broadcast
    print("\nScalar broadcast")
    print("-" * 60)

    a = await create_object_from_value([1, 2, 3, 4, 5])
    print(f"a: {await a.data()}\n")

    result = await (a * 10)
    print(f"a * 10  = {await result.data()}")

    result = await (a + 100)
    print(f"a + 100 = {await result.data()}")

    result = await (100 - a)
    print(f"100 - a = {await result.data()}")

    result = await (2 ** a)
    print(f"2 ** a  = {await result.data()}")

    # Comparison
    print("\nComparison operators")
    print("-" * 60)

    x = await create_object_from_value([1, 5, 10, 15, 20])
    y = await create_object_from_value([5, 5, 8, 20, 20])

    print(f"x: {await x.data()}")
    print(f"y: {await y.data()}\n")

    result = await (x == y)
    print(f"x == y = {await result.data()}")

    result = await (x < y)
    print(f"x < y  = {await result.data()}")

    result = await (x >= y)
    print(f"x >= y = {await result.data()}")

    # Bitwise
    print("\nBitwise operators")
    print("-" * 60)

    m = await create_object_from_value([12, 10, 8])
    n = await create_object_from_value([10, 12, 4])

    print(f"m: {await m.data()}")
    print(f"n: {await n.data()}\n")

    result = await (m & n)
    print(f"m & n = {await result.data()}")

    result = await (m | n)
    print(f"m | n = {await result.data()}")

    result = await (m ^ n)
    print(f"m ^ n = {await result.data()}")

    # Aggregations
    print("\nAggregations")
    print("-" * 60)

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

    # Chained operations
    print("\nChained operations")
    print("-" * 60)

    a = await create_object_from_value([1, 2, 3, 4, 5])
    print(f"a: {await a.data()}\n")

    total = await a.sum()
    normalized = await (a / total)
    print(f"normalized (a / sum(a)): {await normalized.data()}")

    b = await create_object_from_value([10, 20, 30, 40, 50])
    diff = await (b - a)
    mean_diff = await diff.mean()
    print(f"mean(b - a): {await mean_diff.data()}")

    # Size mismatch
    print("\nSize mismatch raises ValueError")
    print("-" * 60)

    a = await create_object_from_value([1, 2, 3])
    c = await create_object_from_value([10, 20])

    try:
        await (a + c)
        print("ERROR: Should have raised!")
    except ValueError as e:
        print(f"a + c: {e}")

    # Concat
    print("\nConcat and insert")
    print("-" * 60)

    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([4, 5, 6])

    print(f"a: {await a.data()}")
    print(f"b: {await b.data()}\n")

    result = await a.concat(b)
    print(f"concat(a, b):          {await result.data()}")

    result = await a.concat(b, [7, 8, 9])
    print(f"concat(a, b, [7,8,9]): {await result.data()}")

    # ── array_map Operators ─────────────────────────────────────
    print("\n\n" + "=" * 60)
    print("PART 2: array_map operators (ClickHouse arrayMap)")
    print("=" * 60)

    # Arithmetic
    print("\nArithmetic via array_map")
    print("-" * 60)

    a = await create_object_from_value([10, 20, 30, 40, 50])
    b = await create_object_from_value([2, 4, 5, 8, 10])

    print(f"a: {await a.data()}")
    print(f"b: {await b.data()}\n")

    result = await a.array_map(b, "+")
    print(f"array_map('+')  = {await result.data()}")

    result = await a.array_map(b, "-")
    print(f"array_map('-')  = {await result.data()}")

    result = await a.array_map(b, "*")
    print(f"array_map('*')  = {await result.data()}")

    result = await a.array_map(b, "/")
    print(f"array_map('/')  = {await result.data()}")

    result = await a.array_map(b, "//")
    print(f"array_map('//') = {await result.data()}")

    result = await a.array_map(b, "%")
    print(f"array_map('%%') = {await result.data()}")

    result = await a.array_map(b, "**")
    print(f"array_map('**') = {await result.data()}")

    # Scalar broadcast via array_map
    print("\nScalar broadcast via array_map")
    print("-" * 60)

    a = await create_object_from_value([1, 2, 3, 4, 5])
    print(f"a: {await a.data()}\n")

    result = await a.array_map(10, "*")
    print(f"array_map(10, '*')  = {await result.data()}")

    result = await a.array_map(100, "+")
    print(f"array_map(100, '+') = {await result.data()}")

    # Comparison via array_map
    print("\nComparison via array_map")
    print("-" * 60)

    x = await create_object_from_value([1, 5, 10, 15, 20])
    y = await create_object_from_value([5, 5, 8, 20, 20])

    print(f"x: {await x.data()}")
    print(f"y: {await y.data()}\n")

    result = await x.array_map(y, "==")
    print(f"array_map('==') = {await result.data()}")

    result = await x.array_map(y, "<")
    print(f"array_map('<')  = {await result.data()}")

    result = await x.array_map(y, ">=")
    print(f"array_map('>=') = {await result.data()}")

    # Bitwise via array_map
    print("\nBitwise via array_map")
    print("-" * 60)

    m = await create_object_from_value([12, 10, 8])
    n = await create_object_from_value([10, 12, 4])

    print(f"m: {await m.data()}")
    print(f"n: {await n.data()}\n")

    result = await m.array_map(n, "&")
    print(f"array_map('&') = {await result.data()}")

    result = await m.array_map(n, "|")
    print(f"array_map('|') = {await result.data()}")

    result = await m.array_map(n, "^")
    print(f"array_map('^') = {await result.data()}")

    # Size mismatch via array_map
    print("\nSize mismatch raises error")
    print("-" * 60)

    a = await create_object_from_value([1, 2, 3])
    c = await create_object_from_value([10, 20])

    try:
        await a.array_map(c, "+")
        print("ERROR: Should have raised!")
    except Exception as e:
        print(f"array_map(c, '+'): {type(e).__name__}: {e}")


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
