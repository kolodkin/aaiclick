"""
Unary transform operators example for aaiclick.

This example demonstrates Object-level transform operators that apply
ClickHouse functions element-wise and return new Objects. These are the
Object equivalents of View domain helpers (with_year, with_lower, etc.).
"""

import asyncio
from datetime import datetime, timezone

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context


async def example():
    """Run all transform examples."""
    # Example 1: Date/time transforms
    print("Example 1: Date/time transforms")
    print("-" * 50)

    dates = [
        datetime(2023, 3, 15, tzinfo=timezone.utc),
        datetime(2024, 7, 4, tzinfo=timezone.utc),
        datetime(2025, 12, 25, tzinfo=timezone.utc),
    ]
    obj = await create_object_from_value(dates)
    print(f"Dates: {dates}")

    years = await obj.year()
    print(f"year():        {await years.data()}")

    months = await obj.month()
    print(f"month():       {await months.data()}")

    dows = await obj.day_of_week()
    print(f"day_of_week(): {await dows.data()}")

    # Example 2: String transforms
    print("\n" + "=" * 50)
    print("Example 2: String transforms")
    print("-" * 50)

    strings = ["  Hello World  ", " FOO ", "  bar  "]
    obj = await create_object_from_value(strings)
    print(f"Strings: {strings}")

    print(f"lower():  {await (await obj.lower()).data()}")
    print(f"upper():  {await (await obj.upper()).data()}")
    print(f"trim():   {await (await obj.trim()).data()}")
    print(f"length(): {await (await obj.length()).data()}")

    # Example 3: Math transforms
    print("\n" + "=" * 50)
    print("Example 3: Math transforms")
    print("-" * 50)

    numbers = [-9, -4, 0, 4, 16]
    obj = await create_object_from_value(numbers)
    print(f"Numbers: {numbers}")

    print(f"abs():  {await (await obj.abs()).data()}")

    positives = await create_object_from_value([1, 2, 4, 8, 16])
    print(f"\nPositives: {await positives.data()}")
    print(f"log2(): {await (await positives.log2()).data()}")
    print(f"sqrt(): {await (await positives.sqrt()).data()}")

    # Example 4: Chaining transforms with other operators
    print("\n" + "=" * 50)
    print("Example 4: Chaining transforms with aggregations")
    print("-" * 50)

    dates = [
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 6, 15, tzinfo=timezone.utc),
        datetime(2025, 3, 10, tzinfo=timezone.utc),
    ]
    obj = await create_object_from_value(dates)
    years = await obj.year()
    unique_years = await years.unique()
    print(f"Unique years: {sorted(await unique_years.data())}")

    year_sum = await years.sum()
    print(f"Sum of years: {await year_sum.data()}")

    words = ["apple", "banana", "cherry", "date"]
    obj = await create_object_from_value(words)
    lengths = await obj.length()
    max_len = await lengths.max()
    print(f"Longest word length: {await max_len.data()}")


async def amain():
    async with data_context():
        await example()


if __name__ == "__main__":
    asyncio.run(amain())
