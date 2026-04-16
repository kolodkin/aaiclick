"""
Unary transform operators example for aaiclick.

This example demonstrates Object-level transform operators that apply
ClickHouse functions element-wise and return new Objects. These are the
Object equivalents of View domain helpers (with_year, with_lower, etc.).
"""

import asyncio
from datetime import datetime, timezone

from aaiclick import create_object_from_value, literal
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
    print(f"year():        {await years.data()}")  # → [2023, 2024, 2025]

    months = await obj.month()
    print(f"month():       {await months.data()}")  # → [3, 7, 12]

    dows = await obj.day_of_week()
    print(f"day_of_week(): {await dows.data()}")  # → [3, 4, 4]

    # Example 2: String transforms
    print("\n" + "=" * 50)
    print("Example 2: String transforms")
    print("-" * 50)

    strings = ["  Hello World  ", " FOO ", "  bar  "]
    obj = await create_object_from_value(strings)
    print(f"Strings: {strings}")

    print(f"lower():  {await (await obj.lower()).data()}")  # → ['  hello world  ', ' foo ', '  bar  ']
    print(f"upper():  {await (await obj.upper()).data()}")  # → ['  HELLO WORLD  ', ' FOO ', '  BAR  ']
    print(f"trim():   {await (await obj.trim()).data()}")  # → ['Hello World', 'FOO', 'bar']
    print(f"length(): {await (await obj.length()).data()}")  # → [15, 5, 7]

    # Example 3: Math transforms
    print("\n" + "=" * 50)
    print("Example 3: Math transforms")
    print("-" * 50)

    numbers = [-9, -4, 0, 4, 16]
    obj = await create_object_from_value(numbers)
    print(f"Numbers: {numbers}")

    print(f"abs():  {await (await obj.abs()).data()}")  # → [9.0, 4.0, 0.0, 4.0, 16.0]

    positives = await create_object_from_value([1, 2, 4, 8, 16])
    print(f"\nPositives: {await positives.data()}")
    print(f"log2(): {await (await positives.log2()).data()}")  # → [0.0, 1.0, 2.0, 3.0, 4.0]
    print(f"sqrt(): {await (await positives.sqrt()).data()}")  # → [1.0, 1.414..., 2.0, 2.828..., 4.0]

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
    print(f"Unique years: {sorted(await unique_years.data())}")  # → [2024, 2025]

    year_sum = await years.sum()
    print(f"Sum of years: {await year_sum.data()}")  # → 6073

    words = ["apple", "banana", "cherry", "date"]
    obj = await create_object_from_value(words)
    lengths = await obj.length()
    max_len = await lengths.max()
    print(f"Longest word length: {await max_len.data()}")  # → 6

    # Example 5: literal() — constant computed columns
    print("\n" + "=" * 50)
    print("Example 5: literal() helper")
    print("-" * 50)

    obj = await create_object_from_value([{"city": "NYC"}, {"city": "London"}])
    view = obj.with_columns(
        {
            "source": literal("survey_2024", "String"),
            "active": literal(True, "UInt8"),
            "weight": literal(1.0, "Float64"),
        }
    )
    result = await view.data()
    print(f"source: {result['source']}")  # → ['survey_2024', 'survey_2024']
    print(f"active: {result['active']}")  # → [1, 1]
    print(f"weight: {result['weight']}")  # → [1.0, 1.0]


async def amain():
    async with data_context():
        await example()


if __name__ == "__main__":
    asyncio.run(amain())
