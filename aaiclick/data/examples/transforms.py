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
from aaiclick.example_runner import section


async def example():
    """Run all transform examples."""
    section("Example 1: Date/time transforms")

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

    section("Example 2: String transforms")

    strings = ["  Hello World  ", " FOO ", "  bar  "]
    obj = await create_object_from_value(strings)
    print(f"Strings: {strings}")

    print(f"lower():  {await obj.lower().data()}")  # → ['  hello world  ', ' foo ', '  bar  ']
    print(f"upper():  {await obj.upper().data()}")  # → ['  HELLO WORLD  ', ' FOO ', '  BAR  ']
    print(f"trim():   {await obj.trim().data()}")  # → ['Hello World', 'FOO', 'bar']
    print(f"length(): {await obj.length().data()}")  # → [15, 5, 7]

    section("Example 3: Math transforms")

    numbers = [-9, -4, 0, 4, 16]
    obj = await create_object_from_value(numbers)
    print(f"Numbers: {numbers}")

    print(f"abs():  {await obj.abs().data()}")  # → [9.0, 4.0, 0.0, 4.0, 16.0]

    positives = await create_object_from_value([1, 2, 4, 8, 16])
    print(f"\nPositives: {await positives.data()}")
    print(f"log2(): {await positives.log2().data()}")  # → [0.0, 1.0, 2.0, 3.0, 4.0]
    print(f"sqrt(): {await positives.sqrt().data()}")  # → [1.0, 1.414..., 2.0, 2.828..., 4.0]

    section("Example 4: Chaining transforms with aggregations")

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

    section("Example 5: literal() helper")

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

    section("Example 6: with_multi_if() n-way conditional column")

    obj = await create_object_from_value({"score": [95, 72, 45, 88, 60]})
    graded = obj.with_multi_if(
        [("score >= 90", "'A'"), ("score >= 80", "'B'"), ("score >= 60", "'C'")],
        default="'F'",
        alias="grade",
    )
    result = await graded.data()
    print(f"score: {result['score']}")  # → [95, 72, 45, 88, 60]
    print(f"grade: {result['grade']}")  # → ['A', 'C', 'F', 'B', 'C']

    tiers = await (await graded.group_by("grade").count()).data()
    counts = dict(sorted(zip(tiers["grade"], tiers["_count"], strict=True)))
    print(f"counts: {counts}")  # → {'A': 1, 'B': 1, 'C': 2, 'F': 1}

    # Both halves are SQL: boolean operators compose in conditions, and a
    # result may be a column or an expression, not just a constant.
    orders = await create_object_from_value(
        {
            "price": [100.0, 200.0, 50.0, 80.0],
            "status": ["void", "ok", "ok", "ok"],
            "region": ["EU", "US", "EU", "APAC"],
            "is_member": [0, 1, 0, 1],
        }
    )
    priced = orders.with_multi_if(
        [
            ("status = 'void' OR price = 0", "0.0"),
            ("is_member = 1 AND region IN ('EU', 'US')", "price * 0.8"),
            ("NOT (region = 'APAC') AND price >= 100", "price * 0.95"),
        ],
        default="price",
        alias="final_price",
        type="Float64",
    )
    print(f"final_price: {(await priced.data())['final_price']}")  # → [0.0, 160.0, 50.0, 80.0]


async def amain():
    async with data_context():
        await example()


if __name__ == "__main__":
    asyncio.run(amain())
