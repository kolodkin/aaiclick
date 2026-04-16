"""
Membership testing (isin) example for aaiclick.

This example demonstrates the isin() operator and with_isin() computed column:
- Filtering values by membership in another Object
- Using isin() with Python lists
- Chaining isin() with aggregation
- Adding computed membership columns with with_isin()
- Filtering and grouping by membership
"""

import asyncio

from aaiclick import ORIENT_RECORDS, create_object_from_value
from aaiclick.data.data_context import data_context


async def example():
    """Run all isin examples."""
    # Example 1: Basic isin() with strings
    print("Example 1: Basic isin() with strings")
    print("-" * 50)

    fruits = await create_object_from_value(["apple", "banana", "cherry", "date", "elderberry"])
    tropical = await create_object_from_value(["banana", "date", "mango"])

    mask = await fruits.isin(tropical)
    print(f"Fruits:   {await fruits.data()}")
    print(f"Tropical: {await tropical.data()}")
    print(f"isin():   {await mask.data()}")  # → [0, 1, 0, 1, 0]

    # Example 2: isin() with a Python list
    print("\n" + "=" * 50)
    print("Example 2: isin() with a Python list")
    print("-" * 50)

    scores = await create_object_from_value([85, 90, 75, 95, 60])
    mask = await scores.isin([90, 95, 100])
    print(f"Scores:    {await scores.data()}")
    print(f"Top marks: {await mask.data()}")  # → [0, 1, 0, 1, 0]

    # Example 3: Chaining isin() with sum() to count matches
    print("\n" + "=" * 50)
    print("Example 3: Count matches with isin() + sum()")
    print("-" * 50)

    tags = await create_object_from_value(["python", "java", "python", "rust", "java", "go"])
    popular = await create_object_from_value(["python", "java"])
    mask = await tags.isin(popular)
    count = await mask.sum()
    print(f"Tags:    {await tags.data()}")
    print(f"Popular: {await popular.data()}")
    print(f"Matches: {await count.data()}")  # → 4

    # Example 4: isin() on a dict column
    print("\n" + "=" * 50)
    print("Example 4: isin() on a dict column")
    print("-" * 50)

    employees = await create_object_from_value({
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "department": ["engineering", "sales", "engineering", "marketing"],
        "salary": [120000, 80000, 110000, 90000],
    })
    tech_depts = await create_object_from_value(["engineering", "data science"])
    mask = await employees["department"].isin(tech_depts)
    print(f"Departments: {(await employees.data())['department']}")
    print(f"Is tech:     {await mask.data()}")  # → [1, 0, 1, 0]

    # Example 5: with_isin() computed column
    print("\n" + "=" * 50)
    print("Example 5: with_isin() computed column")
    print("-" * 50)

    allowed_depts = await create_object_from_value(["engineering", "marketing"])
    view = employees.with_isin("department", allowed_depts, alias="in_focus")
    data = await view.data()
    print(f"Names:    {data['name']}")
    print(f"Depts:    {data['department']}")
    print(f"In focus: {data['in_focus']}")  # → [1, 0, 1, 1]

    # Example 6: with_isin() + where() to filter
    print("\n" + "=" * 50)
    print("Example 6: Filter with with_isin() + where()")
    print("-" * 50)

    filtered = view.where("in_focus = 1")
    data = await filtered.data()
    print("Focus departments only:")
    print(f"  Names:    {data['name']}")  # → ['Alice', 'Charlie', 'Diana']
    print(f"  Salaries: {data['salary']}")  # → [120000, 110000, 90000]

    # Example 7: with_isin() + group_by() for segmented analysis
    print("\n" + "=" * 50)
    print("Example 7: Group by membership with with_isin()")
    print("-" * 50)

    result = await view.group_by("in_focus").sum("salary")
    rows = await result.data(orient=ORIENT_RECORDS)
    for row in rows:
        label = "Focus" if row["in_focus"] == 1 else "Other"
        print(f"  {label}: total salary = {row['salary']}")


async def amain():
    async with data_context():
        await example()


if __name__ == "__main__":
    asyncio.run(amain())
