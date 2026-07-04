"""
Nested arrays example for aaiclick.

Demonstrates creating Objects from dicts containing nested list-of-dicts,
which are stored as parallel Array columns with dot-star column notation.

Data is stored flat in ClickHouse using dot-star columns, but ``.data()``
automatically reconstructs the nested dict structure.
"""

import asyncio

from aaiclick import ORIENT_DICT, ORIENT_RECORDS, create_object_from_value
from aaiclick.data.data_context import data_context
from aaiclick.example_runner import section


async def example():
    """Run all nested array examples."""
    # ── Single Record with Nested Arrays ─────────────────────────
    section("PART 1: Single record with nested arrays")

    obj = await create_object_from_value(
        {
            "a": 2,
            "b": [{"c": [1, 2, 3], "d": 5}, {"c": [4, 5, 6], "d": 10}],
        }
    )

    print("\nInput: {a: 2, b: [{c: [1,2,3], d: 5}, {c: [4,5,6], d: 10}]}")
    print(f"\nSchema columns: {list(obj.schema.columns.keys())}")
    for name, col in obj.schema.columns.items():
        print(f"  {name}: {col.ch_type()}")

    data = await obj.data()
    print("\nReconstructed output:")
    for key, val in data.items():
        print(f"  {key}: {val}")

    # ── Multiple Records ─────────────────────────────────────────
    section("PART 2: Multiple records with nested arrays")

    obj = await create_object_from_value(
        [
            {"a": 2, "b": [{"c": [1, 2, 3], "d": 5}, {"c": [4, 5, 6], "d": 10}]},
            {"a": 3, "b": [{"c": [7, 8, 9], "d": 15}]},
        ]
    )

    data = await obj.data(orient=ORIENT_RECORDS)
    print(f"\nORIENT_RECORDS ({len(data)} rows):")
    for i, row in enumerate(data):
        print(f"  [{i}] {row}")

    data = await obj.data(orient=ORIENT_DICT)
    print("\nORIENT_DICT:")
    for key, val in data.items():
        print(f"  {key}: {val}")

    # ── Scalar-Only Sub-Fields ───────────────────────────────────
    section("PART 3: Nested objects with scalar-only sub-fields")

    obj = await create_object_from_value(
        {
            "name": "test",
            "items": [{"x": 1, "y": 2}, {"x": 3, "y": 4}],
        }
    )

    print("\nInput: {name: 'test', items: [{x: 1, y: 2}, {x: 3, y: 4}]}")
    for name, col in obj.schema.columns.items():
        print(f"  {name}: {col.ch_type()}")

    data = await obj.data()
    print(f"\nResult: {data}")

    # ── Array Sub-Fields (Array of Arrays) ───────────────────────
    section("PART 4: Nested objects with array sub-fields → Array(Array(T))")

    obj = await create_object_from_value(
        {
            "id": 1,
            "groups": [
                {"tags": ["a", "b"], "score": 10},
                {"tags": ["c"], "score": 20},
            ],
        }
    )

    print("\nInput: {id: 1, groups: [{tags: ['a','b'], score: 10}, {tags: ['c'], score: 20}]}")
    for name, col in obj.schema.columns.items():
        print(f"  {name}: {col.ch_type()}")

    data = await obj.data()
    print(f"\nResult: {data}")

    # ── Deep Nesting (Two Levels) ────────────────────────────────
    section("PART 5: Deep nesting (two levels of list-of-dicts)")

    obj = await create_object_from_value(
        {
            "root": 1,
            "level1": [
                {"name": "first", "level2": [{"val": 10}, {"val": 20}]},
                {"name": "second", "level2": [{"val": 30}]},
            ],
        }
    )

    print("\nSchema columns:")
    for name, col in obj.schema.columns.items():
        print(f"  {name}: {col.ch_type()}")

    data = await obj.data()
    print("\nResult:")
    for key, val in data.items():
        print(f"  {key}: {val}")


async def amain():
    """Main entry point that creates data_context() and calls example."""
    async with data_context():
        await example()


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Nested Arrays Example")
    print("=" * 50)
    asyncio.run(amain())
