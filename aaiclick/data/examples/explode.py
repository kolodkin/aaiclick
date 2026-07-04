"""
Explode example for aaiclick.

Demonstrates Object.explode() which flattens Array columns into individual
rows using ClickHouse ARRAY JOIN. Returns a lazy View — no materialization
until copy() is called.
"""

import asyncio

from aaiclick import ORIENT_RECORDS, create_object_from_value
from aaiclick.data.data_context import data_context
from aaiclick.example_runner import section


async def example():
    """Run all explode examples."""
    # ── Basic Explode ────────────────────────────────────────
    section("PART 1: Basic explode — Array(String) → one row per tag")

    obj = await create_object_from_value(
        [
            {"user": "alice", "tags": ["python", "data"]},
            {"user": "bob", "tags": ["ml", "python", "nlp"]},
            {"user": "carol", "tags": ["data"]},
        ]
    )

    print("\nBefore explode:")
    for row in await obj.data(orient=ORIENT_RECORDS):
        print(f"  {row}")

    exploded = obj.explode("tags")
    print("\nAfter explode('tags'):")
    for row in await exploded.data(orient=ORIENT_RECORDS):
        print(f"  {row}")

    # ── Schema Change ────────────────────────────────────────
    section("PART 2: Schema — Array(String) becomes String after explode")

    obj = await create_object_from_value(
        [
            {"item": "A", "scores": [10, 20, 30]},
            {"item": "B", "scores": [5, 15]},
        ]
    )

    print(
        f"\nBefore explode — scores type: {obj.schema.columns['scores'].ch_type()}"
    )  # → Array(Int64)  # → Array(Int64)

    exploded = obj.explode("scores")
    print(f"After  explode — scores type: {exploded._effective_columns['scores'].ch_type()}")  # → Int64  # → Int64

    print("\nExploded rows:")
    for row in await exploded.data(orient=ORIENT_RECORDS):
        print(f"  {row}")

    # ── Chaining with where() ────────────────────────────────
    section("PART 3: Chain explode with where() filter")

    obj = await create_object_from_value(
        [
            {"user": "alice", "tags": ["python", "data", "sql"]},
            {"user": "bob", "tags": ["ml", "python"]},
            {"user": "carol", "tags": ["java", "go"]},
        ]
    )

    python_rows = obj.explode("tags").where("tags = 'python'")
    print("\nRows where tags == 'python':")
    for row in await python_rows.data(orient=ORIENT_RECORDS):
        print(f"  {row}")

    # ── Multi-Column Zip Explode ─────────────────────────────
    section("PART 4: Multi-column zip explode (NOT Cartesian)")

    obj = await create_object_from_value(
        [
            {"product": "widget", "months": ["Jan", "Feb", "Mar"], "sales": [100, 150, 120]},
            {"product": "gadget", "months": ["Jan", "Feb"], "sales": [200, 180]},
        ]
    )

    print("\nBefore explode:")
    for row in await obj.data(orient=ORIENT_RECORDS):
        print(f"  {row}")

    exploded = obj.explode("months", "sales")
    print("\nAfter explode('months', 'sales') — columns zipped, not Cartesian:")
    for row in await exploded.data(orient=ORIENT_RECORDS):
        print(f"  {row}")

    # ── LEFT Explode ─────────────────────────────────────────
    section("PART 5: LEFT explode — preserve rows with empty arrays")

    obj = await create_object_from_value(
        [
            {"user": "alice", "tags": ["python", "data"]},
            {"user": "bob", "tags": []},  # empty — dropped by default
            {"user": "carol", "tags": ["sql"]},
        ]
    )

    print("\nDefault explode — drops empty array row:")
    for row in await obj.explode("tags").data(orient=ORIENT_RECORDS):
        print(f"  {row}")

    print("\nLEFT explode — keeps empty array row (tags becomes ''):")
    for row in await obj.explode("tags", left=True).data(orient=ORIENT_RECORDS):
        print(f"  {row}")

    # ── Materialized copy() ──────────────────────────────────
    section("PART 6: Materialize exploded view with copy()")

    obj = await create_object_from_value(
        [
            {"category": "fruit", "items": ["apple", "banana", "cherry"]},
            {"category": "veggie", "items": ["carrot", "pea"]},
        ]
    )

    materialized = await obj.explode("items").copy()
    print(f"\nMaterialized type:  {type(materialized).__name__}")
    print("Materialized rows:")
    for row in await materialized.data(orient=ORIENT_RECORDS):
        print(f"  {row}")


async def amain():
    """Main entry point that creates data_context() and calls example."""
    async with data_context():
        await example()


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Explode Example")
    print("=" * 50)
    asyncio.run(amain())
