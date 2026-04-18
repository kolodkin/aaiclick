"""
Join example for aaiclick.

Demonstrates joining two dict Objects on one or more key columns:
- Inner join on a shared key (USING form)
- Left join with nullable promotion on the right side
- left_on/right_on form when key names differ
- Collision suffixes for non-key columns with the same name
- Cross join (Cartesian product)
"""

import asyncio

from aaiclick import ORIENT_RECORDS, create_object_from_value
from aaiclick.data.data_context import data_context


async def example():
    # Example 1: Inner join on a shared key
    print("Example 1: Inner join on 'id'")
    print("-" * 50)

    users = await create_object_from_value(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
        }
    )
    orders = await create_object_from_value(
        {
            "id": [1, 1, 4],
            "total": [9.5, 14.0, 2.0],
        }
    )

    joined = await users.join(orders, on="id")
    rows = sorted(await joined.data(orient=ORIENT_RECORDS), key=lambda r: r["total"])
    print(f"Rows: {rows}")  # → [{'id': 1, 'name': 'Alice', 'total': 9.5}, {'id': 1, 'name': 'Alice', 'total': 14.0}]

    # Example 2: Left join keeps unmatched left rows; right columns are NULL
    print("\n" + "=" * 50)
    print("Example 2: Left join (unmatched rows → NULL on right)")
    print("-" * 50)

    joined = await users.join(orders, on="id", how="left")
    rows = sorted(await joined.data(orient=ORIENT_RECORDS), key=lambda r: r["id"])
    print(
        f"Rows: {rows}"
    )  # → [{'id': 1, 'name': 'Alice', 'total': 9.5}, {'id': 1, 'name': 'Alice', 'total': 14.0}, {'id': 2, 'name': 'Bob', 'total': None}, {'id': 3, 'name': 'Carol', 'total': None}]
    print(f"'total' nullable? {joined.schema.columns['total'].nullable}")  # → True

    # Example 3: left_on / right_on when key names differ
    print("\n" + "=" * 50)
    print("Example 3: left_on='id' / right_on='user_id'")
    print("-" * 50)

    orders2 = await create_object_from_value(
        {
            "user_id": [1, 1, 2],
            "total": [9.5, 14.0, 3.0],
        }
    )
    joined = await users.join(orders2, left_on="id", right_on="user_id")
    rows = sorted(await joined.data(orient=ORIENT_RECORDS), key=lambda r: r["total"])
    print(
        f"Rows: {rows}"
    )  # → [{'id': 2, 'user_id': 2, 'name': 'Bob', 'total': 3.0}, {'id': 1, 'user_id': 1, 'name': 'Alice', 'total': 9.5}, {'id': 1, 'user_id': 1, 'name': 'Alice', 'total': 14.0}]

    # Example 4: suffixes on non-key collision — True uses default ("_l", "_r")
    print("\n" + "=" * 50)
    print("Example 4: suffixes=True on non-key collision")
    print("-" * 50)

    a = await create_object_from_value({"id": [1, 2], "score": [10, 20]})
    b = await create_object_from_value({"id": [1, 2], "score": [99, 88]})
    merged = await a.join(b, on="id", suffixes=True)
    rows = sorted(await merged.data(orient=ORIENT_RECORDS), key=lambda r: r["id"])
    print(f"Rows: {rows}")  # → [{'id': 1, 'score_l': 10, 'score_r': 99}, {'id': 2, 'score_l': 20, 'score_r': 88}]

    # Example 5: cross join
    print("\n" + "=" * 50)
    print("Example 5: Cross join (every color × every size)")
    print("-" * 50)

    colors = await create_object_from_value({"c": ["red", "blue"]})
    sizes = await create_object_from_value({"s": ["S", "M"]})
    skus = await colors.join(sizes, how="cross")
    rows = sorted((r["c"], r["s"]) for r in await skus.data(orient=ORIENT_RECORDS))
    print(f"Pairs: {rows}")  # → [('blue', 'M'), ('blue', 'S'), ('red', 'M'), ('red', 'S')]


async def amain():
    async with data_context():
        await example()


if __name__ == "__main__":
    asyncio.run(amain())
