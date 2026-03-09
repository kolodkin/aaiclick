"""
Nullable columns example for aaiclick.

This example demonstrates how nullable columns work:
- Creating objects with nullable schemas
- Nullable promotion during concat
- Arithmetic with nullable values
- Coalesce to replace NULLs with defaults
"""

import asyncio

from aaiclick import (
    ColumnDef,
    Schema,
    create_object,
    create_object_from_value,
    FIELDTYPE_ARRAY,
)
from aaiclick.data.data_context import data_context, get_ch_client


async def example():
    """Run all nullable examples."""
    ch = get_ch_client()

    # Example 1: Create an object with a nullable column
    print("Example 1: Creating objects with nullable columns")
    print("-" * 50)

    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnDef("UInt64"),
            "value": ColumnDef("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (1), (NULL), (3)")

    data = await obj.data()
    print(f"Nullable array: {data}")

    meta = await obj.metadata()
    print(f"Column nullable: {meta.columns['value'].nullable}")

    # Example 2: Nullable promotion in concat
    print("\n" + "=" * 50)
    print("Example 2: Nullable promotion during concat")
    print("-" * 50)

    obj_nullable = await create_object(schema)
    await ch.command(f"INSERT INTO {obj_nullable.table} (value) VALUES (10), (NULL)")

    obj_regular = await create_object_from_value([20, 30])

    meta_a = await obj_nullable.metadata()
    meta_b = await obj_regular.metadata()
    print(f"Source A nullable: {meta_a.columns['value'].nullable}")
    print(f"Source B nullable: {meta_b.columns['value'].nullable}")

    result = await obj_nullable.concat(obj_regular)
    meta_result = await result.metadata()
    print(f"Result nullable:  {meta_result.columns['value'].nullable}")
    print(f"Result data: {await result.data()}")

    # Example 3: Arithmetic with nullable values
    print("\n" + "=" * 50)
    print("Example 3: Arithmetic with nullable values")
    print("-" * 50)

    obj_with_nulls = await create_object(schema)
    await ch.command(
        f"INSERT INTO {obj_with_nulls.table} (value) VALUES (5), (NULL), (15)"
    )

    added = await (obj_with_nulls + 10)
    print(f"Original:    {await obj_with_nulls.data()}")
    print(f"Added + 10:  {await added.data()}")
    print("Note: NULL + 10 = NULL (NULL propagates through arithmetic)")

    # Example 4: Coalesce - replace NULLs with a default
    print("\n" + "=" * 50)
    print("Example 4: Coalesce to replace NULLs")
    print("-" * 50)

    obj_nulls = await create_object(schema)
    await ch.command(
        f"INSERT INTO {obj_nulls.table} (value) VALUES (1), (NULL), (3), (NULL), (5)"
    )
    print(f"Before coalesce: {await obj_nulls.data()}")

    filled = await obj_nulls.coalesce(0)
    print(f"After coalesce(0): {await filled.data()}")

    meta_filled = await filled.metadata()
    print(f"Result nullable: {meta_filled.columns['value'].nullable}")
    print("Note: coalesce with non-nullable fallback produces non-nullable result")

    # Note: All objects created via context are automatically cleaned up when context exits
    print("\n" + "=" * 50)
    print("Cleanup: All context-created objects will be cleaned up automatically")
    print("-" * 50)


async def amain():
    """Main entry point that creates data_context() and calls example."""
    async with data_context():
        await example()


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Nullable Columns Example")
    print("=" * 50)
    print("\nNote: This example requires a running ClickHouse server")
    print("      on localhost:8123\n")
    asyncio.run(amain())
