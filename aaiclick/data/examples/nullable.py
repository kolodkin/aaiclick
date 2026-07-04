"""
Nullable columns example for aaiclick.

This example demonstrates how nullable columns work:
- Creating objects with nullable schemas
- Using FieldSpec for nullable/low_cardinality columns
- Nullable promotion during concat
- Arithmetic with nullable values
- Coalesce to replace NULLs with defaults
"""

import asyncio

from aaiclick import (
    FIELDTYPE_ARRAY,
    ColumnInfo,
    FieldSpec,
    Schema,
    create_object,
    create_object_from_value,
)
from aaiclick.data.data_context import data_context, get_ch_client
from aaiclick.example_runner import section


async def example():
    """Run all nullable examples."""
    ch = get_ch_client()

    section("Example 1: Creating objects with nullable columns (Schema)")

    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "value": ColumnInfo("Int64", nullable=True),
        },
    )
    obj = await create_object(schema)
    await ch.command(f"INSERT INTO {obj.table} (value) VALUES (1), (NULL), (3)")

    data = await obj.data()
    print(f"Nullable array: {data}")  # → [1, None, 3]

    schema = obj.schema
    print(f"Column nullable: {schema.columns['value'].nullable}")  # → True

    section("Example 1b: Using FieldSpec with create_object_from_value")

    obj = await create_object_from_value(
        {"category": ["a", "b", "a"], "score": [95.5, 88.0, 72.0]},
        fields={
            "category": FieldSpec(low_cardinality=True),
            "score": FieldSpec(nullable=True),
        },
    )
    print(f"category type: {obj.schema.columns['category'].ch_type()}")  # → LowCardinality(String)
    print(f"score type:    {obj.schema.columns['score'].ch_type()}")  # → Nullable(Float64)
    print(f"data: {await obj.data()}")  # → {'category': ['a', 'b', 'a'], 'score': [95.5, 88.0, 72.0]}

    # FieldSpec also works with lists (column name is 'value')
    obj = await create_object_from_value(
        [10, 20, 30],
        fields={"value": FieldSpec(nullable=True)},
    )
    print(f"value nullable: {obj.schema.columns['value'].nullable}")  # → True

    section("Example 2: Nullable promotion during concat")

    obj_nullable = await create_object(schema)
    await ch.command(f"INSERT INTO {obj_nullable.table} (value) VALUES (10), (NULL)")

    obj_regular = await create_object_from_value([20, 30])

    schema_a = obj_nullable.schema
    schema_b = obj_regular.schema
    print(f"Source A nullable: {schema_a.columns['value'].nullable}")  # → True
    print(f"Source B nullable: {schema_b.columns['value'].nullable}")  # → False

    result = await obj_nullable.concat(obj_regular)
    schema_result = result.schema
    print(f"Result nullable:  {schema_result.columns['value'].nullable}")  # → True
    print(f"Result data: {await result.data()}")  # → [10, None, 20, 30]

    section("Example 3: Arithmetic with nullable values")

    obj_with_nulls = await create_object(schema)
    await ch.command(f"INSERT INTO {obj_with_nulls.table} (value) VALUES (5), (NULL), (15)")

    added = obj_with_nulls + 10
    print(f"Original:    {await obj_with_nulls.data()}")  # → [5, None, 15]
    print(f"Added + 10:  {await added.data()}")  # → [15, None, 25]
    print("Note: NULL + 10 = NULL (NULL propagates through arithmetic)")

    section("Example 4: Coalesce to replace NULLs")

    obj_nulls = await create_object(schema)
    await ch.command(f"INSERT INTO {obj_nulls.table} (value) VALUES (1), (NULL), (3), (NULL), (5)")
    print(f"Before coalesce: {await obj_nulls.data()}")  # → [1, None, 3, None, 5]

    filled = await obj_nulls.coalesce(0)
    print(f"After coalesce(0): {await filled.data()}")  # → [1, 0, 3, 0, 5]

    schema_filled = filled.schema
    print(f"Result nullable: {schema_filled.columns['value'].nullable}")  # → False
    print("Note: coalesce with non-nullable fallback produces non-nullable result")

    # Note: All objects created via context are automatically cleaned up when context exits
    section("Cleanup: All context-created objects will be cleaned up automatically")


async def amain():
    """Main entry point that creates data_context() and calls example."""
    async with data_context():
        await example()


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Nullable Columns Example")
    print("=" * 50)
    asyncio.run(amain())
