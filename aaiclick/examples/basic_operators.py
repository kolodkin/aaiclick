"""
Basic operators example for aaiclick.

This example demonstrates how to use the Object class with basic arithmetic operators
to perform computations in ClickHouse using the global client manager.
"""

import asyncio
import aaiclick
from aaiclick import Object


async def main():
    # Connect to ClickHouse using global client manager
    await aaiclick.connect(host="localhost", port=8123)

    try:
        # Example 1: Create objects from scratch
        print("Example 1: Creating objects with values")
        print("-" * 50)

        # Create first object with a table containing values
        obj_a = await Object.create("a", "value Float64")
        client = aaiclick.get_client()
        await client.command(f"INSERT INTO {obj_a.table} VALUES (10.5), (20.3), (30.1)")
        print(f"Created {obj_a}")
        print(f"Values in {obj_a.name}:")
        result = await client.query(f"SELECT * FROM {obj_a.table}")
        print(result.result_rows)

        # Create second object
        obj_b = await Object.create("b", "value Float64")
        await client.command(f"INSERT INTO {obj_b.table} VALUES (5.5), (10.3), (15.1)")
        print(f"\nCreated {obj_b}")
        print(f"Values in {obj_b.name}:")
        result = await client.query(f"SELECT * FROM {obj_b.table}")
        print(result.result_rows)

        # Example 2: Addition operator
        print("\n" + "=" * 50)
        print("Example 2: Addition operator (a + b)")
        print("-" * 50)

        obj_c = await (obj_a + obj_b)
        print(f"Created result object: {obj_c}")
        print(f"Values in {obj_c.name}:")
        result = await client.query(f"SELECT * FROM {obj_c.table}")
        print(result.result_rows)

        # Example 3: Subtraction operator
        print("\n" + "=" * 50)
        print("Example 3: Subtraction operator (a - b)")
        print("-" * 50)

        obj_d = await (obj_a - obj_b)
        print(f"Created result object: {obj_d}")
        print(f"Values in {obj_d.name}:")
        result = await client.query(f"SELECT * FROM {obj_d.table}")
        print(result.result_rows)

        # Example 4: Create object from existing object
        print("\n" + "=" * 50)
        print("Example 4: Create object from existing object")
        print("-" * 50)

        obj_f = await Object.create_from_value("f", obj_a)
        print(f"Created {obj_f} from {obj_a.name}")
        print(f"Values in {obj_f.name}:")
        result = await client.query(f"SELECT * FROM {obj_f.table}")
        print(result.result_rows)

        # Example 5: Table name generation
        print("\n" + "=" * 50)
        print("Example 5: Table name generation")
        print("-" * 50)

        obj_g = Object("custom")
        obj_h = Object("custom")
        print(f"Two objects with same name have different tables:")
        print(f"  {obj_g.name} -> {obj_g.table}")
        print(f"  {obj_h.name} -> {obj_h.table}")

        # Example 6: Custom table name
        print("\n" + "=" * 50)
        print("Example 6: Custom table name")
        print("-" * 50)

        obj_i = Object("myobject", table="my_custom_table")
        print(f"Object with custom table: {obj_i}")
        print(f"  Name: {obj_i.name}")
        print(f"  Table: {obj_i.table}")

        # Example 7: Delete objects from database
        print("\n" + "=" * 50)
        print("Example 7: Deleting objects")
        print("-" * 50)

        for obj in [obj_a, obj_b, obj_c, obj_d, obj_f]:
            await obj.delete_db()
            print(f"Deleted table {obj.table}")

    finally:
        await aaiclick.close()
        print("\nConnection closed.")


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Basic Operators Example")
    print("=" * 50)
    print("\nNote: This example requires a running ClickHouse server")
    print("      on localhost:8123\n")
    asyncio.run(main())
