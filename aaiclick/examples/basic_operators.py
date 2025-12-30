"""
Basic operators example for aaiclick.

This example demonstrates how to use the Object class with basic arithmetic operators
to perform computations in ClickHouse.
"""

import asyncio
from clickhouse_connect import get_async_client
from aaiclick import Object


async def main():
    # Connect to ClickHouse
    client = await get_async_client(host="localhost", port=8123)

    try:
        # Example 1: Create objects from scratch
        print("Example 1: Creating objects with values")
        print("-" * 50)

        # Create first object with a table containing values
        obj_a = await Object.create(client, "a", "value Float64")
        await client.command(f"INSERT INTO {obj_a.table} VALUES (10.5), (20.3), (30.1)")
        print(f"Created {obj_a}")
        print(f"Values in {obj_a.name}:")
        result = await client.query(f"SELECT * FROM {obj_a.table}")
        print(result.result_rows)

        # Create second object
        obj_b = await Object.create(client, "b", "value Float64")
        await client.command(f"INSERT INTO {obj_b.table} VALUES (5.5), (10.3), (15.1)")
        print(f"\nCreated {obj_b}")
        print(f"Values in {obj_b.name}:")
        result = await client.query(f"SELECT * FROM {obj_b.table}")
        print(result.result_rows)

        # Example 2: Addition operator
        print("\n" + "=" * 50)
        print("Example 2: Addition operator (a + b)")
        print("-" * 50)

        obj_c = obj_a + obj_b
        print(f"Created result object: {obj_c}")
        print(f"Operation stored: addition of {obj_a.name} and {obj_b.name}")

        # Example 3: Subtraction operator
        print("\n" + "=" * 50)
        print("Example 3: Subtraction operator (a - b)")
        print("-" * 50)

        obj_d = obj_a - obj_b
        print(f"Created result object: {obj_d}")
        print(f"Operation stored: subtraction of {obj_b.name} from {obj_a.name}")

        # Example 4: Chaining operations
        print("\n" + "=" * 50)
        print("Example 4: Chaining operations ((a + b) - a)")
        print("-" * 50)

        obj_e = (obj_a + obj_b) - obj_a
        print(f"Created result object: {obj_e}")
        print(f"Result table: {obj_e.table}")

        # Example 5: Create object from existing object
        print("\n" + "=" * 50)
        print("Example 5: Create object from existing object")
        print("-" * 50)

        obj_f = await Object.create_from_value(client, "f", obj_a)
        print(f"Created {obj_f} from {obj_a.name}")
        print(f"Values in {obj_f.name}:")
        result = await client.query(f"SELECT * FROM {obj_f.table}")
        print(result.result_rows)

        # Example 6: Table name generation
        print("\n" + "=" * 50)
        print("Example 6: Table name generation")
        print("-" * 50)

        obj_g = Object("custom")
        obj_h = Object("custom")
        print(f"Two objects with same name have different tables:")
        print(f"  {obj_g.name} -> {obj_g.table}")
        print(f"  {obj_h.name} -> {obj_h.table}")

        # Example 7: Custom table name
        print("\n" + "=" * 50)
        print("Example 7: Custom table name")
        print("-" * 50)

        obj_i = Object("myobject", table="my_custom_table")
        print(f"Object with custom table: {obj_i}")
        print(f"  Name: {obj_i.name}")
        print(f"  Table: {obj_i.table}")

        # Cleanup: Drop created tables
        print("\n" + "=" * 50)
        print("Cleaning up...")
        print("-" * 50)
        for obj in [obj_a, obj_b, obj_f]:
            await client.command(f"DROP TABLE IF EXISTS {obj.table}")
            print(f"Dropped table {obj.table}")

    finally:
        await client.close()
        print("\nConnection closed.")


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Basic Operators Example")
    print("=" * 50)
    asyncio.run(main())
