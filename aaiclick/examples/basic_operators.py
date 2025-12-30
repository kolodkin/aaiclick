"""
Basic operators example for aaiclick.

This example demonstrates how to use the Object class with basic arithmetic operators
to perform computations in ClickHouse using the global client manager.
"""

import asyncio
import aaiclick


async def main():
    # Connect to ClickHouse using global client manager
    await aaiclick.connect(host="localhost", port=8123)

    try:
        # Example 1: Create objects using factory functions
        print("Example 1: Creating objects with schema")
        print("-" * 50)

        # Create object with single column
        obj_a = await aaiclick.create_object("a", "value Float64")
        client = aaiclick.get_client()
        await client.command(f"INSERT INTO {obj_a.table} VALUES (10.5), (20.3), (30.1)")
        print(f"Created {obj_a}")
        print(f"Values in {obj_a.name}:")
        result = await client.query(f"SELECT * FROM {obj_a.table}")
        print(result.result_rows)

        # Create object with multiple columns
        obj_multi = await aaiclick.create_object("multi", ["id Int64", "value Float64", "label String"])
        await client.command(f"INSERT INTO {obj_multi.table} VALUES (1, 100.5, 'first'), (2, 200.3, 'second')")
        print(f"\nCreated {obj_multi} with multiple columns")
        print(f"Values in {obj_multi.name}:")
        result = await client.query(f"SELECT * FROM {obj_multi.table}")
        print(result.result_rows)

        # Example 2: Create objects from Python values
        print("\n" + "=" * 50)
        print("Example 2: Creating objects from Python values")
        print("-" * 50)

        # From scalar
        obj_scalar = await aaiclick.create_object_from_value("scalar", 42)
        print(f"Created from scalar: {obj_scalar}")
        result = await client.query(f"SELECT * FROM {obj_scalar.table}")
        print(f"Value: {result.result_rows}")

        # From list
        obj_list = await aaiclick.create_object_from_value("numbers", [1.5, 2.5, 3.5, 4.5])
        print(f"\nCreated from list: {obj_list}")
        result = await client.query(f"SELECT * FROM {obj_list.table}")
        print(f"Values: {result.result_rows}")

        # From dict
        obj_dict = await aaiclick.create_object_from_value(
            "user", {"id": 1, "name": "Alice", "age": 30, "score": 95.5}
        )
        print(f"\nCreated from dict: {obj_dict}")
        result = await client.query(f"SELECT * FROM {obj_dict.table}")
        print(f"Values: {result.result_rows}")

        # Example 3: Addition operator
        print("\n" + "=" * 50)
        print("Example 3: Addition operator")
        print("-" * 50)

        obj_b = await aaiclick.create_object_from_value("b", [5.5, 10.3, 15.1])
        print(f"Created {obj_b}")
        result = await client.query(f"SELECT * FROM {obj_b.table}")
        print(f"Values: {result.result_rows}")

        # Note: obj_a was created with manual inserts, obj_b from list
        # For operators to work, both tables need compatible schema
        obj_c = await aaiclick.create_object("c", "value Float64")
        await client.command(f"INSERT INTO {obj_c.table} VALUES (100.0), (200.0), (300.0)")

        obj_sum = await (obj_c + obj_b)
        print(f"\nCreated sum object: {obj_sum}")
        print(f"Values in {obj_sum.name}:")
        result = await client.query(f"SELECT * FROM {obj_sum.table}")
        print(result.result_rows)

        # Example 4: Subtraction operator
        print("\n" + "=" * 50)
        print("Example 4: Subtraction operator")
        print("-" * 50)

        obj_diff = await (obj_c - obj_b)
        print(f"Created difference object: {obj_diff}")
        print(f"Values in {obj_diff.name}:")
        result = await client.query(f"SELECT * FROM {obj_diff.table}")
        print(result.result_rows)

        # Example 5: Table name generation
        print("\n" + "=" * 50)
        print("Example 5: Table name generation")
        print("-" * 50)

        obj_g = aaiclick.Object("custom")
        obj_h = aaiclick.Object("custom")
        print(f"Two objects with same name have different tables:")
        print(f"  {obj_g.name} -> {obj_g.table}")
        print(f"  {obj_h.name} -> {obj_h.table}")

        # Example 6: Custom table name
        print("\n" + "=" * 50)
        print("Example 6: Custom table name")
        print("-" * 50)

        obj_i = aaiclick.Object("myobject", table="my_custom_table")
        print(f"Object with custom table: {obj_i}")
        print(f"  Name: {obj_i.name}")
        print(f"  Table: {obj_i.table}")

        # Example 7: Delete objects from database
        print("\n" + "=" * 50)
        print("Example 7: Deleting objects")
        print("-" * 50)

        for obj in [obj_a, obj_multi, obj_scalar, obj_list, obj_dict, obj_b, obj_c, obj_sum, obj_diff]:
            await obj.delete_table()
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
