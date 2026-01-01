"""
Basic operators example for aaiclick.

This example demonstrates how to use create_object_from_value with automatic schema
inference and basic arithmetic operators on Objects.
"""

import asyncio
import aaiclick


async def main():
    # Client connects automatically using environment variables
    # (or defaults: localhost:8123)

    try:
        # Example 1: Create objects from scalar values
        print("Example 1: Creating objects from scalar values")
        print("-" * 50)

        obj_scalar_int = await aaiclick.create_object_from_value("scalar_int", 42)
        print(f"Created from int: {obj_scalar_int}")
        print(f"Value: {await obj_scalar_int.data()}\n")

        obj_scalar_float = await aaiclick.create_object_from_value("scalar_float", 3.14)
        print(f"Created from float: {obj_scalar_float}")
        print(f"Value: {await obj_scalar_float.data()}\n")

        obj_scalar_str = await aaiclick.create_object_from_value("scalar_str", "Hello, ClickHouse!")
        print(f"Created from string: {obj_scalar_str}")
        print(f"Value: {await obj_scalar_str.data()}")

        # Example 2: Create objects from lists (numpy dtype inference)
        print("\n" + "=" * 50)
        print("Example 2: Creating objects from lists (numpy infers dtype)")
        print("-" * 50)

        obj_list_int = await aaiclick.create_object_from_value("list_int", [1, 2, 3, 4, 5])
        print(f"Created from int list: {obj_list_int}")
        print(f"Values: {await obj_list_int.data()}\n")

        obj_list_float = await aaiclick.create_object_from_value("list_float", [1.5, 2.5, 3.5, 4.5])
        print(f"Created from float list: {obj_list_float}")
        print(f"Values: {await obj_list_float.data()}\n")

        obj_list_str = await aaiclick.create_object_from_value("list_str", ["apple", "banana", "cherry"])
        print(f"Created from string list: {obj_list_str}")
        print(f"Values: {await obj_list_str.data()}")

        # Example 3: Create objects from dictionaries
        print("\n" + "=" * 50)
        print("Example 3: Creating objects from dictionaries")
        print("-" * 50)

        obj_dict = await aaiclick.create_object_from_value(
            "user", {"id": 1, "name": "Alice", "age": 30, "score": 95.5}
        )
        print(f"Created from dict: {obj_dict}")
        print(f"Values: {await obj_dict.data()}\n")

        obj_dict2 = await aaiclick.create_object_from_value(
            "product", {"product_id": 100, "product_name": "Widget", "price": 29.99, "in_stock": True}
        )
        print(f"Created from dict: {obj_dict2}")
        print(f"Values: {await obj_dict2.data()}")

        # Example 4: Arithmetic operations
        print("\n" + "=" * 50)
        print("Example 4: Arithmetic operators")
        print("-" * 50)

        # Create two numeric objects for operations
        obj_a = await aaiclick.create_object_from_value("a", [10.0, 20.0, 30.0])
        obj_b = await aaiclick.create_object_from_value("b", [5.5, 10.3, 15.1])

        print(f"Created {obj_a}")
        print(f"Values in a: {await obj_a.data()}\n")

        print(f"Created {obj_b}")
        print(f"Values in b: {await obj_b.data()}")

        # Addition
        print("\n" + "-" * 50)
        print("Addition: a + b")
        obj_sum = await (obj_a + obj_b)
        print(f"Created sum object: {obj_sum}")
        print(f"Values: {await obj_sum.data()}")

        # Subtraction
        print("\n" + "-" * 50)
        print("Subtraction: a - b")
        obj_diff = await (obj_a - obj_b)
        print(f"Created difference object: {obj_diff}")
        print(f"Values: {await obj_diff.data()}")

        # Example 5: Table name generation
        print("\n" + "=" * 50)
        print("Example 5: Automatic table name generation")
        print("-" * 50)

        obj_x = aaiclick.Object("data")
        obj_y = aaiclick.Object("data")
        print(f"Two objects with same name get unique table names:")
        print(f"  {obj_x.name} -> {obj_x.table}")
        print(f"  {obj_y.name} -> {obj_y.table}")

        # Cleanup
        print("\n" + "=" * 50)
        print("Cleanup: Deleting all created tables")
        print("-" * 50)

        objects_to_delete = [
            obj_scalar_int,
            obj_scalar_float,
            obj_scalar_str,
            obj_list_int,
            obj_list_float,
            obj_list_str,
            obj_dict,
            obj_dict2,
            obj_a,
            obj_b,
            obj_sum,
            obj_diff,
        ]

        for obj in objects_to_delete:
            await obj.delete_table()
            print(f"Deleted table {obj.table}")

    except Exception as e:
        print(f"\nError: {e}")
        raise


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Basic Operators Example")
    print("=" * 50)
    print("\nNote: This example requires a running ClickHouse server")
    print("      on localhost:8123\n")
    asyncio.run(main())
