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

        obj_scalar_int = await aaiclick.create_object_from_value(42)
        print(f"Created from int: {obj_scalar_int}")
        print(f"Value: {await obj_scalar_int.data()}\n")

        obj_scalar_float = await aaiclick.create_object_from_value(3.14)
        print(f"Created from float: {obj_scalar_float}")
        print(f"Value: {await obj_scalar_float.data()}\n")

        obj_scalar_str = await aaiclick.create_object_from_value("Hello, ClickHouse!")
        print(f"Created from string: {obj_scalar_str}")
        print(f"Value: {await obj_scalar_str.data()}")

        # Example 2: Create objects from lists (numpy dtype inference)
        print("\n" + "=" * 50)
        print("Example 2: Creating objects from lists (numpy infers dtype)")
        print("-" * 50)

        obj_list_int = await aaiclick.create_object_from_value([1, 2, 3, 4, 5])
        print(f"Created from int list: {obj_list_int}")
        print(f"Values: {await obj_list_int.data()}\n")

        obj_list_float = await aaiclick.create_object_from_value([1.5, 2.5, 3.5, 4.5])
        print(f"Created from float list: {obj_list_float}")
        print(f"Values: {await obj_list_float.data()}\n")

        obj_list_str = await aaiclick.create_object_from_value(["apple", "banana", "cherry"])
        print(f"Created from string list: {obj_list_str}")
        print(f"Values: {await obj_list_str.data()}")

        # Example 3: Create objects from dictionaries
        print("\n" + "=" * 50)
        print("Example 3: Creating objects from dictionaries")
        print("-" * 50)

        # Dict of scalars (single row)
        obj_dict = await aaiclick.create_object_from_value(
            {"id": 1, "name": "Alice", "age": 30, "score": 95.5}
        )
        print(f"Created from dict of scalars: {obj_dict}")
        print(f"Values: {await obj_dict.data()}\n")

        # Dict of arrays (multiple rows)
        obj_dict_arrays = await aaiclick.create_object_from_value(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [30, 25, 35]
            }
        )
        print(f"Created from dict of arrays: {obj_dict_arrays}")

        # Default: returns first row as dict
        first_row = await obj_dict_arrays.data()
        print(f"First row (default): {first_row}")

        # With orient='records': returns all rows as list of dicts
        all_rows = await obj_dict_arrays.data(orient=aaiclick.ORIENT_RECORDS)
        print(f"All rows (orient='records'): {all_rows}")

        # Example 4: Arithmetic operations
        print("\n" + "=" * 50)
        print("Example 4: Arithmetic operators")
        print("-" * 50)

        # Create two numeric objects for operations
        obj_a = await aaiclick.create_object_from_value([10.0, 20.0, 30.0])
        obj_b = await aaiclick.create_object_from_value([2.0, 4.0, 5.0])

        print(f"Created {obj_a}")
        print(f"Values in a: {await obj_a.data()}\n")

        print(f"Created {obj_b}")
        print(f"Values in b: {await obj_b.data()}")

        # All arithmetic operators
        print("\n" + "-" * 50)

        # Addition
        result_add = await (obj_a + obj_b)
        print(f"Addition (a + b): {await result_add.data()}")
        await result_add.delete_table()

        # Subtraction
        result_sub = await (obj_a - obj_b)
        print(f"Subtraction (a - b): {await result_sub.data()}")
        await result_sub.delete_table()

        # Multiplication
        result_mul = await (obj_a * obj_b)
        print(f"Multiplication (a * b): {await result_mul.data()}")
        await result_mul.delete_table()

        # Division
        result_div = await (obj_a / obj_b)
        print(f"Division (a / b): {await result_div.data()}")
        await result_div.delete_table()

        # Floor Division
        result_floordiv = await (obj_a // obj_b)
        print(f"Floor Division (a // b): {await result_floordiv.data()}")
        await result_floordiv.delete_table()

        # Modulo
        result_mod = await (obj_a % obj_b)
        print(f"Modulo (a % b): {await result_mod.data()}")
        await result_mod.delete_table()

        # Power
        result_pow = await (obj_a ** obj_b)
        print(f"Power (a ** b): {await result_pow.data()}")
        await result_pow.delete_table()

        # Example 5: Comparison operators
        print("\n" + "=" * 50)
        print("Example 5: Comparison operators")
        print("-" * 50)

        obj_x = await aaiclick.create_object_from_value([1, 5, 10, 15])
        obj_y = await aaiclick.create_object_from_value([5, 5, 8, 20])

        print(f"Values in x: {await obj_x.data()}")
        print(f"Values in y: {await obj_y.data()}\n")

        # Equal
        result_eq = await (obj_x == obj_y)
        print(f"Equal (x == y): {await result_eq.data()}")
        await result_eq.delete_table()

        # Not Equal
        result_ne = await (obj_x != obj_y)
        print(f"Not Equal (x != y): {await result_ne.data()}")
        await result_ne.delete_table()

        # Less Than
        result_lt = await (obj_x < obj_y)
        print(f"Less Than (x < y): {await result_lt.data()}")
        await result_lt.delete_table()

        # Less or Equal
        result_le = await (obj_x <= obj_y)
        print(f"Less or Equal (x <= y): {await result_le.data()}")
        await result_le.delete_table()

        # Greater Than
        result_gt = await (obj_x > obj_y)
        print(f"Greater Than (x > y): {await result_gt.data()}")
        await result_gt.delete_table()

        # Greater or Equal
        result_ge = await (obj_x >= obj_y)
        print(f"Greater or Equal (x >= y): {await result_ge.data()}")
        await result_ge.delete_table()

        # Example 6: Bitwise operators
        print("\n" + "=" * 50)
        print("Example 6: Bitwise operators")
        print("-" * 50)

        obj_m = await aaiclick.create_object_from_value([12, 10, 8])  # Binary: 1100, 1010, 1000
        obj_n = await aaiclick.create_object_from_value([10, 12, 4])  # Binary: 1010, 1100, 0100

        print(f"Values in m: {await obj_m.data()}")
        print(f"Values in n: {await obj_n.data()}\n")

        # Bitwise AND
        result_and = await (obj_m & obj_n)
        print(f"Bitwise AND (m & n): {await result_and.data()}")
        await result_and.delete_table()

        # Bitwise OR
        result_or = await (obj_m | obj_n)
        print(f"Bitwise OR (m | n): {await result_or.data()}")
        await result_or.delete_table()

        # Bitwise XOR
        result_xor = await (obj_m ^ obj_n)
        print(f"Bitwise XOR (m ^ n): {await result_xor.data()}")
        await result_xor.delete_table()

        # Example 7: Table name generation with Snowflake IDs
        print("\n" + "=" * 50)
        print("Example 7: Automatic table name generation with Snowflake IDs")
        print("-" * 50)

        obj_auto = aaiclick.Object()
        obj_auto2 = aaiclick.Object()
        print(f"Each object gets a unique Snowflake ID as table name (prefixed with 't'):")
        print(f"  Object 1 -> table: {obj_auto.table}")
        print(f"  Object 2 -> table: {obj_auto2.table}")

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
            obj_dict_arrays,
            obj_a,
            obj_b,
            obj_x,
            obj_y,
            obj_m,
            obj_n,
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
