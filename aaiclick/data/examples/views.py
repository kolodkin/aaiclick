"""
View examples for aaiclick.

This example demonstrates how to use Views with query constraints:
- WHERE clause filtering
- LIMIT and OFFSET pagination
- ORDER BY sorting
- Mixed combinations of constraints

Views are read-only and reference the underlying table data.
"""

import asyncio

from aaiclick import DataContext, ORIENT_RECORDS, create_object_from_value


async def example(context):
    """Run all view examples using the provided context."""
    # Example 1: WHERE clause with int scalar array
    print("Example 1: WHERE clause with int scalar array")
    print("-" * 50)

    obj_int = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    print(f"Original data: {await obj_int.data()}\n")

    # Filter values greater than 5
    view_where = obj_int.view(where="value > 5")
    print(f"WHERE value > 5: {await view_where.data()}")

    # Filter values between 3 and 7
    view_where_range = obj_int.view(where="value >= 3 AND value <= 7")
    print(f"WHERE value >= 3 AND value <= 7: {await view_where_range.data()}")

    # Filter even values
    view_where_even = obj_int.view(where="value % 2 = 0")
    print(f"WHERE value % 2 = 0 (even): {await view_where_even.data()}")

    # Example 2: LIMIT and OFFSET with int scalar array
    print("\n" + "=" * 50)
    print("Example 2: LIMIT and OFFSET with int scalar array")
    print("-" * 50)

    obj_nums = await create_object_from_value([10, 20, 30, 40, 50, 60, 70, 80])
    print(f"Original data: {await obj_nums.data()}\n")

    # Get first 3 elements
    view_limit = obj_nums.view(limit=3)
    print(f"LIMIT 3: {await view_limit.data()}")

    # Skip first 2, get next 3
    view_offset_limit = obj_nums.view(offset=2, limit=3)
    print(f"OFFSET 2 LIMIT 3: {await view_offset_limit.data()}")

    # Skip first 5
    view_offset = obj_nums.view(offset=5)
    print(f"OFFSET 5: {await view_offset.data()}")

    # Example 3: ORDER BY with int scalar array
    print("\n" + "=" * 50)
    print("Example 3: ORDER BY with int scalar array")
    print("-" * 50)

    obj_unsorted = await create_object_from_value([50, 20, 80, 10, 40, 60, 30, 70])
    print(f"Original data: {await obj_unsorted.data()}\n")

    # Sort ascending
    view_asc = obj_unsorted.view(order_by="value ASC")
    print(f"ORDER BY value ASC: {await view_asc.data()}")

    # Sort descending
    view_desc = obj_unsorted.view(order_by="value DESC")
    print(f"ORDER BY value DESC: {await view_desc.data()}")

    # Example 4: Mixed constraints with int scalar array
    print("\n" + "=" * 50)
    print("Example 4: Mixed constraints with int scalar array")
    print("-" * 50)

    obj_mixed = await create_object_from_value([15, 8, 42, 23, 4, 16, 35, 12, 28, 50])
    print(f"Original data: {await obj_mixed.data()}\n")

    # WHERE + LIMIT
    view_where_limit = obj_mixed.view(where="value > 10", limit=4)
    print(f"WHERE value > 10 LIMIT 4: {await view_where_limit.data()}")

    # WHERE + ORDER BY + LIMIT
    view_complex = obj_mixed.view(where="value >= 15", order_by="value DESC", limit=3)
    print(f"WHERE value >= 15 ORDER BY value DESC LIMIT 3: {await view_complex.data()}")

    # WHERE + ORDER BY + OFFSET + LIMIT (pagination)
    view_paginate = obj_mixed.view(where="value > 10", order_by="value ASC", offset=2, limit=3)
    print(f"WHERE value > 10 ORDER BY value ASC OFFSET 2 LIMIT 3: {await view_paginate.data()}")

    # Example 5: WHERE clause with dict of scalars
    print("\n" + "=" * 50)
    print("Example 5: WHERE clause with dict of scalars")
    print("-" * 50)

    obj_dict_scalar = await create_object_from_value(
        {"id": 101, "name": "Alice", "age": 30, "score": 95.5}
    )
    print(f"Original data: {await obj_dict_scalar.data()}\n")

    # Filter by age
    view_dict_where = obj_dict_scalar.view(where="age >= 25")
    print(f"WHERE age >= 25: {await view_dict_where.data()}")

    # Filter by score
    view_dict_score = obj_dict_scalar.view(where="score > 90.0")
    print(f"WHERE score > 90.0: {await view_dict_score.data()}")

    # Example 6: WHERE clause with dict of arrays
    print("\n" + "=" * 50)
    print("Example 6: WHERE clause with dict of arrays")
    print("-" * 50)

    obj_dict_arrays = await create_object_from_value(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"],
            "age": [25, 30, 35, 28, 22, 40],
            "score": [85.5, 92.0, 78.5, 95.0, 88.0, 91.5]
        }
    )
    print(f"Original data (all rows):")
    all_rows = await obj_dict_arrays.data(orient=ORIENT_RECORDS)
    for row in all_rows:
        print(f"  {row}")
    print()

    # Filter by age
    view_dict_age = obj_dict_arrays.view(where="age >= 30")
    filtered_rows = await view_dict_age.data(orient=ORIENT_RECORDS)
    print(f"WHERE age >= 30:")
    for row in filtered_rows:
        print(f"  {row}")
    print()

    # Filter by score
    view_dict_score_arr = obj_dict_arrays.view(where="score > 90.0")
    score_rows = await view_dict_score_arr.data(orient=ORIENT_RECORDS)
    print(f"WHERE score > 90.0:")
    for row in score_rows:
        print(f"  {row}")

    # Example 7: LIMIT and OFFSET with dict of arrays
    print("\n" + "=" * 50)
    print("Example 7: LIMIT and OFFSET with dict of arrays")
    print("-" * 50)

    people = await create_object_from_value(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"],
            "department": ["HR", "IT", "Sales", "IT", "HR", "Sales", "IT", "HR"]
        }
    )
    print(f"Original data (8 people):")
    all_people = await people.data(orient=ORIENT_RECORDS)
    for person in all_people:
        print(f"  {person}")
    print()

    # First page (first 3 records)
    page1 = people.view(limit=3)
    page1_data = await page1.data(orient=ORIENT_RECORDS)
    print(f"Page 1 (LIMIT 3):")
    for person in page1_data:
        print(f"  {person}")
    print()

    # Second page (skip 3, get next 3)
    page2 = people.view(offset=3, limit=3)
    page2_data = await page2.data(orient=ORIENT_RECORDS)
    print(f"Page 2 (OFFSET 3 LIMIT 3):")
    for person in page2_data:
        print(f"  {person}")
    print()

    # Third page (skip 6, get remaining)
    page3 = people.view(offset=6)
    page3_data = await page3.data(orient=ORIENT_RECORDS)
    print(f"Page 3 (OFFSET 6):")
    for person in page3_data:
        print(f"  {person}")

    # Example 8: ORDER BY with dict of arrays
    print("\n" + "=" * 50)
    print("Example 8: ORDER BY with dict of arrays")
    print("-" * 50)

    products = await create_object_from_value(
        {
            "id": [101, 102, 103, 104, 105],
            "name": ["Laptop", "Mouse", "Keyboard", "Monitor", "Headset"],
            "price": [999.99, 25.50, 75.00, 350.00, 125.00],
            "stock": [15, 50, 30, 8, 22]
        }
    )
    print(f"Original data:")
    orig_products = await products.data(orient=ORIENT_RECORDS)
    for prod in orig_products:
        print(f"  {prod}")
    print()

    # Sort by price ascending
    by_price_asc = products.view(order_by="price ASC")
    price_asc_data = await by_price_asc.data(orient=ORIENT_RECORDS)
    print(f"ORDER BY price ASC:")
    for prod in price_asc_data:
        print(f"  {prod}")
    print()

    # Sort by price descending
    by_price_desc = products.view(order_by="price DESC")
    price_desc_data = await by_price_desc.data(orient=ORIENT_RECORDS)
    print(f"ORDER BY price DESC:")
    for prod in price_desc_data:
        print(f"  {prod}")
    print()

    # Sort by stock ascending
    by_stock = products.view(order_by="stock ASC")
    stock_data = await by_stock.data(orient=ORIENT_RECORDS)
    print(f"ORDER BY stock ASC:")
    for prod in stock_data:
        print(f"  {prod}")

    # Example 9: Mixed constraints with dict of arrays
    print("\n" + "=" * 50)
    print("Example 9: Mixed constraints with dict of arrays")
    print("-" * 50)

    students = await create_object_from_value(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"],
            "grade": [85, 92, 78, 95, 88, 72, 90, 83, 96, 87],
            "class": ["A", "B", "A", "B", "A", "B", "A", "B", "A", "B"]
        }
    )
    print(f"Original data (10 students):")
    all_students = await students.data(orient=ORIENT_RECORDS)
    for student in all_students:
        print(f"  {student}")
    print()

    # Top 3 students with grade >= 85
    top_students = students.view(where="grade >= 85", order_by="grade DESC", limit=3)
    top_data = await top_students.data(orient=ORIENT_RECORDS)
    print(f"Top 3 students with grade >= 85:")
    for student in top_data:
        print(f"  {student}")
    print()

    # Students in class A with grade > 80, sorted by grade
    class_a_good = students.view(where="class = 'A' AND grade > 80", order_by="grade DESC")
    class_a_data = await class_a_good.data(orient=ORIENT_RECORDS)
    print(f"Class A students with grade > 80, sorted by grade DESC:")
    for student in class_a_data:
        print(f"  {student}")
    print()

    # Pagination: second page of students with grade >= 85 (sorted by grade)
    page_2_high_grades = students.view(
        where="grade >= 85",
        order_by="grade DESC",
        offset=3,
        limit=3
    )
    page_2_data = await page_2_high_grades.data(orient=ORIENT_RECORDS)
    print(f"Page 2 of students with grade >= 85 (OFFSET 3 LIMIT 3):")
    for student in page_2_data:
        print(f"  {student}")

    # Example 10: Views are read-only
    print("\n" + "=" * 50)
    print("Example 10: Views are read-only")
    print("-" * 50)

    obj = await create_object_from_value([1, 2, 3, 4, 5])
    print(f"Original data: {await obj.data()}\n")

    view = obj.view(where="value > 2")
    print(f"View data (value > 2): {await view.data()}")
    print(f"Attempting to insert into view...")

    try:
        await view.insert(6)
        print("ERROR: Insert should have failed!")
    except RuntimeError as e:
        print(f"Expected error: {e}")

    # Example 11: Views work with operators
    print("\n" + "=" * 50)
    print("Example 11: Views work with operators")
    print("-" * 50)

    obj_a = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    obj_b = await create_object_from_value([10, 20, 30, 40, 50, 60, 70, 80, 90, 100])

    print(f"Original data A: {await obj_a.data()}")
    print(f"Original data B: {await obj_b.data()}\n")

    # Create views
    view_a = obj_a.view(where="value > 5", limit=3)
    view_b = obj_b.view(where="value <= 50", limit=3)

    print(f"View A (value > 5, limit 3): {await view_a.data()}")
    print(f"View B (value <= 50, limit 3): {await view_b.data()}\n")

    # Add views
    result_add = await (view_a + view_b)
    print(f"View A + View B: {await result_add.data()}")

    # Multiply views
    result_mul = await (view_a * view_b)
    print(f"View A * View B: {await result_mul.data()}")

    # Note: All objects created via context are automatically cleaned up when context exits
    print("\n" + "=" * 50)
    print("Cleanup: All context-created objects will be cleaned up automatically")
    print("-" * 50)


async def main():
    """Main entry point that creates context and calls example."""
    async with DataContext() as context:
        await example(context)


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Views Example")
    print("=" * 50)
    print("\nNote: This example requires a running ClickHouse server")
    print("      on localhost:8123\n")
    asyncio.run(main())
