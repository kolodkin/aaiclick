"""
Group By example for aaiclick.

This example demonstrates how to use group_by operations on Objects
to aggregate data per group, including single/multi-key grouping,
various aggregation methods, multi-agg, HAVING filters, and View support.
"""

import asyncio

from aaiclick import DataContext, create_object_from_value


async def example(context):
    """Run all group_by examples using the provided context."""
    # Example 1: Basic group_by with sum
    print("Example 1: Basic group_by with sum")
    print("-" * 50)

    sales = await create_object_from_value({
        "category": ["Electronics", "Electronics", "Clothing", "Clothing", "Food"],
        "amount": [500, 300, 150, 200, 80],
    })
    result = await sales.group_by("category").sum("amount")
    data = await result.data()
    print(f"Sales by category (sum):")
    for cat, amt in sorted(zip(data["category"], data["amount"])):
        print(f"  {cat}: ${amt}")

    # Example 2: Count per group
    print("\n" + "=" * 50)
    print("Example 2: Count per group")
    print("-" * 50)

    result = await sales.group_by("category").count()
    data = await result.data()
    print(f"Transaction count by category:")
    for cat, cnt in sorted(zip(data["category"], data["_count"])):
        print(f"  {cat}: {cnt} transactions")

    # Example 3: Multiple group keys
    print("\n" + "=" * 50)
    print("Example 3: Multiple group keys")
    print("-" * 50)

    orders = await create_object_from_value({
        "region": ["East", "East", "West", "West", "East", "West"],
        "category": ["A", "B", "A", "B", "A", "A"],
        "revenue": [100, 200, 150, 250, 120, 180],
    })
    result = await orders.group_by("region", "category").sum("revenue")
    data = await result.data()
    print(f"Revenue by region + category:")
    triples = sorted(zip(data["region"], data["category"], data["revenue"]))
    for region, cat, rev in triples:
        print(f"  {region} / {cat}: ${rev}")

    # Example 4: Multi-aggregation with agg()
    print("\n" + "=" * 50)
    print("Example 4: Multi-aggregation with agg()")
    print("-" * 50)

    products = await create_object_from_value({
        "category": ["Electronics", "Electronics", "Clothing", "Clothing"],
        "price": [999.99, 499.99, 59.99, 89.99],
        "quantity": [10, 25, 100, 75],
    })
    result = await products.group_by("category").agg({
        "price": "mean",
        "quantity": "sum",
    })
    data = await result.data()
    print(f"Product stats by category:")
    for i, cat in enumerate(data["category"]):
        print(f"  {cat}: avg price=${data['price'][i]:.2f}, total qty={data['quantity'][i]}")

    # Example 5: Statistical aggregations (std, var)
    print("\n" + "=" * 50)
    print("Example 5: Statistical aggregations")
    print("-" * 50)

    scores = await create_object_from_value({
        "class": ["A", "A", "A", "A", "B", "B", "B", "B"],
        "score": [85, 90, 78, 92, 70, 95, 60, 88],
    })
    result = await scores.group_by("class").agg({
        "score": "mean",
    })
    data = await result.data()
    print(f"Mean score by class:")
    for cls, score in sorted(zip(data["class"], data["score"])):
        print(f"  Class {cls}: {score:.1f}")

    std_result = await scores.group_by("class").std("score")
    std_data = await std_result.data()
    print(f"Score std deviation by class:")
    for cls, std in sorted(zip(std_data["class"], std_data["score"])):
        print(f"  Class {cls}: {std:.2f}")

    # Example 6: HAVING — filter groups after aggregation
    print("\n" + "=" * 50)
    print("Example 6: HAVING — filter groups after aggregation")
    print("-" * 50)

    transactions = await create_object_from_value({
        "store": ["NYC", "NYC", "NYC", "LA", "LA", "Chicago"],
        "amount": [500, 300, 200, 100, 50, 800],
    })
    print("All stores:")
    all_result = await transactions.group_by("store").sum("amount")
    all_data = await all_result.data()
    for store, amt in sorted(zip(all_data["store"], all_data["amount"])):
        print(f"  {store}: ${amt}")

    print("\nStores with total > $200 (HAVING):")
    filtered = await transactions.group_by("store").having("sum(amount) > 200").sum("amount")
    fdata = await filtered.data()
    for store, amt in sorted(zip(fdata["store"], fdata["amount"])):
        print(f"  {store}: ${amt}")

    # Example 7: WHERE + HAVING combined
    print("\n" + "=" * 50)
    print("Example 7: WHERE + HAVING combined")
    print("-" * 50)

    print("Filter rows WHERE amount >= 100, then HAVING count() >= 2:")
    view = transactions.view(where="amount >= 100")
    result = await view.group_by("store").having("count() >= 2").count()
    data = await result.data()
    for store, cnt in sorted(zip(data["store"], data["_count"])):
        print(f"  {store}: {cnt} large transactions")

    # Example 8: Chained HAVING with AND
    print("\n" + "=" * 50)
    print("Example 8: Chained HAVING with AND")
    print("-" * 50)

    print("Stores with total > $200 AND at least 2 transactions:")
    result = await (
        transactions.group_by("store")
        .having("sum(amount) > 200")
        .having("count() >= 2")
        .sum("amount")
    )
    data = await result.data()
    for store, amt in sorted(zip(data["store"], data["amount"])):
        print(f"  {store}: ${amt}")

    # Example 9: OR HAVING
    print("\n" + "=" * 50)
    print("Example 9: OR HAVING")
    print("-" * 50)

    print("Stores with total > $700 OR only 1 transaction:")
    result = await (
        transactions.group_by("store")
        .having("sum(amount) > 700")
        .or_having("count() = 1")
        .sum("amount")
    )
    data = await result.data()
    for store, amt in sorted(zip(data["store"], data["amount"])):
        print(f"  {store}: ${amt}")

    # Example 10: Array value_counts pattern
    print("\n" + "=" * 50)
    print("Example 10: Array value_counts pattern")
    print("-" * 50)

    colors = await create_object_from_value(["red", "blue", "red", "green", "blue", "red"])
    counts = await colors.group_by("value").count()
    data = await counts.data()
    print(f"Color frequencies:")
    for val, cnt in sorted(zip(data["value"], data["_count"]), key=lambda x: -x[1]):
        print(f"  {val}: {cnt}")

    # Example 11: Working with group_by results
    print("\n" + "=" * 50)
    print("Example 11: Working with group_by results")
    print("-" * 50)

    result = await sales.group_by("category").sum("amount")
    print(f"Group by result is a normal dict Object")

    # Field selection on result
    amounts = result["amount"]
    print(f"  Select 'amount' column: {await amounts.data()}")

    # Further aggregation on result
    total = await amounts.sum()
    print(f"  Total across all groups: ${await total.data()}")

    # Orient as records
    records = await result.data(orient="records")
    print(f"  As records: {records}")

    print("\n" + "=" * 50)
    print("Cleanup: All context-created objects will be cleaned up automatically")
    print("-" * 50)


async def amain():
    """Main entry point that creates context and calls example."""
    async with DataContext() as context:
        await example(context)


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Group By Example")
    print("=" * 50)
    print("\nNote: This example requires a running ClickHouse server")
    print("      on localhost:8123\n")
    asyncio.run(amain())
