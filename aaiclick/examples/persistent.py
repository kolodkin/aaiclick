"""
Persistent objects example for aaiclick.

This example demonstrates how to create, reopen, and manage persistent
objects that survive across data_context() exits.
"""

import asyncio

from aaiclick import create_object_from_value, open_object
from aaiclick.data.data_context import (
    data_context,
    delete_persistent_object,
    list_persistent_objects,
)


async def example():
    """Run all persistent object examples."""
    # Example 1: Create a persistent object with name=
    print("Example 1: Create a persistent object")
    print("-" * 50)

    obj = await create_object_from_value(
        {"city": ["Tokyo", "London", "NYC"], "pop_m": [14, 9, 8]},
        name="cities",
    )
    print(f"Created persistent object: {obj}")  # → Object(table='p_cities')
    print(f"Data: {await obj.data()}")  # → {'city': ['Tokyo', 'London', 'NYC'], 'pop_m': [14, 9, 8]}


async def example_reopen():
    """Reopen a persistent object in a new context."""
    # Example 2: Reopen in a new data_context with open_object()
    print("\nExample 2: Reopen in a new data_context")
    print("-" * 50)

    obj = await open_object("cities")
    print(f"Reopened: {obj}")  # → Object(table='p_cities')
    print(f"Data: {await obj.data()}")  # → {'city': ['Tokyo', 'London', 'NYC'], 'pop_m': [14, 9, 8]}

    # Operations work normally
    total_pop = await (await obj["pop_m"].sum()).data()
    print(f"Total population: {total_pop}M")  # → 31M


async def example_append():
    """Append data to an existing persistent object."""
    # Example 3: Append data — create_object_from_value with same name appends
    print("\nExample 3: Append data to persistent object")
    print("-" * 50)

    obj = await create_object_from_value(
        {"city": ["Paris", "Berlin"], "pop_m": [11, 4]},
        name="cities",
    )
    print(f"After append: {await obj.data()}")  # → {'city': ['Tokyo', 'London', 'NYC', 'Paris', 'Berlin'], 'pop_m': [14, 9, 8, 11, 4]}


async def example_list_and_cleanup():
    """List and delete persistent objects."""
    # Example 4: List persistent objects
    print("\nExample 4: List and delete persistent objects")
    print("-" * 50)

    names = await list_persistent_objects()
    print(f"Persistent objects: {names}")  # → ['cities']

    # Clean up
    await delete_persistent_object("cities")
    names = await list_persistent_objects()
    print(f"After delete: {names}")  # → []


async def amain():
    """Main entry point."""
    # First context: create persistent object
    async with data_context():
        await example()

    # Second context: reopen — data survives!
    async with data_context():
        await example_reopen()

    # Third context: append more data
    async with data_context():
        await example_append()

    # Fourth context: list and cleanup
    async with data_context():
        await example_list_and_cleanup()


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Persistent Objects Example")
    print("=" * 50)
    asyncio.run(amain())
