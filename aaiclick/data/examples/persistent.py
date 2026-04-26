"""
Persistent objects example for aaiclick.

Demonstrates how named persistent tables (``p_<name>``) survive across
``data_context()`` exits — re-openable via ``open_object()`` in a fresh
context. Run with the local backend (file-backed chdb + SQLite); no
orchestration setup required.
"""

import asyncio

from aaiclick import create_object_from_value
from aaiclick.data.data_context import (
    data_context,
    delete_persistent_object,
    list_persistent_objects,
    open_object,
)


async def example_create():
    """Create a persistent object with name=."""
    print("Example 1: Create a persistent object")
    print("-" * 50)
    obj = await create_object_from_value(
        {"city": ["Tokyo", "London", "NYC"], "pop_m": [14, 9, 8]},
        name="cities",
        scope="global",
    )
    print(f"Created: {obj.table}")  # → p_cities
    print(f"Data: {await obj.data()}")  # → {'city': ['Tokyo', 'London', 'NYC'], 'pop_m': [14, 9, 8]}


async def example_reopen():
    """Reopen the persistent object in a brand-new data_context."""
    print("\nExample 2: Reopen in a fresh data_context")
    print("-" * 50)
    obj = await open_object("cities")
    print(f"Reopened: {obj.table}")  # → p_cities
    print(f"Data: {await obj.data()}")  # → {'city': ['Tokyo', 'London', 'NYC'], 'pop_m': [14, 9, 8]}


async def example_append():
    """Append more rows by re-creating with the same name."""
    print("\nExample 3: Append data")
    print("-" * 50)
    obj = await create_object_from_value(
        {"city": ["Paris", "Berlin"], "pop_m": [11, 4]},
        name="cities",
        scope="global",
    )
    print(f"After append: {await obj.data()}")
    # → {'city': ['Tokyo', 'London', 'NYC', 'Paris', 'Berlin'], 'pop_m': [14, 9, 8, 11, 4]}


async def example_list_and_cleanup():
    """List then delete the persistent object."""
    print("\nExample 4: List and delete")
    print("-" * 50)
    print(f"Persistent objects: {await list_persistent_objects()}")  # → ['cities', ...]
    await delete_persistent_object("cities")
    print(f"After delete: {'cities' in await list_persistent_objects()}")  # → False


async def amain():
    async with data_context():
        await example_create()
    async with data_context():
        await example_reopen()
    async with data_context():
        await example_append()
    async with data_context():
        await example_list_and_cleanup()


if __name__ == "__main__":
    print("=" * 50)
    print("aaiclick Persistent Objects Example")
    print("=" * 50)
    asyncio.run(amain())
