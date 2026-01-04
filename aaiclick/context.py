"""
aaiclick.context - Context manager for managing ClickHouse client and Object lifecycle.

This module provides a context manager that manages the lifecycle of Objects created
within its scope, automatically cleaning up tables when the context exits.
"""

from typing import Optional, Dict
import weakref

from clickhouse_connect import get_async_client
from clickhouse_connect.driver.asyncclient import AsyncClient
from urllib3 import PoolManager

from .env import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DB,
)


# Global connection pool shared across all Context instances
_pool: list = [None]


def get_pool() -> PoolManager:
    """
    Get or create the global urllib3 connection pool.

    Returns:
        PoolManager: Shared connection pool for ClickHouse clients
    """
    if _pool[0] is None:
        _pool[0] = PoolManager(num_pools=10, maxsize=10)
    return _pool[0]


async def get_ch_client() -> AsyncClient:
    """
    Create a ClickHouse client using the shared connection pool.

    Connection parameters are read from environment variables:
    - CLICKHOUSE_HOST (default: "localhost")
    - CLICKHOUSE_PORT (default: 8123)
    - CLICKHOUSE_USER (default: "default")
    - CLICKHOUSE_PASSWORD (default: "")
    - CLICKHOUSE_DB (default: "default")

    Returns:
        AsyncClient: ClickHouse client instance
    """
    return await get_async_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
        pool_mgr=get_pool(),
    )


class Context:
    """
    Context manager for managing ClickHouse client and Object lifecycle.

    This context manager:
    - Manages a ClickHouse client instance (automatically initialized on enter)
    - Tracks all Objects created within the context via weakref
    - Automatically deletes tables and marks Objects as stale on exit

    Example:
        >>> async with Context() as ctx:
        ...     obj = await ctx.create_object_from_value([1, 2, 3])
        ...     # Use obj...
        ... # Tables are automatically deleted here
    """

    def __init__(self):
        """Initialize a Context."""
        self._ch_client: Optional[AsyncClient] = None
        self._objects: Dict[int, weakref.ref] = {}

    @property
    def ch_client(self) -> AsyncClient:
        """
        Get the ClickHouse client for this context.

        Returns:
            AsyncClient: The ClickHouse client (initialized in __aenter__)

        Raises:
            RuntimeError: If accessed outside of context manager
        """
        if self._ch_client is None:
            raise RuntimeError(
                "Context client not initialized. Use 'async with Context() as ctx:' to enter context."
            )
        return self._ch_client

    def _register_object(self, obj: "Object") -> None:
        """
        Register an Object to be tracked by this context.

        Args:
            obj: Object instance to register
        """
        from .object import Object

        # Use id(obj) as key and weakref as value
        self._objects[id(obj)] = weakref.ref(obj)

    async def __aenter__(self):
        """Enter the context, initializing the client if needed."""
        if self._ch_client is None:
            self._ch_client = await get_async_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DB,
                pool_mgr=get_pool(),
            )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context, cleaning up all tracked Objects.

        Deletes all tables and marks Objects as stale.
        """
        # Clean up all tracked objects
        for obj_ref in self._objects.values():
            obj = obj_ref()
            if obj is not None and not obj.stale:
                await obj.delete_table()
                obj._stale = True

        # Clear the tracking dict
        self._objects.clear()

        return False

    async def create_object(self, schema):
        """
        Create a new Object with a ClickHouse table using the specified schema.

        Args:
            schema: Column definition(s). Can be:
                - str: Single column definition (e.g., "value Float64")
                - list[str]: Multiple column definitions (e.g., ["id Int64", "value Float64"])

        Returns:
            Object: New Object instance with created table

        Examples:
            >>> async with Context() as ctx:
            ...     obj = await ctx.create_object("value Float64")
            ...     # Multiple columns
            ...     obj2 = await ctx.create_object(["id Int64", "name String"])
        """
        from .factories import create_object

        obj = await create_object(schema, ch_client=self.ch_client)
        self._register_object(obj)
        return obj

    async def create_object_from_value(self, val):
        """
        Create a new Object from Python values with automatic schema inference.

        Args:
            val: Value to create object from. Can be:
                - Scalar (int, float, bool, str): Creates "aai_id" and "value" columns
                - List of scalars: Creates "aai_id" and "value" columns with multiple rows
                - Dict of scalars: Creates "aai_id" plus one column per key
                - Dict of arrays: Creates "aai_id" plus one column per key with multiple rows

        Returns:
            Object: New Object instance with data

        Examples:
            >>> async with Context() as ctx:
            ...     # Scalar
            ...     obj = await ctx.create_object_from_value(42)
            ...     # List
            ...     obj = await ctx.create_object_from_value([1, 2, 3])
            ...     # Dict
            ...     obj = await ctx.create_object_from_value({"x": [1, 2], "y": [3, 4]})
        """
        from .factories import create_object_from_value

        obj = await create_object_from_value(val, ch_client=self.ch_client)
        self._register_object(obj)
        return obj
