"""
aaiclick.context - Context manager for managing ClickHouse client and Object lifecycle.

This module provides a context manager that manages the lifecycle of Objects created
within its scope, automatically cleaning up tables when the context exits.
"""

from typing import Optional, Dict
import weakref
from clickhouse_connect.driver.asyncclient import AsyncClient
from .ch_client import get_ch_client


class Context:
    """
    Context manager for managing ClickHouse client and Object lifecycle.

    This context manager:
    - Manages a ClickHouse client instance
    - Tracks all Objects created within the context via weakref
    - Automatically deletes tables and marks Objects as stale on exit

    Example:
        >>> async with Context() as ctx:
        ...     obj = await ctx.create_object_from_value([1, 2, 3])
        ...     # Use obj...
        ... # Tables are automatically deleted here
    """

    def __init__(self, ch_client: Optional[AsyncClient] = None):
        """
        Initialize a Context.

        Args:
            ch_client: Optional ClickHouse client. If not provided, uses global client.
        """
        self._ch_client = ch_client
        self._objects: Dict[int, weakref.ref] = {}

    @property
    def ch_client(self) -> Optional[AsyncClient]:
        """Get the ClickHouse client for this context."""
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
            self._ch_client = await get_ch_client()
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

        This is a wrapper around factories.create_object that registers the object
        with this context.

        Args:
            schema: Column definition(s). See factories.create_object for details.

        Returns:
            Object: New Object instance with created table
        """
        from .factories import create_object

        obj = await create_object(schema, context=self)
        self._register_object(obj)
        return obj

    async def create_object_from_value(self, val):
        """
        Create a new Object from Python values with automatic schema inference.

        This is a wrapper around factories.create_object_from_value that registers
        the object with this context.

        Args:
            val: Value to create object from. See factories.create_object_from_value for details.

        Returns:
            Object: New Object instance with data
        """
        from .factories import create_object_from_value

        obj = await create_object_from_value(val, context=self)
        self._register_object(obj)
        return obj
