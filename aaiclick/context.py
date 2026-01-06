"""
aaiclick.context - Context manager for managing ClickHouse client and Object lifecycle.

This module provides a context manager that manages the lifecycle of Objects created
within its scope, automatically cleaning up tables when the context exits.
"""

from __future__ import annotations

from typing import Optional, Dict, Union, List
import weakref

import numpy as np
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
from .snowflake import get_snowflake_ids


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

    def _register_object(self, obj: Object) -> None:
        """
        Register an Object to be tracked by this context.

        Args:
            obj: Object instance to register
        """
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
                await self._delete_object(obj)

        # Clear the tracking dict
        self._objects.clear()

        return False

    async def _delete_object(self, obj: Object) -> None:
        """
        Internal method to delete an object's table and mark it as stale.

        Args:
            obj: Object to delete
        """
        await self.ch_client.command(f"DROP TABLE IF EXISTS {obj.table}")
        obj._ctx = None

    async def delete(self, obj: Object) -> None:
        """
        Delete an Object's table and mark it as stale.

        This removes the Object from tracking and cleans up its ClickHouse table.

        Args:
            obj: Object to delete

        Example:
            >>> async with Context() as ctx:
            ...     obj = await ctx.create_object_from_value([1, 2, 3])
            ...     result = await (obj + obj)
            ...     await ctx.delete(result)  # Clean up intermediate result
        """
        # Delete the table and mark as stale
        await self._delete_object(obj)

        # Remove from tracking if present
        obj_id = id(obj)
        if obj_id in self._objects:
            del self._objects[obj_id]

    async def create_object(self, schema: Schema):
        """
        Create a new Object with a ClickHouse table using the specified schema.

        This is the ONLY method that creates Objects and their tables in the entire codebase.
        All objects are automatically registered and cleaned up when context exits.

        Args:
            schema: Schema dataclass with fieldtype and columns dict.
                   Example: Schema(
                       fieldtype='a',
                       columns={"aai_id": "UInt64", "value": "Float64"}
                   )

        Returns:
            Object: New Object instance with created table

        Examples:
            >>> async with Context() as ctx:
            ...     from aaiclick.object import Schema
            ...     schema = Schema(
            ...         fieldtype='a',
            ...         columns={"aai_id": "UInt64", "value": "Float64"}
            ...     )
            ...     obj = await ctx.create_object(schema)
        """
        from .object import Object
        from .models import ColumnMeta, FIELDTYPE_SCALAR

        obj = Object(self)

        # Build column definitions with comments derived from fieldtype
        columns = []
        for name, col_type in schema.columns.items():
            col_def = f"{name} {col_type}"
            # Determine comment based on column name and schema fieldtype
            if name == "aai_id":
                comment = ColumnMeta(fieldtype=FIELDTYPE_SCALAR).to_yaml()
            else:
                comment = ColumnMeta(fieldtype=schema.fieldtype).to_yaml()

            if comment:
                col_def += f" COMMENT '{comment}'"
            columns.append(col_def)

        # Create table with all columns and comments in single query
        create_query = f"""
        CREATE TABLE {obj.table} (
            {', '.join(columns)}
        ) ENGINE = MergeTree ORDER BY tuple()
        """
        await self.ch_client.command(create_query)

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
        obj = await create_object_from_value(val, ctx=self)
        self._register_object(obj)
        return obj


# Type aliases
ValueScalarType = Union[int, float, bool, str]
ValueListType = Union[List[int], List[float], List[bool], List[str]]
ValueType = Union[ValueScalarType, ValueListType, Dict[str, Union[ValueScalarType, ValueListType]]]


def _infer_clickhouse_type(value: Union[ValueScalarType, ValueListType]) -> str:
    """
    Infer ClickHouse column type from Python value using numpy.

    Args:
        value: Python value (scalar or list)

    Returns:
        str: ClickHouse type string
    """
    if isinstance(value, list):
        if not value:
            return "String"  # Default for empty list

        # Use numpy to infer the dtype
        arr = np.array(value)
        dtype = arr.dtype

        # Map numpy dtype to ClickHouse type
        if np.issubdtype(dtype, np.bool_):
            return "UInt8"
        elif np.issubdtype(dtype, np.integer):
            return "Int64"
        elif np.issubdtype(dtype, np.floating):
            return "Float64"
        else:
            return "String"

    # Scalar value type inference
    if isinstance(value, bool):
        return "UInt8"
    elif isinstance(value, int):
        return "Int64"
    elif isinstance(value, float):
        return "Float64"
    elif isinstance(value, str):
        return "String"
    else:
        return "String"  # Default fallback


async def create_object_from_value(val: ValueType, ctx: Context) -> Object:
    """
    Create a new Object from Python values with automatic schema inference.

    Internal function - use Context.create_object_from_value() instead.

    Args:
        val: Value to create object from. Can be:
            - Scalar (int, float, bool, str): Creates "aai_id" and "value" columns, single row
            - List of scalars: Creates "aai_id" and "value" columns with multiple rows
            - Dict of scalars: Creates "aai_id" plus one column per key, single row
            - Dict of arrays: Creates "aai_id" plus one column per key, multiple rows
        ctx: Context instance managing this object

    Returns:
        Object: New Object instance with data

    Table Schema Details:
        - All tables include aai_id column with snowflake IDs
        - Scalars (single value): Single row with aai_id and value
        - Arrays (lists): Multiple rows with aai_id and value, ordered by aai_id
        - Dict of scalars: Single row with aai_id plus columns for each key
        - Dict of arrays: Multiple rows with aai_id plus columns for each key, ordered by aai_id
    """
    from .object import Object
    from .models import Schema, FIELDTYPE_SCALAR, FIELDTYPE_ARRAY

    if isinstance(val, dict):
        # Check if any values are lists (dict of arrays)
        has_arrays = any(isinstance(v, list) for v in val.values())

        if has_arrays:
            # Dict of arrays: one column per key, one row per array element
            # All arrays must have the same length
            columns = {"aai_id": "UInt64"}
            array_len = None

            # First pass: build schema and validate array lengths
            for key, value in val.items():
                if isinstance(value, list):
                    if array_len is None:
                        array_len = len(value)
                    elif len(value) != array_len:
                        raise ValueError(
                            f"All arrays must have same length. "
                            f"Expected {array_len}, got {len(value)} for key '{key}'"
                        )
                    col_type = _infer_clickhouse_type(value)
                else:
                    raise ValueError(
                        f"Dict of arrays requires all values to be lists. "
                        f"Key '{key}' has type {type(value).__name__}"
                    )
                columns[key] = col_type

            schema = Schema(
                fieldtype=FIELDTYPE_ARRAY,
                columns=columns
            )

            # Create object with schema
            obj = await ctx.create_object(schema)

            # Generate snowflake IDs for all rows
            aai_ids = get_snowflake_ids(array_len or 0)

            # Build data rows for bulk insert
            if array_len and array_len > 0:
                keys = list(val.keys())
                # Zip aai_ids with all column arrays to create rows
                data = [list(row) for row in zip(aai_ids, *[val[key] for key in keys])]

                # Use clickhouse-connect's built-in insert
                await ctx.ch_client.insert(obj.table, data)

        else:
            # Dict of scalars: one column per key, single row with aai_id
            columns = {"aai_id": "UInt64"}
            values = []

            for key, value in val.items():
                col_type = _infer_clickhouse_type(value)
                columns[key] = col_type

                # Format value for SQL
                if isinstance(value, str):
                    values.append(f"'{value}'")
                elif isinstance(value, bool):
                    values.append("1" if value else "0")
                else:
                    values.append(str(value))

            schema = Schema(
                fieldtype=FIELDTYPE_SCALAR,
                columns=columns
            )

            # Create object with schema
            obj = await ctx.create_object(schema)

            # Generate single aai_id for scalar dict
            aai_id = get_snowflake_ids(1)[0]
            values.insert(0, str(aai_id))

            # Insert single row
            insert_query = f"INSERT INTO {obj.table} VALUES ({', '.join(values)})"
            await ctx.ch_client.command(insert_query)

    elif isinstance(val, list):
        # List: single column "value" with multiple rows
        # Add aai_id column to ensure stable ordering for element-wise operations
        col_type = _infer_clickhouse_type(val)

        schema = Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns={"aai_id": "UInt64", "value": col_type}
        )

        # Create object with schema
        obj = await ctx.create_object(schema)

        # Generate snowflake IDs for all rows
        aai_ids = get_snowflake_ids(len(val))

        # Build data rows for bulk insert
        if val:
            # Zip aai_ids with values to create rows
            data = [list(row) for row in zip(aai_ids, val)]
            # Use clickhouse-connect's built-in insert
            await ctx.ch_client.insert(obj.table, data)

    else:
        # Scalar: single row with aai_id and value
        col_type = _infer_clickhouse_type(val)

        schema = Schema(
            fieldtype=FIELDTYPE_SCALAR,
            columns={"aai_id": "UInt64", "value": col_type}
        )

        # Create object with schema
        obj = await ctx.create_object(schema)

        # Generate single aai_id for scalar
        aai_id = get_snowflake_ids(1)[0]

        # Insert single row
        if isinstance(val, str):
            value_str = f"'{val}'"
        elif isinstance(val, bool):
            value_str = "1" if val else "0"
        else:
            value_str = str(val)

        insert_query = f"INSERT INTO {obj.table} VALUES ({aai_id}, {value_str})"
        await ctx.ch_client.command(insert_query)

    return obj
