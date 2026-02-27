"""
aaiclick.data.data_context - DataContext manager for managing ClickHouse client and Object lifecycle.

This module provides a context manager that manages the lifecycle of Objects created
within its scope, automatically cleaning up tables when the context exits.
"""

from __future__ import annotations

import re
from contextvars import ContextVar
from typing import Optional, Union, List, Dict
from urllib.parse import urlparse
import weakref

import numpy as np
from clickhouse_connect import get_async_client
from clickhouse_connect.driver.asyncclient import AsyncClient
from urllib3 import PoolManager

from .env import get_ch_creds
from .lifecycle import LifecycleHandler, LocalLifecycleHandler
from .models import (
    ClickHouseCreds,
    ValueScalarType,
    ValueListType,
    ValueType,
    Schema,
    ColumnMeta,
    FIELDTYPE_SCALAR,
    FIELDTYPE_ARRAY,
    EngineType,
    ENGINE_DEFAULT,
)
from ..snowflake_id import get_snowflake_id, get_snowflake_ids


# Global ContextVar to hold the current DataContext instance
_current_context: ContextVar['DataContext'] = ContextVar('current_context')


def get_data_context() -> 'DataContext':
    """
    Get the current DataContext instance from ContextVar.

    Returns:
        DataContext: The active DataContext instance

    Raises:
        RuntimeError: If no active context (must be called within 'async with DataContext()')
    """
    try:
        return _current_context.get()
    except LookupError:
        raise RuntimeError("No active context - must be called within 'async with DataContext()'")


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


async def get_ch_client(creds: ClickHouseCreds | None = None) -> AsyncClient:
    """
    Create a ClickHouse client using the shared connection pool.

    Args:
        creds: ClickHouse credentials. If None, reads from environment variables.

    Returns:
        AsyncClient: ClickHouse client instance
    """
    if creds is None:
        creds = get_ch_creds()

    return await get_async_client(
        host=creds.host,
        port=creds.port,
        username=creds.user,
        password=creds.password,
        database=creds.database,
        pool_mgr=get_pool(),
    )


class DataContext:
    """
    DataContext manager for managing ClickHouse client and Object lifecycle.

    This context manager:
    - Manages a ClickHouse client instance (automatically initialized on enter)
    - Tracks all Objects created within the context via weakref
    - Automatically deletes tables and marks Objects as stale on exit

    Example:
        >>> async with DataContext() as ctx:
        ...     obj = await ctx.create_object_from_value([1, 2, 3])
        ...     # Use obj...
        ... # Tables are automatically deleted here
    """

    def __init__(
        self,
        creds: ClickHouseCreds | None = None,
        engine: EngineType | None = None,
        lifecycle: LifecycleHandler | None = None,
    ):
        """Initialize a DataContext.

        Args:
            creds: ClickHouse credentials. If None, reads from environment variables.
            engine: ClickHouse table engine to use. Defaults to ENGINE_DEFAULT (MergeTree).
                   Use ENGINE_MEMORY for in-memory tables (faster, no disk I/O).
            lifecycle: LifecycleHandler for table refcounting. If None, creates a
                      LocalLifecycleHandler (current default behavior).
        """
        self._creds = creds or get_ch_creds()
        self._ch_client: Optional[AsyncClient] = None
        self._lifecycle: Optional[LifecycleHandler] = lifecycle
        self._owns_lifecycle: bool = lifecycle is None
        self._objects: Dict[int, weakref.ref] = {}  # Track objects for stale marking
        self._token = None
        self._engine: EngineType = engine if engine is not None else ENGINE_DEFAULT

    @property
    def engine(self) -> EngineType:
        """Get the default engine for this context."""
        return self._engine

    @property
    def lifecycle(self) -> Optional[LifecycleHandler]:
        """Get the lifecycle handler for this context."""
        return self._lifecycle

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
                "DataContext client not initialized. Use 'async with DataContext() as ctx:' to enter context."
            )
        return self._ch_client

    def incref(self, table_name: str) -> None:
        """Increment reference count for table. Thread-safe, non-blocking."""
        if self._lifecycle is not None:
            self._lifecycle.incref(table_name)

    def decref(self, table_name: str) -> None:
        """Decrement reference count for table. Thread-safe, non-blocking."""
        if self._lifecycle is not None:
            self._lifecycle.decref(table_name)

    def _register_object(self, obj: Object) -> None:
        """
        Register an Object to be tracked by this context for stale marking.

        Args:
            obj: Object instance to register
        """
        self._objects[id(obj)] = weakref.ref(obj)

    async def __aenter__(self):
        """Enter the context, initializing the client and starting the lifecycle handler."""
        if self._ch_client is None:
            self._ch_client = await get_ch_client(self._creds)

        if self._owns_lifecycle:
            self._lifecycle = LocalLifecycleHandler(self._creds)
            await self._lifecycle.start()

        self._token = _current_context.set(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context, stopping the lifecycle handler and resetting ContextVar.

        Owned lifecycle handlers will clean up all remaining tables on shutdown.
        """
        # Mark all tracked objects as stale
        for obj_ref in self._objects.values():
            obj = obj_ref()
            if obj is not None:
                obj._stale = True

        # Clear the tracking dict
        self._objects.clear()

        # Only stop lifecycle if we own it (local mode)
        if self._owns_lifecycle and self._lifecycle:
            await self._lifecycle.stop()
        self._lifecycle = None

        # Reset the ContextVar
        _current_context.reset(self._token)

        return False

    async def delete(self, obj: Object) -> None:
        """
        Delete an Object's table and mark it as stale.

        This removes the Object from tracking and cleans up its ClickHouse table.

        Args:
            obj: Object to delete

        Example:
            >>> async with DataContext() as ctx:
            ...     obj = await ctx.create_object_from_value([1, 2, 3])
            ...     result = await (obj + obj)
            ...     await ctx.delete(result)  # Clean up intermediate result
        """
        # Mark as stale
        obj._stale = True

        # Remove from tracking if present
        obj_id = id(obj)
        if obj_id in self._objects:
            del self._objects[obj_id]

        # Force decref to trigger cleanup
        if self._lifecycle is not None:
            self._lifecycle.decref(obj.table)


def get_engine_clause(engine: EngineType) -> str:
    """Get the ENGINE clause for table creation.

    Args:
        engine: ClickHouse engine type

    Returns:
        ENGINE clause string (e.g., "ENGINE = MergeTree ORDER BY tuple()")
    """
    if engine == "Memory":
        return "ENGINE = Memory"
    return "ENGINE = MergeTree ORDER BY tuple()"


async def create_object(schema: Schema, engine: EngineType | None = None):
    """
    Create a new Object with a ClickHouse table using the specified schema.

    This function creates Objects and their tables, automatically registering them
    with the current context for cleanup when the context exits.

    Args:
        schema: Schema dataclass with fieldtype and columns dict.
               Example: Schema(
                   fieldtype='a',
                   columns={"aai_id": "UInt64", "value": "Float64"}
               )
        engine: ClickHouse table engine. If None, uses context's engine setting.

    Returns:
        Object: New Object instance with created table

    Raises:
        RuntimeError: If no active context (must be called within 'async with DataContext()')

    Examples:
        >>> async with DataContext():
        ...     from aaiclick import Schema
        ...     schema = Schema(
        ...         fieldtype='a',
        ...         columns={"aai_id": "UInt64", "value": "Float64"}
        ...     )
        ...     obj = await create_object(schema)
    """
    from .object import Object

    ctx = get_data_context()

    # Create Object with schema (metadata built internally)
    obj = Object(schema=schema)

    # Build column definitions for CREATE TABLE
    column_defs = []
    for name, col_type in schema.columns.items():
        col_def = f"{name} {col_type}"
        if name == "aai_id":
            col_fieldtype = FIELDTYPE_SCALAR
        else:
            col_fieldtype = schema.fieldtype

        comment = ColumnMeta(fieldtype=col_fieldtype).to_yaml()
        if comment:
            col_def += f" COMMENT '{comment}'"
        column_defs.append(col_def)

    # Use provided engine or fall back to context's engine
    effective_engine = engine if engine is not None else ctx.engine

    # Create table with all columns and comments in single query
    engine_clause = get_engine_clause(effective_engine)
    create_query = f"""
    CREATE TABLE {obj.table} (
        {', '.join(column_defs)}
    ) {engine_clause}
    """
    obj._register(ctx)  # Write-ahead incref: register before CREATE TABLE
    ctx._register_object(obj)  # Object lifecycle: track for stale marking on exit
    await ctx.ch_client.command(create_query)
    return obj


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


SUPPORTED_URL_FORMATS = frozenset({
    "Parquet", "CSV", "CSVWithNames", "CSVWithNamesAndTypes",
    "TSV", "TSVWithNames", "TSVWithNamesAndTypes",
    "JSON", "JSONEachRow", "JSONCompactEachRow",
    "ORC", "Avro",
})

_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_url(url: str) -> None:
    """Validate URL is a proper HTTP(S) URL."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(
            f"URL must use http or https scheme, got '{parsed.scheme}'"
        )
    if not parsed.netloc:
        raise ValueError("URL must have a valid host")


def _validate_url_columns(columns: list[str]) -> None:
    """Validate column names are safe identifiers."""
    if not columns:
        raise ValueError("columns must be a non-empty list")
    for col in columns:
        if not _SAFE_IDENTIFIER_RE.match(col):
            raise ValueError(
                f"Column name '{col}' is not a valid identifier. "
                "Use only letters, digits, and underscores, starting with a letter or underscore."
            )
        if col == "aai_id":
            raise ValueError("'aai_id' is a reserved column name and cannot be used")


def _validate_url_format(fmt: str) -> None:
    """Validate format is a supported ClickHouse URL format."""
    if fmt not in SUPPORTED_URL_FORMATS:
        raise ValueError(
            f"Unsupported format '{fmt}'. "
            f"Supported formats: {sorted(SUPPORTED_URL_FORMATS)}"
        )


async def create_object_from_value(val: ValueType) -> Object:
    """
    Create a new Object from Python values with automatic schema inference.

    Internal function - use DataContext.create_object_from_value() instead.

    Args:
        val: Value to create object from. Can be:
            - Object or View: Returned directly without modification
            - Scalar (int, float, bool, str): Creates "aai_id" and "value" columns, single row
            - List of scalars: Creates "aai_id" and "value" columns with multiple rows
            - Dict of scalars: Creates "aai_id" plus one column per key, single row
            - Dict of arrays: Creates "aai_id" plus one column per key, multiple rows

    Returns:
        Object: New Object instance with data (or passed Object/View directly)

    Table Schema Details:
        - All tables include aai_id column with snowflake IDs
        - Scalars (single value): Single row with aai_id and value
        - Arrays (lists): Multiple rows with aai_id and value, ordered by aai_id
        - Dict of scalars: Single row with aai_id plus columns for each key
        - Dict of arrays: Multiple rows with aai_id plus columns for each key, ordered by aai_id
    """
    from .object import Object, View

    # Return Objects and Views directly without modification
    if isinstance(val, (Object, View)):
        return val

    ctx = get_data_context()

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
            obj = await create_object(schema)

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
            obj = await create_object(schema)

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
        obj = await create_object(schema)

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
        obj = await create_object(schema)

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


async def create_object_from_url(
    url: str,
    columns: list[str],
    format: str = "Parquet",
    where: str | None = None,
    limit: int | None = None,
) -> Object:
    """
    Create a new Object by loading data from an external URL using ClickHouse's url() table function.

    All data flows directly from the URL into ClickHouse - zero Python memory footprint.
    ClickHouse handles the HTTP request, parsing, and type inference natively.

    Args:
        url: HTTP(S) URL to load data from (e.g., Parquet file on S3, CSV on web server)
        columns: List of column names to select from the URL source
        format: ClickHouse format name. Default "Parquet".
            Supported: Parquet, CSV, CSVWithNames, TSV, TSVWithNames,
            JSON, JSONEachRow, ORC, Avro, etc.
        where: Optional SQL WHERE clause for filtering rows at load time
        limit: Optional row limit applied at load time

    Returns:
        Object: New Object with loaded data.
            - 1 column: column named "value"
            - Multiple columns: columns keep original names

    Raises:
        ValueError: If URL, columns, format, or limit are invalid
        RuntimeError: If no active DataContext
    """
    from .object import Object

    _validate_url(url)
    _validate_url_columns(columns)
    _validate_url_format(format)
    if limit is not None and (not isinstance(limit, int) or limit <= 0):
        raise ValueError(f"limit must be a positive integer, got {limit}")
    if where is not None and ";" in where:
        raise ValueError("WHERE clause must not contain ';'")

    ctx = get_data_context()

    # Escape single quotes in URL for safe SQL embedding
    safe_url = url.replace("'", "\\'")

    # Infer column types via DESCRIBE on the url() table function
    columns_str = ", ".join(columns)
    describe_query = (
        f"DESCRIBE (SELECT {columns_str} FROM url('{safe_url}', '{format}') LIMIT 0)"
    )
    describe_result = await ctx.ch_client.query(describe_query)

    ch_types: dict[str, str] = {}
    for row in describe_result.result_rows:
        ch_types[row[0]] = row[1]

    # Build schema
    if len(columns) == 1:
        schema = Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns={"aai_id": "UInt64", "value": ch_types[columns[0]]},
        )
        select_cols = f"{columns[0]} AS value"
    else:
        schema_columns: dict[str, str] = {"aai_id": "UInt64"}
        for col_name in columns:
            schema_columns[col_name] = ch_types[col_name]
        schema = Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns=schema_columns,
        )
        select_cols = columns_str

    # Create target table
    obj = await create_object(schema)

    # Insert data from URL with Snowflake ID generation
    base_id = get_snowflake_id()
    where_clause = f" WHERE {where}" if where else ""
    limit_clause = f" LIMIT {limit}" if limit is not None else ""

    insert_query = (
        f"INSERT INTO {obj.table} "
        f"SELECT toUInt64({base_id} + row_number() OVER ()) AS aai_id, {select_cols} "
        f"FROM url('{safe_url}', '{format}')"
        f"{where_clause}"
        f"{limit_clause}"
    )
    await ctx.ch_client.command(insert_query)

    return obj
