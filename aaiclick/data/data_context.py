"""
aaiclick.data.data_context - Function-based context management for ClickHouse client and Object lifecycle.

This module provides a context manager that manages the lifecycle of Objects created
within its scope, automatically cleaning up tables when the context exits.
"""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import AsyncIterator, Dict, List, Optional, Union
import weakref

import numpy as np
from clickhouse_connect import get_async_client
from clickhouse_connect.driver.asyncclient import AsyncClient
from urllib3 import PoolManager

from .env import get_ch_creds
from .lifecycle import LifecycleHandler, LocalLifecycleHandler
from .models import (
    ClickHouseCreds,
    ColumnDef,
    ValueScalarType,
    ValueListType,
    ValueType,
    Schema,
    ColumnMeta,
    FIELDTYPE_SCALAR,
    FIELDTYPE_ARRAY,
    EngineType,
    ENGINE_DEFAULT,
    parse_ch_type,
)
from .sql_utils import quote_identifier


@dataclass
class DataCtxState:
    """State bundle for a named data context."""

    ch_client: AsyncClient
    lifecycle: Optional[LifecycleHandler]
    owns_lifecycle: bool
    engine: EngineType
    creds: ClickHouseCreds
    objects: Dict[int, weakref.ref] = field(default_factory=dict)


# ContextVar holding dict[name -> DataCtxState]
_data_contexts: ContextVar[dict[str, DataCtxState]] = ContextVar('data_contexts')


def _get_data_state(ctx: str = "default") -> DataCtxState:
    """Get state bundle for a named context.

    Raises:
        RuntimeError: If no active context with that name.
    """
    try:
        contexts = _data_contexts.get()
    except LookupError:
        raise RuntimeError(
            f"No active data context '{ctx}' - use 'async with data_context()'"
        )
    if ctx not in contexts:
        raise RuntimeError(
            f"No active data context '{ctx}' - use 'async with data_context()'"
        )
    return contexts[ctx]


def get_ch_client(ctx: str = "default") -> AsyncClient:
    """Get the ClickHouse client from the active context."""
    return _get_data_state(ctx).ch_client


def get_engine(ctx: str = "default") -> EngineType:
    """Get the engine type from the active context."""
    return _get_data_state(ctx).engine


def incref(table_name: str, ctx: str = "default") -> None:
    """Increment reference count for table."""
    state = _get_data_state(ctx)
    if state.lifecycle is not None:
        state.lifecycle.incref(table_name)


def decref(table_name: str, ctx: str = "default") -> None:
    """Decrement reference count for table."""
    state = _get_data_state(ctx)
    if state.lifecycle is not None:
        state.lifecycle.decref(table_name)


def register_object(obj: object, ctx: str = "default") -> None:
    """Register an Object for stale marking on context exit."""
    state = _get_data_state(ctx)
    state.objects[id(obj)] = weakref.ref(obj)


async def delete_object(obj: object, ctx: str = "default") -> None:
    """Delete an Object's table and mark it as stale."""
    state = _get_data_state(ctx)
    obj._stale = True
    obj_id = id(obj)
    if obj_id in state.objects:
        del state.objects[obj_id]
    if state.lifecycle is not None:
        state.lifecycle.decref(obj.table)


# Global connection pool shared across all contexts
_pool: list = [None]


def get_pool() -> PoolManager:
    """Get or create the global urllib3 connection pool."""
    if _pool[0] is None:
        _pool[0] = PoolManager(num_pools=10, maxsize=10)
    return _pool[0]


async def _create_ch_client(creds: ClickHouseCreds | None = None) -> AsyncClient:
    """Create a ClickHouse client using the shared connection pool."""
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


@asynccontextmanager
async def data_context(
    ctx: str = "default",
    creds: ClickHouseCreds | None = None,
    engine: EngineType | None = None,
    lifecycle: LifecycleHandler | None = None,
) -> AsyncIterator[None]:
    """Async context manager for data operations.

    Args:
        ctx: Named context key (default "default").
        creds: ClickHouse credentials. If None, reads from environment.
        engine: ClickHouse table engine. Defaults to ENGINE_DEFAULT.
        lifecycle: LifecycleHandler for table refcounting.
                  If None, creates a LocalLifecycleHandler.
    """
    creds = creds or get_ch_creds()
    ch_client = await _create_ch_client(creds)
    owns_lifecycle = lifecycle is None
    effective_engine = engine if engine is not None else ENGINE_DEFAULT

    if owns_lifecycle:
        lifecycle = LocalLifecycleHandler(creds)
        await lifecycle.start()

    state = DataCtxState(
        ch_client=ch_client,
        lifecycle=lifecycle,
        owns_lifecycle=owns_lifecycle,
        engine=effective_engine,
        creds=creds,
    )

    # Copy-on-write: copy existing dict before mutation
    try:
        existing = _data_contexts.get()
    except LookupError:
        existing = {}
    contexts = dict(existing)
    contexts[ctx] = state
    token = _data_contexts.set(contexts)

    try:
        yield
    finally:
        # Mark all tracked objects as stale
        for obj_ref in state.objects.values():
            obj = obj_ref()
            if obj is not None:
                obj._stale = True
        state.objects.clear()

        # Stop lifecycle if we own it
        if owns_lifecycle and state.lifecycle:
            await state.lifecycle.stop()

        # Reset ContextVar
        _data_contexts.reset(token)


def get_engine_clause(engine: EngineType) -> str:
    """Get the ENGINE clause for table creation."""
    if engine == "Memory":
        return "ENGINE = Memory"
    return "ENGINE = MergeTree ORDER BY tuple()"


_VALID_NAME_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def _validate_persistent_name(name: str) -> None:
    """Validate a persistent object name.

    Raises:
        ValueError: If name doesn't match [a-zA-Z_][a-zA-Z0-9_]*
    """
    if not _VALID_NAME_RE.match(name):
        raise ValueError(
            f"Invalid persistent name '{name}': "
            f"must match [a-zA-Z_][a-zA-Z0-9_]*"
        )


async def create_object(
    schema: Schema,
    engine: EngineType | None = None,
    name: str | None = None,
):
    """Create a new Object with a ClickHouse table using the specified schema.

    Args:
        schema: Schema dataclass with fieldtype and columns dict.
        engine: ClickHouse table engine. If None, uses context's engine setting.
        name: Optional persistent name. When provided, creates a persistent
              table with prefix ``p_`` that survives context exit. Uses
              ``CREATE TABLE IF NOT EXISTS`` so subsequent calls with the same
              name append data. Forces MergeTree engine.

    Returns:
        Object: New Object instance with created table
    """
    from .object import Object

    state = _get_data_state()

    if name is not None:
        _validate_persistent_name(name)
        obj = Object(table=f"p_{name}", schema=schema)
    else:
        obj = Object(schema=schema)

    # Build column definitions for CREATE TABLE
    column_defs = []
    for col_name, col_def in schema.columns.items():
        if col_name == "aai_id" and col_def.nullable:
            raise ValueError("aai_id column cannot be nullable")

        ddl = f"{quote_identifier(col_name)} {col_def.ch_type()}"
        if col_name == "aai_id":
            ddl += " DEFAULT generateSnowflakeID()"
            col_fieldtype = FIELDTYPE_SCALAR
        else:
            col_fieldtype = schema.fieldtype

        comment = ColumnMeta(fieldtype=col_fieldtype).to_yaml()
        if comment:
            ddl += f" COMMENT '{comment}'"
        column_defs.append(ddl)

    if obj.persistent:
        effective_engine = "MergeTree"
    else:
        effective_engine = engine if engine is not None else state.engine

    engine_clause = get_engine_clause(effective_engine)

    create_or = "CREATE TABLE IF NOT EXISTS" if obj.persistent else "CREATE TABLE"
    create_query = f"""
    {create_or} {obj.table} (
        {', '.join(column_defs)}
    ) {engine_clause}
    """

    if not obj.persistent:
        obj._register()  # Write-ahead incref: register before CREATE TABLE
    else:
        obj._ctx = "default"
    register_object(obj)  # Object lifecycle: track for stale marking on exit
    await state.ch_client.command(create_query)
    return obj


def _infer_array_clickhouse_type(value: list) -> ColumnDef:
    """Infer Array(T) ClickHouse type from a Python list for use as an Array column."""
    element_def = _infer_clickhouse_type(value)
    return ColumnDef(f"Array({element_def.type})")


def _infer_clickhouse_type(value: Union[ValueScalarType, ValueListType]) -> ColumnDef:
    """Infer ClickHouse column type from Python value using numpy.

    Returns a ColumnDef with nullable=False. Nullable columns must be
    created explicitly via Schema with ColumnDef(type, nullable=True).
    """
    if isinstance(value, list):
        if not value:
            return ColumnDef("String")

        arr = np.array(value)
        dtype = arr.dtype

        if np.issubdtype(dtype, np.bool_):
            return ColumnDef("UInt8")
        elif np.issubdtype(dtype, np.integer):
            return ColumnDef("Int64")
        elif np.issubdtype(dtype, np.floating):
            return ColumnDef("Float64")
        else:
            return ColumnDef("String")

    if isinstance(value, bool):
        return ColumnDef("UInt8")
    elif isinstance(value, int):
        return ColumnDef("Int64")
    elif isinstance(value, float):
        return ColumnDef("Float64")
    elif isinstance(value, str):
        return ColumnDef("String")
    else:
        return ColumnDef("String")


async def create_object_from_value(
    val: ValueType,
    name: str | None = None,
) -> Object:
    """Create a new Object from Python values with automatic schema inference.

    Args:
        val: Value to create object from. Can be:
            - Object or View: Returned directly without modification
            - Scalar (int, float, bool, str): Creates single row
            - List of scalars: Creates multiple rows
            - Dict of scalars: Single row with columns per key
            - Dict of arrays: Multiple rows with columns per key
        name: Optional persistent name. When provided, creates a persistent
              table that survives context exit. If the table already exists,
              data is appended.

    Returns:
        Object: New Object instance with data
    """
    from .object import Object, View

    if isinstance(val, (Object, View)):
        return val

    ch = get_ch_client()

    if isinstance(val, dict):
        has_arrays = any(isinstance(v, list) for v in val.values())

        if has_arrays:
            columns = {"aai_id": ColumnDef("UInt64")}
            array_len = None

            for key, value in val.items():
                if isinstance(value, list):
                    if array_len is None:
                        array_len = len(value)
                    elif len(value) != array_len:
                        raise ValueError(
                            f"All arrays must have same length. "
                            f"Expected {array_len}, got {len(value)} for key '{key}'"
                        )
                    col_def = _infer_clickhouse_type(value)
                else:
                    raise ValueError(
                        f"Dict of arrays requires all values to be lists. "
                        f"Key '{key}' has type {type(value).__name__}"
                    )
                columns[key] = col_def

            schema = Schema(fieldtype=FIELDTYPE_ARRAY, columns=columns)
            obj = await create_object(schema, name=name)

            if array_len and array_len > 0:
                keys = list(val.keys())
                data = [list(row) for row in zip(*[val[key] for key in keys])]
                await ch.insert(obj.table, data, column_names=keys)

        else:
            columns = {"aai_id": ColumnDef("UInt64")}
            values = []

            for key, value in val.items():
                col_def = _infer_clickhouse_type(value)
                columns[key] = col_def

                if isinstance(value, str):
                    values.append(f"'{value}'")
                elif isinstance(value, bool):
                    values.append("1" if value else "0")
                else:
                    values.append(str(value))

            schema = Schema(fieldtype=FIELDTYPE_SCALAR, columns=columns)
            obj = await create_object(schema, name=name)

            col_names = [quote_identifier(k) for k in val.keys()]
            insert_query = f"INSERT INTO {obj.table} ({', '.join(col_names)}) VALUES ({', '.join(values)})"
            await ch.command(insert_query)

    elif isinstance(val, list):
        if val and isinstance(val[0], dict):
            # Records format: list of dicts with possible Array fields
            first_keys = set(val[0].keys())
            for i, record in enumerate(val[1:], 1):
                if set(record.keys()) != first_keys:
                    raise ValueError(
                        f"All records must have identical keys. "
                        f"Record 0 has {sorted(first_keys)}, "
                        f"record {i} has {sorted(record.keys())}"
                    )

            columns = {"aai_id": ColumnDef("UInt64")}
            keys = list(val[0].keys())
            for key in keys:
                sample = val[0][key]
                if isinstance(sample, list):
                    # Find a non-empty sample for better type inference
                    if not sample:
                        for record in val[1:]:
                            if isinstance(record[key], list) and record[key]:
                                sample = record[key]
                                break
                    columns[key] = _infer_array_clickhouse_type(sample)
                else:
                    columns[key] = _infer_clickhouse_type(sample)

            schema = Schema(fieldtype=FIELDTYPE_ARRAY, columns=columns)
            obj = await create_object(schema, name=name)

            data = [[record[key] for key in keys] for record in val]
            await ch.insert(obj.table, data, column_names=keys)
        else:
            col_def = _infer_clickhouse_type(val)
            schema = Schema(
                fieldtype=FIELDTYPE_ARRAY,
                columns={"aai_id": ColumnDef("UInt64"), "value": col_def},
            )
            obj = await create_object(schema, name=name)

            if val:
                data = [[v] for v in val]
                await ch.insert(obj.table, data, column_names=["value"])

    else:
        col_def = _infer_clickhouse_type(val)
        schema = Schema(
            fieldtype=FIELDTYPE_SCALAR,
            columns={"aai_id": ColumnDef("UInt64"), "value": col_def},
        )
        obj = await create_object(schema, name=name)

        if isinstance(val, str):
            value_str = f"'{val}'"
        elif isinstance(val, bool):
            value_str = "1" if val else "0"
        else:
            value_str = str(val)

        insert_query = f"INSERT INTO {obj.table} (value) VALUES ({value_str})"
        await ch.command(insert_query)

    return obj


async def open_object(name: str) -> Object:
    """Open an existing persistent Object by name.

    Args:
        name: Persistent name (without ``p_`` prefix).

    Returns:
        Object with schema loaded from ClickHouse.

    Raises:
        ValueError: If name is invalid.
        RuntimeError: If table does not exist.
    """
    from .object import Object
    from .ingest import _get_table_schema

    _validate_persistent_name(name)
    state = _get_data_state()
    table_name = f"p_{name}"

    result = await state.ch_client.command(f"EXISTS TABLE {table_name}")
    if not result:
        raise RuntimeError(
            f"Persistent object '{name}' does not exist "
            f"(table {table_name})"
        )

    fieldtype, columns = await _get_table_schema(table_name, state.ch_client)
    schema = Schema(fieldtype=fieldtype, columns=columns)
    obj = Object(table=table_name, schema=schema)
    obj._ctx = "default"
    register_object(obj)
    return obj


async def delete_persistent_object(name: str) -> None:
    """Drop a persistent table by name.

    Args:
        name: Persistent name (without ``p_`` prefix).

    Raises:
        ValueError: If name is invalid.
    """
    _validate_persistent_name(name)
    state = _get_data_state()
    table_name = f"p_{name}"
    await state.ch_client.command(f"DROP TABLE IF EXISTS {table_name}")


async def delete_persistent_objects(
    after: datetime | None = None,
    before: datetime | None = None,
) -> list[str]:
    """Drop persistent tables filtered by creation time.

    Uses ClickHouse ``system.tables.metadata_modification_time`` to
    determine when each table was created.

    Args:
        after: Drop tables created at or after this time (inclusive).
        before: Drop tables created before this time (exclusive).

    Returns:
        List of deleted persistent names (without ``p_`` prefix).

    Raises:
        ValueError: If neither ``after`` nor ``before`` is specified.
    """
    if after is None and before is None:
        raise ValueError(
            "At least one of 'after' or 'before' must be specified "
            "to prevent accidental deletion of all persistent objects"
        )
    state = _get_data_state()
    conditions = [
        f"database = '{state.creds.database}'",
        r"name LIKE 'p\_%'",
    ]
    if after is not None:
        after_str = after.strftime("%Y-%m-%d %H:%M:%S")
        conditions.append(f"metadata_modification_time >= '{after_str}'")
    if before is not None:
        before_str = before.strftime("%Y-%m-%d %H:%M:%S")
        conditions.append(f"metadata_modification_time < '{before_str}'")

    where = " AND ".join(conditions)
    result = await state.ch_client.query(
        f"SELECT name FROM system.tables WHERE {where}"
    )
    names = [row[0] for row in result.result_rows]

    for table_name in names:
        await state.ch_client.command(f"DROP TABLE IF EXISTS {table_name}")

    return [n[2:] for n in names]


async def list_persistent_objects() -> list[str]:
    """List all persistent object names.

    Returns:
        List of persistent names (without ``p_`` prefix).
    """
    state = _get_data_state()
    result = await state.ch_client.query(
        "SELECT name FROM system.tables "
        f"WHERE database = '{state.creds.database}' "
        r"AND name LIKE 'p\_%'"
    )
    return [row[0][2:] for row in result.result_rows]
