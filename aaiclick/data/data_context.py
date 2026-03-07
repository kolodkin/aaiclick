"""
aaiclick.data.data_context - Function-based context management for ClickHouse client and Object lifecycle.

This module provides a context manager that manages the lifecycle of Objects created
within its scope, automatically cleaning up tables when the context exits.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
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
from .sql_utils import quote_identifier, values_to_select, insert_with_ids


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


async def create_object(schema: Schema, engine: EngineType | None = None):
    """Create a new Object with a ClickHouse table using the specified schema.

    Args:
        schema: Schema dataclass with fieldtype and columns dict.
        engine: ClickHouse table engine. If None, uses context's engine setting.

    Returns:
        Object: New Object instance with created table
    """
    from .object import Object

    state = _get_data_state()

    obj = Object(schema=schema)

    # Build column definitions for CREATE TABLE
    column_defs = []
    for name, col_type in schema.columns.items():
        col_def = f"{quote_identifier(name)} {col_type}"
        if name == "aai_id":
            col_fieldtype = FIELDTYPE_SCALAR
        else:
            col_fieldtype = schema.fieldtype

        comment = ColumnMeta(fieldtype=col_fieldtype).to_yaml()
        if comment:
            col_def += f" COMMENT '{comment}'"
        column_defs.append(col_def)

    effective_engine = engine if engine is not None else state.engine

    engine_clause = get_engine_clause(effective_engine)
    create_query = f"""
    CREATE TABLE {obj.table} (
        {', '.join(column_defs)}
    ) {engine_clause}
    """
    obj._register()  # Write-ahead incref: register before CREATE TABLE
    register_object(obj)  # Object lifecycle: track for stale marking on exit
    await state.ch_client.command(create_query)
    return obj


def _infer_clickhouse_type(value: Union[ValueScalarType, ValueListType]) -> str:
    """Infer ClickHouse column type from Python value using numpy."""
    if isinstance(value, list):
        if not value:
            return "String"

        arr = np.array(value)
        dtype = arr.dtype

        if np.issubdtype(dtype, np.bool_):
            return "UInt8"
        elif np.issubdtype(dtype, np.integer):
            return "Int64"
        elif np.issubdtype(dtype, np.floating):
            return "Float64"
        else:
            return "String"

    if isinstance(value, bool):
        return "UInt8"
    elif isinstance(value, int):
        return "Int64"
    elif isinstance(value, float):
        return "Float64"
    elif isinstance(value, str):
        return "String"
    else:
        return "String"


async def create_object_from_value(val: ValueType) -> Object:
    """Create a new Object from Python values with automatic schema inference.

    Args:
        val: Value to create object from. Can be:
            - Object or View: Returned directly without modification
            - Scalar (int, float, bool, str): Creates single row
            - List of scalars: Creates multiple rows
            - Dict of scalars: Single row with columns per key
            - Dict of arrays: Multiple rows with columns per key

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
            columns = {"aai_id": "UInt64"}
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
                    col_type = _infer_clickhouse_type(value)
                else:
                    raise ValueError(
                        f"Dict of arrays requires all values to be lists. "
                        f"Key '{key}' has type {type(value).__name__}"
                    )
                columns[key] = col_type

            schema = Schema(fieldtype=FIELDTYPE_ARRAY, columns=columns)
            row_count = array_len or 0

        else:
            columns = {"aai_id": "UInt64"}
            for key, value in val.items():
                col_type = _infer_clickhouse_type(value)
                columns[key] = col_type

            schema = Schema(fieldtype=FIELDTYPE_SCALAR, columns=columns)
            row_count = 1

    elif isinstance(val, list):
        col_type = _infer_clickhouse_type(val)
        schema = Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns={"aai_id": "UInt64", "value": col_type},
        )
        row_count = len(val)

    else:
        col_type = _infer_clickhouse_type(val)
        schema = Schema(
            fieldtype=FIELDTYPE_SCALAR,
            columns={"aai_id": "UInt64", "value": col_type},
        )
        row_count = 1

    obj = await create_object(schema)
    select_query = values_to_select(val)
    if select_query:
        await insert_with_ids(ch, obj.table, "*", f"FROM ({select_query})", count=row_count)

    return obj
