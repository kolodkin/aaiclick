"""
aaiclick.data.data_context - Function-based context management for ClickHouse client and Object lifecycle.

This module provides a context manager that manages the lifecycle of Objects created
within its scope, automatically cleaning up tables when the context exits.
"""

from __future__ import annotations

import re
import warnings
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import AsyncIterator, Dict, List, Optional, Union
import weakref

import numpy as np

from aaiclick.backend import get_ch_url, is_chdb, parse_ch_url

from .lifecycle import LifecycleHandler, LocalLifecycleHandler
from .models import (
    ColumnInfo,
    ValueScalarType,
    ValueListType,
    ValueType,
    Schema,
    ColumnMeta,
    FIELDTYPE_SCALAR,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    EngineType,
    ENGINE_DEFAULT,
    parse_ch_type,
)
from .sql_utils import quote_identifier

# clickhouse-connect (0.6.x–0.8.x) triggers FutureWarnings from numpy datetime
# internals during query result processing. Suppress globally so the filter covers
# all call sites (client creation, queries, inserts), not just client init.
# Remove once clickhouse-connect ships a release that no longer emits these warnings.
warnings.filterwarnings("ignore", category=FutureWarning, module=r"clickhouse_connect\.")


@dataclass
class DataCtxState:
    """State bundle for a named data context."""

    ch_client: object  # AsyncClient (distributed) or ChdbClient (local)
    lifecycle: Optional[LifecycleHandler]
    owns_lifecycle: bool
    engine: EngineType
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


def get_ch_client(ctx: str = "default") -> object:
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


# Global connection pool shared across all contexts (distributed mode only)
_pool: list = [None]


def get_pool():
    """Get or create the global urllib3 connection pool."""
    from urllib3 import PoolManager

    if _pool[0] is None:
        _pool[0] = PoolManager(num_pools=10, maxsize=10)
    return _pool[0]


async def _create_ch_client() -> object:
    """Create a ClickHouse client.

    chdb URL (chdb:///path): returns ChdbClient wrapping a chdb Session.
    clickhouse URL: returns clickhouse-connect AsyncClient.
    """
    if is_chdb():
        from .chdb_client import create_chdb_client, get_chdb_data_path

        return create_chdb_client(get_chdb_data_path())

    from clickhouse_connect import get_async_client

    params = parse_ch_url()
    return await get_async_client(
        pool_mgr=get_pool(),
        **params,
    )


@asynccontextmanager
async def data_context(
    ctx: str = "default",
    engine: EngineType | None = None,
    lifecycle: LifecycleHandler | None = None,
) -> AsyncIterator[None]:
    """Async context manager for data operations.

    Args:
        ctx: Named context key (default "default").
        engine: ClickHouse table engine. Defaults to ENGINE_DEFAULT.
        lifecycle: LifecycleHandler for table refcounting.
                  If None, creates a LocalLifecycleHandler.
    """
    ch_client = await _create_ch_client()

    owns_lifecycle = lifecycle is None
    effective_engine = engine if engine is not None else ENGINE_DEFAULT

    if owns_lifecycle:
        lifecycle = LocalLifecycleHandler(get_ch_url())
        await lifecycle.start()

    state = DataCtxState(
        ch_client=ch_client,
        lifecycle=lifecycle,
        owns_lifecycle=owns_lifecycle,
        engine=effective_engine,
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


def get_engine_clause(engine: EngineType, order_by: str = "tuple()") -> str:
    """Get the ENGINE clause for table creation."""
    if engine == "Memory":
        return "ENGINE = Memory"
    return f"ENGINE = {engine} ORDER BY {order_by}"


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
        schema: Schema dataclass with fieldtype, columns, engine, and order_by.
        engine: Deprecated — use schema.engine instead. If both are set,
                this parameter takes precedence for backward compatibility.
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
            col_fieldtype = schema.col_fieldtype or schema.fieldtype

        comment = ColumnMeta(fieldtype=col_fieldtype).to_yaml()
        if comment:
            ddl += f" COMMENT '{comment}'"
        column_defs.append(ddl)

    # Engine priority: persistent forces MergeTree > engine param > schema.engine > context default
    if obj.persistent:
        effective_engine = "MergeTree"
    else:
        effective_engine = engine or schema.engine or state.engine

    order_by = schema.order_by or "tuple()"
    engine_clause = get_engine_clause(effective_engine, order_by=order_by)

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


def _infer_array_clickhouse_type(value: list) -> ColumnInfo:
    """Infer Array(T) ClickHouse type from a Python list for use as an Array column."""
    element_def = _infer_clickhouse_type(value)
    return ColumnInfo(element_def.type, array=True)


def _is_list_of_dicts(value: object) -> bool:
    """Check if a value is a non-empty list of dicts (nested array-of-objects)."""
    return isinstance(value, list) and bool(value) and isinstance(value[0], dict)


def _has_nested_dicts(record: dict) -> bool:
    """Check if a dict contains any list-of-dicts values (nested structures)."""
    return any(_is_list_of_dicts(v) for v in record.values())


def _flatten_nested_schema(sample: dict, prefix: str = "", array_depth: int = 0) -> Dict[str, ColumnInfo]:
    """Recursively infer flat column schema from a nested record.

    Uses dot-star notation for nested array-of-objects levels.
    Each ``*`` level adds one Array() wrapper to the leaf column type.

    Args:
        sample: A sample record to infer schema from
        prefix: Column name prefix (e.g., ``"b.*."`` for nested fields)
        array_depth: Number of ``*`` nesting levels above this point

    Returns:
        Dict mapping flat column names to ColumnInfo
    """
    columns: Dict[str, ColumnInfo] = {}
    for key, val in sample.items():
        col_name = f"{prefix}{key}"

        if _is_list_of_dicts(val):
            sub_cols = _flatten_nested_schema(val[0], f"{col_name}.*.", array_depth + 1)
            columns.update(sub_cols)
        elif isinstance(val, list):
            col_info = _infer_array_clickhouse_type(val)
            columns[col_name] = ColumnInfo(
                col_info.type,
                array=int(col_info.array) + array_depth,
                low_cardinality=col_info.low_cardinality,
            )
        else:
            col_info = _infer_clickhouse_type(val)
            if array_depth:
                columns[col_name] = ColumnInfo(
                    col_info.type,
                    array=array_depth,
                    low_cardinality=col_info.low_cardinality,
                )
            else:
                columns[col_name] = col_info
    return columns


def _flatten_nested_record(record: dict, prefix: str = "") -> dict:
    """Flatten a single nested record into dot-star notation.

    Converts nested list-of-dicts into parallel arrays (one row per record).

    Args:
        record: A dict possibly containing list-of-dicts values
        prefix: Column name prefix

    Returns:
        Flat dict with dot-star column names and array values
    """
    result: dict = {}
    for key, val in record.items():
        col_name = f"{prefix}{key}"

        if _is_list_of_dicts(val):
            sub_records = [_flatten_nested_record(item, f"{col_name}.*.") for item in val]
            if sub_records:
                for sub_key in sub_records[0]:
                    result[sub_key] = [sr[sub_key] for sr in sub_records]
        else:
            result[col_name] = val
    return result


def _infer_clickhouse_type(value: Union[ValueScalarType, ValueListType]) -> ColumnInfo:
    """Infer ClickHouse column type from Python value using numpy.

    Returns a ColumnInfo with nullable=False. Nullable columns must be
    created explicitly via Schema with ColumnInfo(type, nullable=True).
    String types default to LowCardinality for better storage and query performance.
    Datetime types map to DateTime64(3, 'UTC') for millisecond-precision UTC storage.
    """
    if isinstance(value, list):
        if not value:
            return ColumnInfo("String", low_cardinality=True)

        if isinstance(value[0], datetime):
            return ColumnInfo("DateTime64(3, 'UTC')")

        arr = np.array(value)
        dtype = arr.dtype

        if np.issubdtype(dtype, np.bool_):
            return ColumnInfo("UInt8")
        elif np.issubdtype(dtype, np.integer):
            return ColumnInfo("Int64")
        elif np.issubdtype(dtype, np.floating):
            return ColumnInfo("Float64")
        else:
            return ColumnInfo("String", low_cardinality=True)

    if isinstance(value, bool):
        return ColumnInfo("UInt8")
    elif isinstance(value, datetime):
        return ColumnInfo("DateTime64(3, 'UTC')")
    elif isinstance(value, int):
        return ColumnInfo("Int64")
    elif isinstance(value, float):
        return ColumnInfo("Float64")
    elif isinstance(value, str):
        return ColumnInfo("String", low_cardinality=True)
    else:
        return ColumnInfo("String", low_cardinality=True)


def _find_non_empty_nested_sample(records: list, key: str) -> dict:
    """Find a non-empty sample for a nested list-of-dicts field across records.

    When the first record has an empty list for a nested field, searches
    subsequent records for a non-empty sample to infer schema from.
    """
    for record in records:
        val = record[key]
        if _is_list_of_dicts(val):
            return val[0]
    return {}


async def _create_nested_object(
    val: dict,
    ch: AsyncClient,
    name: str | None,
) -> Object:
    """Create an Object from a single dict with nested list-of-dicts values.

    Stores nested list-of-dicts as parallel Array columns using dot-star
    notation. For example:
    ``{a: 2, b: [{c: [1,2,3], d: 5}, {c: [4,5,6], d: 10}]}``
    becomes 1 row with columns ``a`` (Int64), ``b.*.c`` (Array(Array(Int64))),
    ``b.*.d`` (Array(Int64)).
    """
    flat = _flatten_nested_record(val)
    nested_cols = _flatten_nested_schema(val)

    columns = {"aai_id": ColumnInfo("UInt64")}
    columns.update(nested_cols)

    schema = Schema(fieldtype=FIELDTYPE_DICT, columns=columns, col_fieldtype=FIELDTYPE_SCALAR)
    obj = await create_object(schema, name=name)

    keys = list(flat.keys())
    data = [[flat[k] for k in keys]]
    await ch.insert(obj.table, data, column_names=keys)

    return obj


async def _create_nested_records_object(
    val: list,
    ch: AsyncClient,
    name: str | None,
) -> Object:
    """Create an Object from a list of dicts with nested list-of-dicts values.

    Each input record becomes one row. Nested list-of-dicts are stored as
    parallel Array columns using dot-star notation.
    """
    first_keys = set(val[0].keys())
    for i, record in enumerate(val[1:], 1):
        if set(record.keys()) != first_keys:
            raise ValueError(
                f"All records must have identical keys. "
                f"Record 0 has {sorted(first_keys)}, "
                f"record {i} has {sorted(record.keys())}"
            )

    # Infer schema from first record, using non-empty samples for nested fields
    sample = dict(val[0])
    for key in sample:
        if _is_list_of_dicts(sample[key]) and not sample[key]:
            found = _find_non_empty_nested_sample(val[1:], key)
            if found:
                sample[key] = [found]

    nested_cols = _flatten_nested_schema(sample)
    columns = {"aai_id": ColumnInfo("UInt64")}
    columns.update(nested_cols)

    schema = Schema(fieldtype=FIELDTYPE_DICT, columns=columns, col_fieldtype=FIELDTYPE_ARRAY)
    obj = await create_object(schema, name=name)

    all_flat = [_flatten_nested_record(record) for record in val]
    keys = list(all_flat[0].keys())
    data = [[flat[k] for k in keys] for flat in all_flat]
    await ch.insert(obj.table, data, column_names=keys)

    return obj


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
            - Dict/List with nested list-of-dicts: Flattened with dot-star notation
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
        if _has_nested_dicts(val):
            return await _create_nested_object(val, ch, name)

        has_arrays = any(isinstance(v, list) for v in val.values())

        if has_arrays:
            columns = {"aai_id": ColumnInfo("UInt64")}
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

            schema = Schema(fieldtype=FIELDTYPE_DICT, columns=columns, col_fieldtype=FIELDTYPE_ARRAY)
            obj = await create_object(schema, name=name)

            if array_len and array_len > 0:
                keys = list(val.keys())
                data = [list(row) for row in zip(*[val[key] for key in keys])]
                await ch.insert(obj.table, data, column_names=keys)

        else:
            columns = {"aai_id": ColumnInfo("UInt64")}
            values = []

            for key, value in val.items():
                col_def = _infer_clickhouse_type(value)
                columns[key] = col_def

                if isinstance(value, str):
                    values.append(f"'{value}'")
                elif isinstance(value, bool):
                    values.append("1" if value else "0")
                elif isinstance(value, datetime):
                    values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'")
                else:
                    values.append(str(value))

            schema = Schema(fieldtype=FIELDTYPE_DICT, columns=columns, col_fieldtype=FIELDTYPE_SCALAR)
            obj = await create_object(schema, name=name)

            col_names = [quote_identifier(k) for k in val.keys()]
            insert_query = f"INSERT INTO {obj.table} ({', '.join(col_names)}) VALUES ({', '.join(values)})"
            await ch.command(insert_query)

    elif isinstance(val, list):
        if val and isinstance(val[0], dict):
            if _has_nested_dicts(val[0]):
                return await _create_nested_records_object(val, ch, name)

            # Records format: list of dicts with possible Array fields
            first_keys = set(val[0].keys())
            for i, record in enumerate(val[1:], 1):
                if set(record.keys()) != first_keys:
                    raise ValueError(
                        f"All records must have identical keys. "
                        f"Record 0 has {sorted(first_keys)}, "
                        f"record {i} has {sorted(record.keys())}"
                    )

            columns = {"aai_id": ColumnInfo("UInt64")}
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

            schema = Schema(fieldtype=FIELDTYPE_DICT, columns=columns, col_fieldtype=FIELDTYPE_ARRAY)
            obj = await create_object(schema, name=name)

            data = [[record[key] for key in keys] for record in val]
            await ch.insert(obj.table, data, column_names=keys)
        else:
            col_def = _infer_clickhouse_type(val)
            schema = Schema(
                fieldtype=FIELDTYPE_ARRAY,
                columns={"aai_id": ColumnInfo("UInt64"), "value": col_def},
            )
            obj = await create_object(schema, name=name)

            if val:
                data = [[v] for v in val]
                await ch.insert(obj.table, data, column_names=["value"])

    else:
        col_def = _infer_clickhouse_type(val)
        schema = Schema(
            fieldtype=FIELDTYPE_SCALAR,
            columns={"aai_id": ColumnInfo("UInt64"), "value": col_def},
        )
        obj = await create_object(schema, name=name)

        if isinstance(val, str):
            value_str = f"'{val}'"
        elif isinstance(val, bool):
            value_str = "1" if val else "0"
        elif isinstance(val, datetime):
            value_str = f"'{val.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
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

    col_fieldtype, columns = await _get_table_schema(table_name, state.ch_client)
    is_dict_type = not (set(columns.keys()) <= {"aai_id", "value"})
    fieldtype = FIELDTYPE_DICT if is_dict_type else col_fieldtype
    schema = Schema(fieldtype=fieldtype, columns=columns, col_fieldtype=col_fieldtype)
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
        "database = currentDatabase()",
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
        "WHERE database = currentDatabase() "
        r"AND name LIKE 'p\_%'"
    )
    return [row[0][2:] for row in result.result_rows]
