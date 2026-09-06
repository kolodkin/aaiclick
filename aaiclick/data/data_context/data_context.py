"""
aaiclick.data.data_context - Function-based context management for ClickHouse client and Object lifecycle.

This module provides a context manager that manages the lifecycle of Objects created
within its scope, automatically cleaning up tables when the context exits.
"""

from __future__ import annotations

import re
import warnings
import weakref
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from sqlalchemy import delete as sql_delete
from sqlmodel import col, select

from aaiclick.locks import load_advisory_id, table_insert_lock
from aaiclick.oplog.oplog_api import oplog_record
from aaiclick.snowflake import get_snowflake_id
from aaiclick.tenancy import get_active_tenant_id

from ..models import (
    AAI_ID_COLUMN,
    AAI_ID_INFO,
    ENGINE_MEMORY,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    FIELDTYPE_SCALAR,
    ColumnInfo,
    EngineType,
    FieldSpec,
    Schema,
    ValueDictType,
    ValueListType,
    ValueScalarType,
    ValueType,
    build_order_by_clause,
)
from ..scope import (
    GLOBAL_PREFIX,
    SCOPE_GLOBAL,
    SCOPE_JOB,
    SCOPE_TEMP_NAMED,
    NamedScope,
    PersistentScope,
    make_scoped_table_name,
    name_from_table,
)
from ..sql_utils import naive_utc, quote_identifier
from .arrow_ingest import (
    arrow_table_for_insert,
    infer_struct_array,
    list_leaf_column_info,
    struct_array_to_columns,
    struct_type_to_columns,
)
from .ch_client import _ch_client_var, create_ch_client, get_ch_client
from .lifecycle import LocalLifecycleHandler, _lifecycle_var, get_data_lifecycle, register_table

if TYPE_CHECKING:
    from ..object import Object

# Per-resource ContextVars — each set by data_context() on entry, reset on exit.
# Resources owned by their respective modules:
#   ChClient        → ch_client.py  (_ch_client_var / get_ch_client)
#   LifecycleHandler→ lifecycle.py  (_lifecycle_var  / get_data_lifecycle)
#   Oplog           → oplog/collector.py (delegates to lifecycle handler)
_engine_var: ContextVar[EngineType] = ContextVar("engine", default=ENGINE_MEMORY)
_objects_var: ContextVar[dict[int, weakref.ref]] = ContextVar("objects")


def get_engine() -> EngineType:
    """Return the table engine for the active data context."""
    return _engine_var.get()


@asynccontextmanager
async def _maybe_insert_lock(table: str, name: str | None) -> AsyncIterator[None]:
    """Acquire the per-table insert lock when writing to a named (persistent)
    destination; no-op for fresh/temp destinations that cannot race."""
    if name is None:
        yield
        return
    async with table_insert_lock(await load_advisory_id(table)):
        yield


def incref(table_name: str) -> None:
    """Increment the reference count for a table, keeping it alive.

    Called automatically when a new Object is derived from an existing one
    (e.g. via operators or copy). Only needed when managing Object lifecycles
    manually outside of `data_context()`.

    Args:
        table_name: ClickHouse table name to retain.
    """
    lifecycle = get_data_lifecycle()
    if lifecycle is not None:
        lifecycle.incref(table_name)


def decref(table_name: str) -> None:
    """Decrement the reference count for a table, dropping it when it reaches zero.

    Called automatically when a derived Object is garbage collected or the
    enclosing `data_context()` exits. Only needed when managing Object
    lifecycles manually outside of `data_context()`.

    Args:
        table_name: ClickHouse table name to release.
    """
    lifecycle = get_data_lifecycle()
    if lifecycle is not None:
        lifecycle.decref(table_name)


def register_object(obj: Object) -> None:
    """Register an Object so it is marked stale when the enclosing context exits.

    Called automatically by `create_object()` and `create_object_from_value()`.
    There is no need to call this directly in normal usage.

    Args:
        obj: Object instance to track.
    """
    try:
        objects = _objects_var.get()
    except LookupError:
        return
    objects[id(obj)] = weakref.ref(obj)


async def delete_object(obj: Object) -> None:
    """Delete an Object's underlying ClickHouse table and mark the Object stale.

    After calling this, any further operations on `obj` will raise `RuntimeError`.
    Existing Python references to `obj` remain valid but unusable.

    Args:
        obj: Object instance to delete.
    """
    obj._stale = True
    obj._registered = False
    try:
        objects = _objects_var.get()
    except LookupError:
        objects = {}
    obj_id = id(obj)
    if obj_id in objects:
        del objects[obj_id]
    lifecycle = get_data_lifecycle()
    if lifecycle is not None:
        lifecycle.decref(obj.table)


@asynccontextmanager
async def data_context(
    engine: EngineType | None = None,
) -> AsyncIterator[None]:
    """Async context manager for standalone data operations.

    Sets per-resource ContextVars for the duration of the block:
    - ChClient (ch_client.py)
    - LocalLifecycleHandler (lifecycle.py)
    - EngineType and object registry (data_context.py)

    Always creates and owns a LocalLifecycleHandler. For orchestration job
    execution use orch_context() + task_scope() instead. ``scope="job"`` and
    ``open_object()`` require an orch_context — ``data_context()`` does not
    provide the SQL session that ``table_registry`` reads/writes need.
    ``scope="global"`` and ``scope="temp_named"`` work in either mode.

    Args:
        engine: ClickHouse table engine. Defaults to Memory (RAM).
    """
    ch_client = await create_ch_client()
    effective_engine = engine if engine is not None else ENGINE_MEMORY

    lifecycle = LocalLifecycleHandler(ch_client)
    await lifecycle.start()

    objects: dict[int, weakref.ref] = {}

    ch_token = _ch_client_var.set(ch_client)
    lc_token = _lifecycle_var.set(lifecycle)
    eng_token = _engine_var.set(effective_engine)
    obj_token = _objects_var.set(objects)

    try:
        yield
    finally:
        # Decref and stale-mark all tracked objects still alive.
        # Dead weakrefs (GC'd during context) were already decreffed by __del__.
        # Setting _stale prevents a later __del__ from double-decrefing.
        for obj_ref in objects.values():
            obj = obj_ref()
            if obj is not None:
                obj._stale = True
                if obj._registered and not obj.persistent:
                    if obj._owns_lifecycle_ref:
                        decref(obj.table)
                    obj._registered = False
        objects.clear()

        await lifecycle.stop()

        # Reset all ContextVars
        _objects_var.reset(obj_token)
        _engine_var.reset(eng_token)
        _lifecycle_var.reset(lc_token)
        _ch_client_var.reset(ch_token)

        await ch_client.close()


def get_engine_clause(engine: EngineType, order_by: str = "tuple()") -> str:
    """Get the ENGINE clause for table creation."""
    if engine == "Memory":
        return "ENGINE = Memory"
    return f"ENGINE = {engine} ORDER BY {order_by}"


_VALID_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Stays clear of ClickHouse's table-name ceiling after any scope prefix —
# see docs/designs/tenant_rbac.md, "Name length budget".
MAX_PERSISTENT_NAME_LEN = 128


def _validate_persistent_name(name: str) -> None:
    """Validate a persistent object name.

    Raises:
        ValueError: If name doesn't match [a-zA-Z_][a-zA-Z0-9_]* or exceeds
            ``MAX_PERSISTENT_NAME_LEN`` characters.
    """
    if not _VALID_NAME_RE.match(name):
        raise ValueError(f"Invalid persistent name '{name}': must match [a-zA-Z_][a-zA-Z0-9_]*")
    if len(name) > MAX_PERSISTENT_NAME_LEN:
        raise ValueError(
            f"Invalid persistent name: {len(name)} characters exceeds the {MAX_PERSISTENT_NAME_LEN}-character limit"
        )


def _resolve_scope(name: str | None, scope: NamedScope | None) -> NamedScope | None:
    """Resolve the effective scope for a named object.

    Rules:
    - ``name is None``: unnamed temp table; ``scope`` must also be ``None``.
    - ``name`` set, ``scope=None``: default to ``"temp_named"`` — a temp
      table tagged with the user-provided ``name`` for readability
      (``t_<name>_<snowflake_id>``). Works in or out of orch.
    - ``scope="job"`` / ``scope="global"``: persistent — require an active
      orch_context() (the SQL ``table_registry`` row is written only by the
      orch lifecycle handler). Bare ``data_context()`` is rejected.
    - ``scope="job"``: additionally requires an active orch job_id (raises
      ValueError otherwise — see scope.py).
    """
    if name is None:
        if scope is not None:
            raise ValueError("scope can only be set together with name")
        return None

    effective: NamedScope = scope if scope is not None else SCOPE_TEMP_NAMED

    if effective in (SCOPE_JOB, SCOPE_GLOBAL):
        lifecycle = get_data_lifecycle()
        if lifecycle is None or isinstance(lifecycle, LocalLifecycleHandler):
            raise RuntimeError(
                f"scope={effective!r} requires an active orch_context() — bare "
                "data_context() only supports temp / temp_named objects. "
                "Wrap your code in 'async with orch_context(): async with task_scope(...):' "
                "or omit scope= to get a 'temp_named' table."
            )

    return effective


def _build_scoped_table(name: str, scope: NamedScope) -> str:
    """Validate ``name`` and build the full CH table name for a scoped object."""
    _validate_persistent_name(name)
    if scope == SCOPE_TEMP_NAMED:
        return make_scoped_table_name(scope, name, snowid=get_snowflake_id())
    job_id: int | None = None
    if scope == SCOPE_JOB:
        lifecycle = get_data_lifecycle()
        job_id = lifecycle.current_job_id() if lifecycle is not None else None
    return make_scoped_table_name(scope, name, job_id=job_id, tenant_id=get_active_tenant_id())


async def create_object(
    schema: Schema,
    engine: EngineType | None = None,
    name: str | None = None,
    scope: NamedScope | None = None,
) -> Object:
    """Create a new Object with a ClickHouse table using the specified schema.

    Args:
        schema: Schema dataclass with fieldtype, columns, engine, and order_by.
        engine: Table engine override. Priority (highest to lowest):
                ``name`` (named) → this param → ``schema.engine`` → context default.
                Ignored when ``name`` is set — named tables always use MergeTree.
        name: Optional name. When set, ``scope`` selects the lifetime tier —
              see ``scope`` below. Defaults to ``"temp_named"`` when omitted.
        scope: Optional. Must be ``None`` when ``name`` is also ``None``.
              ``"temp_named"`` → ``t_<name>_<snowflake>`` (default; dies with
              the context, like an unnamed temp). ``"job"`` → ``j_<job_id>_<name>``
              (lives only as long as the active orch job; raises if no job is
              active). ``"global"`` → ``p_<tenant_id>_<name>`` (user-managed, removed only
              by ``delete_persistent_object()``).

    Returns:
        Object: New Object instance with created table
    """
    from ..object import Object

    effective_scope = _resolve_scope(name, scope)
    if effective_scope is not None:
        assert name is not None
        table_name = _build_scoped_table(name, effective_scope)
        obj = Object(table=table_name, schema=schema)
    else:
        obj = Object(schema=schema)

    # Fieldtype metadata for these columns lives in table_registry.schema_doc
    # (written by register_table below) rather than ClickHouse COMMENTs.
    def _column_ddl(col_name: str, col_def: ColumnInfo) -> str:
        ddl = f"{quote_identifier(col_name)} {col_def.ch_type()}"
        if col_def.default:
            ddl += f" DEFAULT {col_def.default}"
        return ddl

    column_defs = [_column_ddl(name, info) for name, info in schema.columns.items()]

    # Persistent tables always use MergeTree regardless of engine param or schema.engine.
    if obj.persistent:
        effective_engine = "MergeTree"
    else:
        effective_engine = engine or schema.engine or get_engine()

    if schema.order_by is not None:
        order_by = schema.order_by
    elif AAI_ID_COLUMN in schema.columns:
        order_by = f"({AAI_ID_COLUMN})"
    else:
        order_by = "tuple()"

    # Memory engine ignores ORDER BY — warn when both are combined.
    if effective_engine == "Memory" and schema.order_by:
        warnings.warn(
            f"order_by={schema.order_by!r} has no effect with Memory engine. "
            "Use engine='MergeTree' for ORDER BY support.",
            UserWarning,
            stacklevel=2,
        )
    engine_clause = get_engine_clause(effective_engine, order_by=order_by)

    create_or = "CREATE TABLE IF NOT EXISTS" if obj.persistent else "CREATE TABLE"
    create_query = f"""
    {create_or} {obj.table} (
        {", ".join(column_defs)}
    ) {engine_clause}
    """

    if not obj.persistent:
        obj._register()  # Write-ahead incref: register before CREATE TABLE
    register_object(obj)  # Object lifecycle: track for stale marking on exit
    await get_ch_client().command(create_query)

    # Register table in table_registry for cleanup worker.
    # In orch mode this records the job_id so all tables (including persistent)
    # are scoped to their job and cleaned up when the job expires. The
    # schema_doc carries the serialised Schema that _get_table_schema reads
    # back — replaces the per-column ClickHouse COMMENT YAML.
    # operation_log entries are recorded by higher-level callers (operators, ingest, etc.)
    register_table(obj.table, schema_doc=schema.model_dump_json())

    # Flush the lifecycle queue so the registry row is committed before the
    # caller reads schema_doc (e.g. via Object.data() → _get_table_schema).
    # The queue is async and order-sensitive for incref/decref, but registry
    # writes are standalone and idempotent; a synchronous flush here is
    # cheaper than forcing every read path to flush.
    lifecycle = get_data_lifecycle()
    if lifecycle is not None:
        await lifecycle.flush()

    return obj


def _is_list_of_dicts(value: object) -> bool:
    """Check if a value is a non-empty list of dicts (nested array-of-objects).

    Recurses through leading list levels so lists of lists of dicts
    (``[[{...}]]``) are also treated as nested structures.
    """
    if not (isinstance(value, list) and value):
        return False
    return isinstance(value[0], dict) or _is_list_of_dicts(value[0])


def _has_nested_dicts(record: dict) -> bool:
    """Check if a dict contains any dict or list-of-dicts values (nested structures)."""
    return any(isinstance(v, dict) or _is_list_of_dicts(v) for v in record.values())


def _infer_clickhouse_type(value: ValueScalarType | ValueListType) -> ColumnInfo:
    """Infer ClickHouse column type from Python value using pyarrow.

    Returns a ColumnInfo with nullable=False. Nullable columns must be
    created explicitly via Schema with ColumnInfo(type, nullable=True).
    String types default to LowCardinality for better storage and query performance.
    Datetime types map to DateTime64(3, 'UTC') for millisecond-precision UTC storage.
    """
    if isinstance(value, list):
        if not value:
            return ColumnInfo("String")

        arr = pa.array(value)
        pa_type = arr.type

        if pa.types.is_boolean(pa_type):
            return ColumnInfo("Bool")
        elif pa.types.is_timestamp(pa_type):
            return ColumnInfo("DateTime64(3, 'UTC')")
        elif pa.types.is_integer(pa_type):
            return ColumnInfo("Int64")
        elif pa.types.is_floating(pa_type):
            return ColumnInfo("Float64")
        else:
            return ColumnInfo("String")

    if isinstance(value, bool):
        return ColumnInfo("Bool")
    elif isinstance(value, datetime):
        return ColumnInfo("DateTime64(3, 'UTC')")
    elif isinstance(value, int):
        return ColumnInfo("Int64")
    elif isinstance(value, float):
        return ColumnInfo("Float64")
    elif isinstance(value, str):
        return ColumnInfo("String")
    else:
        return ColumnInfo("String")


def _apply_field_spec(col: ColumnInfo, spec: FieldSpec) -> ColumnInfo:
    """Apply a FieldSpec override to an inferred ColumnInfo."""
    return col.model_copy(
        update={
            "type": spec.type if spec.type is not None else col.type,
            "nullable": spec.nullable,
            "low_cardinality": spec.low_cardinality,
        }
    )


def _nullable_column_keys(columns: dict[str, ColumnInfo]) -> frozenset[str]:
    """Keys of nullable columns — exempt from ingest strictness."""
    return frozenset(name for name, col in columns.items() if col.nullable)


def _apply_field_specs(
    columns: dict[str, ColumnInfo],
    fields: dict[str, FieldSpec] | None,
) -> dict[str, ColumnInfo]:
    """Apply field specs to inferred columns, validating field names."""
    if not fields:
        return columns
    unknown = set(fields) - set(columns)
    if unknown:
        raise ValueError(f"fields references unknown columns: {sorted(unknown)}. Available columns: {sorted(columns)}")
    return {name: _apply_field_spec(col, fields[name]) if name in fields else col for name, col in columns.items()}


async def create_object_from_value(
    val: ValueType,
    name: str | None = None,
    order_by: list[str] | None = None,
    fields: dict[str, FieldSpec] | None = None,
    scope: NamedScope | None = None,
    *,
    aai_id: bool = False,
) -> Object:
    """Create a new Object from Python values with automatic schema inference.

    Args:
        val: Value to create object from. Can be:
            - Object or View: Returned directly without modification
            - Scalar (int, float, bool, str): Creates single row
            - List of scalars: Creates multiple rows
            - Dict of scalars: Single row with columns per key
            - Dict of arrays (all values lists): Multiple rows with columns per key
            - Dict mixing scalars and lists: Single row; lists become
              ``Array(T)`` columns
            - Dict/List with nested dicts: flattened to plain-dot columns
              (``{"x": {"y": 1}}`` → column ``x.y``)
            - Dict/List with nested list-of-dicts: flattened with dot-star
              notation (``{"b": [{"c": 1}]}`` → column ``b.*.c``)
            The schema is inferred by pyarrow across ALL records — keys must
            be identical in every record (missing keys raise ``ValueError``),
            except columns marked ``FieldSpec(nullable=True)``, whose missing
            or ``None`` leaf values ingest as NULL.

            Keys containing ``.`` and empty dict values raise ``ValueError``.
        name: Optional name. When set, ``scope`` selects the lifetime tier —
              see ``scope`` below. Defaults to ``"temp_named"`` when omitted.
        order_by: Optional list of column names for the table ORDER BY clause.
                  Empty input yields ``tuple()`` (ClickHouse no-sort form).
                  Example: ``order_by=['date']`` → ``ORDER BY (date)``
        fields: Optional per-column overrides for inferred types. Maps column
                names to ``FieldSpec`` instances that control nullable,
                low_cardinality, and type override.
                Example: ``fields={"name": FieldSpec(low_cardinality=True),
                "score": FieldSpec(nullable=True)}``
        scope: Optional. Must be ``None`` when ``name`` is also ``None``.
              ``"temp_named"`` → ``t_<name>_<snowflake>`` (default; dies with
              the context). ``"job"`` → ``j_<job_id>_<name>`` (lives only as
              long as the active orch job; raises when called from pure
              ``data_context()``). ``"global"`` → ``p_<tenant_id>_<name>`` (user-managed,
              removed only by ``delete_persistent_object()``).
        aai_id: When ``True``, add an ``aai_id`` column (``UInt64`` with
              ``DEFAULT generateSnowflakeID()``). Each row gets a unique,
              monotonically-increasing 64-bit Snowflake assigned per-row by
              ClickHouse at insert time. Use ``view(order_by="aai_id").data()``
              to recover insertion order — handy for cross-table arithmetic
              that needs pair-stable row ordering.

              When the LEFT operand of a binary operator carries ``aai_id``,
              the result table propagates the LEFT side's ``aai_id`` values,
              so ``(a + b).view(order_by="aai_id")`` recovers the original
              row order.

    Returns:
        Object: New Object instance with data
    """
    from ..object import Object, View

    if isinstance(val, (Object, View)):
        return val

    ch = get_ch_client()
    order_by_clause = build_order_by_clause(order_by) if order_by is not None else None

    def _maybe_add_aai_id(columns: dict[str, ColumnInfo]) -> dict[str, ColumnInfo]:
        if not aai_id:
            return columns
        if AAI_ID_COLUMN in columns:
            raise ValueError(f"aai_id=True conflicts with user column '{AAI_ID_COLUMN}'")
        return {**columns, AAI_ID_COLUMN: AAI_ID_INFO}

    async def _insert_columns(
        table: str, columns: dict[str, ColumnInfo], col_map: Mapping[str, pa.Array | list]
    ) -> None:
        arrow_table = arrow_table_for_insert(col_map, columns)
        if arrow_table.num_rows == 0:
            return
        async with _maybe_insert_lock(table, name):
            await ch.insert_arrow(table, arrow_table)

    if isinstance(val, dict):
        all_arrays = bool(val) and all(isinstance(v, list) for v in val.values())

        if all_arrays and not _has_nested_dicts(val):
            # Dict of parallel arrays: one row per element.
            columns = {}
            col_map: dict[str, pa.Array | list] = {}
            array_len = None

            for key, value in val.items():
                if array_len is None:
                    array_len = len(value)
                elif len(value) != array_len:
                    raise ValueError(
                        f"All arrays must have same length. Expected {array_len}, got {len(value)} for key '{key}'"
                    )
                if "." in key:
                    raise ValueError(f"Dict keys must not contain '.': {key!r}")
                try:
                    pa_arr = pa.array(value)
                except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
                    raise ValueError(f"Cannot infer a uniform schema from records: {e}") from e
                col_map[key] = pa_arr
                columns[key] = list_leaf_column_info(pa_arr.type, 0, key).with_fieldtype(FIELDTYPE_ARRAY)

            columns = _maybe_add_aai_id(_apply_field_specs(columns, fields))
            schema = Schema(fieldtype=FIELDTYPE_DICT, columns=columns, order_by=order_by_clause)
            obj = await create_object(schema, name=name, scope=scope)

            await _insert_columns(obj.table, columns, col_map)

        else:
            # Single record (flat, nested, or mixed scalar/list): arrow
            # infers the schema; leaves flatten to dot/dot-star columns.
            struct_arr = infer_struct_array([val])
            columns = struct_type_to_columns(struct_arr.type)
            columns = _maybe_add_aai_id(_apply_field_specs(columns, fields))
            col_map = struct_array_to_columns(struct_arr, _nullable_column_keys(columns))
            schema = Schema(fieldtype=FIELDTYPE_DICT, columns=columns, order_by=order_by_clause)
            obj = await create_object(schema, name=name, scope=scope)

            await _insert_columns(obj.table, columns, col_map)

    elif isinstance(val, list):
        if val and isinstance(val[0], dict):
            # Narrow: list-of-dicts (ValueRecordType). pyright can't infer this
            # from isinstance(val[0], dict) alone.
            records = cast("list[ValueDictType]", val)

            struct_arr = infer_struct_array(records)
            # List-of-records is dict-of-arrays — each column carries N values
            # across the input records, so fieldtype must be ARRAY.
            columns = {
                name_: ci.with_fieldtype(FIELDTYPE_ARRAY)
                for name_, ci in struct_type_to_columns(struct_arr.type).items()
            }
            columns = _maybe_add_aai_id(_apply_field_specs(columns, fields))
            col_map = struct_array_to_columns(struct_arr, _nullable_column_keys(columns))
            schema = Schema(fieldtype=FIELDTYPE_DICT, columns=columns, order_by=order_by_clause)
            obj = await create_object(schema, name=name, scope=scope)

            await _insert_columns(obj.table, columns, col_map)
        else:
            # Narrow: list of scalars (ValueListType).
            scalars = cast(ValueListType, val)
            col_def = _infer_clickhouse_type(scalars)
            columns = {"value": col_def.with_fieldtype(FIELDTYPE_ARRAY)}
            columns = _maybe_add_aai_id(_apply_field_specs(columns, fields))
            schema = Schema(
                fieldtype=FIELDTYPE_ARRAY,
                columns=columns,
                order_by=order_by_clause,
            )
            obj = await create_object(schema, name=name, scope=scope)

            await _insert_columns(obj.table, columns, {"value": scalars})

    else:
        col_def = _infer_clickhouse_type(val)
        columns = {"value": col_def}
        columns = _apply_field_specs(columns, fields)
        schema = Schema(
            fieldtype=FIELDTYPE_SCALAR,
            columns=columns,
            order_by=order_by_clause,
        )
        obj = await create_object(schema, name=name, scope=scope)

        await _insert_columns(obj.table, columns, {"value": [val]})

    oplog_record(obj.table, "create_from_value")
    return obj


class ObjectNotFoundError(RuntimeError):
    """No persistent object exists under the requested name and scope.

    A ``RuntimeError`` subclass so callers written against ``open_object``'s
    original contract keep working.
    """


async def open_object(name: str, scope: PersistentScope = SCOPE_JOB) -> Object:
    """Open an existing persistent Object by name.

    Args:
        name: Persistent name (without prefix).
        scope: Persistence tier the object was created with — ``"global"`` →
               looks up ``p_<tenant_id>_<name>``; ``"job"`` → looks up
               ``j_<job_id>_<name>`` using the active orch job. ``"temp_named"``
               is not openable — temp tables disappear with their context.

    Returns:
        Object with schema loaded from ClickHouse.

    Raises:
        ValueError: If name is invalid.
        ObjectNotFoundError: If the table does not exist.
    """
    from ..object import Object
    from ..object.ingest import _get_table_schema

    table_name = _build_scoped_table(name, scope)
    ch = get_ch_client()

    result = await ch.command(f"EXISTS TABLE {table_name}")
    if not result:
        raise ObjectNotFoundError(f"Persistent object '{name}' does not exist (table {table_name})")

    fieldtype, columns = await _get_table_schema(table_name, ch)
    schema = Schema(fieldtype=fieldtype, columns=columns)
    obj = Object(table=table_name, schema=schema)
    register_object(obj)
    return obj


async def delete_persistent_object(name: str, scope: PersistentScope = SCOPE_JOB) -> None:
    """Drop a persistent table by name.

    Args:
        name: Persistent name (without prefix).
        scope: Tier the object was created with — ``"global"`` drops
               ``p_<tenant_id>_<name>``; ``"job"`` drops ``j_<job_id>_<name>``.

    Raises:
        ValueError: If name is invalid.
    """
    table_name = _build_scoped_table(name, scope)
    await get_ch_client().command(f"DROP TABLE IF EXISTS {table_name}")
    await _forget_registry_rows([table_name])


async def _forget_registry_rows(table_names: list[str]) -> None:
    """Delete ``table_registry`` rows for dropped tables.

    Without this a re-created object would hit the registry's
    ``ON CONFLICT (table_name) DO NOTHING`` and keep a stale schema_doc,
    and registry-backed listing would keep showing the dropped object.
    """
    if not table_names:
        return
    # Circular dep: see list_persistent_tables.
    from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
    from aaiclick.orchestration.sql_context import get_sql_session

    async with get_sql_session() as session:
        await session.execute(sql_delete(TableRegistry).where(col(TableRegistry.table_name).in_(table_names)))
        await session.commit()


async def delete_persistent_objects(
    after: datetime | None = None,
    before: datetime | None = None,
) -> list[str]:
    """Drop the active tenant's persistent tables, filtered by creation time.

    Candidates come from ``table_registry`` (see ``list_persistent_tables``),
    so the purge cannot reach another tenant's tables and the time window is
    evaluated against the registry's ``created_at`` — not ClickHouse's
    ``metadata_modification_time``, which chdb reports as the epoch.

    Args:
        after: Drop tables created at or after this time (inclusive).
        before: Drop tables created before this time (exclusive).

    Returns:
        List of deleted persistent names (without prefix).

    Raises:
        ValueError: If neither ``after`` nor ``before`` is specified.
    """
    if after is None and before is None:
        raise ValueError(
            "At least one of 'after' or 'before' must be specified "
            "to prevent accidental deletion of all persistent objects"
        )
    ch = get_ch_client()
    names = await list_persistent_tables(after=after, before=before)
    for table_name in names:
        await ch.command(f"DROP TABLE IF EXISTS {table_name}")
    await _forget_registry_rows(names)
    return [name_from_table(n) for n in names]


async def list_persistent_tables(
    after: datetime | None = None,
    before: datetime | None = None,
) -> list[str]:
    """List the active tenant's persistent CH table names (``p_*``).

    Reads SQL ``table_registry`` rather than scanning ``system.tables``:
    ownership lives in SQL, and a ClickHouse scan cannot tell one tenant's
    tables from another's without re-parsing every prefix.

    Args:
        after: Only tables registered at or after this time (inclusive).
        before: Only tables registered before this time (exclusive).
    """
    # Circular dep: orchestration imports the data package at import time,
    # so the registry model and SQL session are resolved at call time
    # (same pattern as aaiclick/data/object/ingest.py::_get_table_schema).
    from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
    from aaiclick.orchestration.sql_context import get_sql_session

    predicates = [
        TableRegistry.tenant_id == get_active_tenant_id(),
        col(TableRegistry.table_name).startswith(GLOBAL_PREFIX, autoescape=True),
    ]
    if after is not None:
        predicates.append(col(TableRegistry.created_at) >= naive_utc(after))
    if before is not None:
        predicates.append(col(TableRegistry.created_at) < naive_utc(before))
    async with get_sql_session() as session:
        result = await session.execute(select(TableRegistry.table_name).where(*predicates))
    return [row[0] for row in result.all()]


async def list_persistent_objects() -> list[str]:
    """List the active tenant's persistent object names (without prefix)."""
    return [name_from_table(t) for t in await list_persistent_tables()]
