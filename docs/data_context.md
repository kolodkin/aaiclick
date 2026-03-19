# DataContext Documentation

## Overview

**Implementation**: `aaiclick/data/data_context.py` — see `data_context()`, `create_object()`, `create_object_from_value()`

The `DataContext` manages the ClickHouse client lifecycle, Object tracking, and table lifecycle. It is the entry point for all data operations — Objects are created within a context and become stale when it exits.

```python
async with data_context():
    obj = await create_object_from_value([1, 2, 3])
    result = await obj.sum()
    print(await result.data())  # 6
# obj and result are now stale — using them raises RuntimeError
```

**Key responsibilities:**
- ClickHouse client creation (chdb or remote, via `ChClient` protocol)
- Object tracking via weakref dict — marks all Objects stale on context exit
- Lifecycle/refcounting (`incref`/`decref`) for automatic table cleanup
- Accessed via `async with data_context():` or `get_data_context()` for the current context

## Managed Resources

`data_context()` owns and sets five per-resource ContextVars for the duration of the block.
Each resource lives in its own module so it can be accessed without importing `data_context`:

| Resource             | Type                          | ContextVar          | Module               | Accessor              |
|----------------------|-------------------------------|---------------------|----------------------|-----------------------|
| ClickHouse client    | `ChClient`                    | `_ch_client_var`    | `ch_client.py`       | `get_ch_client()`     |
| Table lifecycle      | `LifecycleHandler \| None`    | `_lifecycle_var`    | `lifecycle.py`       | `get_data_lifecycle()`|
| Table engine         | `EngineType`                  | `_engine_var`       | `data_context.py`    | `get_engine()`        |
| Object registry      | `dict[int, weakref.ref]`      | `_objects_var`      | `data_context.py`    | internal              |
| Oplog collector      | `OplogCollector \| None`      | `_oplog_collector`  | `oplog/collector.py` | `get_oplog_collector()`|

Each ContextVar is reset (via token) on context exit, so nested `data_context()` calls are safe.

## Deployment Modes

`AAICLICK_CH_URL` selects the ClickHouse backend: `chdb:///path` (embedded, default) or `clickhouse://user:pass@host:8123/db` (remote server). Both satisfy the `ChClient` protocol (`aaiclick/data/ch_client.py`), so all Object operations work identically. `create_ch_client()` dispatches based on `is_chdb()`.

**Implementation**: `aaiclick/data/chdb_client.py` (local), `aaiclick/data/clickhouse_client.py` (distributed), `aaiclick/backend.py` (URL helpers)

See [Orchestration documentation](orchestration.md) — "Deployment Modes" for the full local/distributed comparison table.

## Object Lifecycle and Staleness

Objects are managed by a `data_context()` and become **stale** when the context exits. All async methods call `self.checkstale()` — using a stale Object raises `RuntimeError`.

**Implementation**: `aaiclick/data/object.py` — see `checkstale()`, `stale` property, `_register()`

**Rules**: Create and use Objects within the same `data_context()`. Don't store Objects for use after context exit. Don't pass Objects between contexts.

## Table Schema and Structure

Each Object gets a dedicated ClickHouse table named `t{snowflake_id}`. All tables include an `aai_id` column (Snowflake ID) for ordering — ClickHouse doesn't guarantee insertion order in SELECT.

**Implementation**: `aaiclick/data/data_context.py` — see `create_object()` and `create_object_from_value()`

### Schema Patterns

| Data Type       | Columns                              | Rows     |
|-----------------|--------------------------------------|----------|
| Scalar          | `aai_id UInt64`, `value {type}`      | 1        |
| Array/List      | `aai_id UInt64`, `value {type}`      | N        |
| Dict of Scalars | `aai_id UInt64`, `col1`, `col2`, ... | 1        |
| Dict of Arrays  | `aai_id UInt64`, `col1`, `col2`, ... | N        |

Column names are backtick-quoted via `quote_identifier()` in `aaiclick/data/sql_utils.py`.

### Snowflake ID Structure

64-bit IDs: sign (1) + timestamp ms (41) + machine ID (10) + sequence (12). See `aaiclick/snowflake_id.py`.

## Supported Data Types

**Auto-detected by `create_object_from_value()`**: Python `int` → `Int64`, `float` → `Float64`, `bool` → `UInt8`, `str` → `LowCardinality(String)`, `datetime` → `DateTime64(3, 'UTC')`.

**Datetime handling**: `create_object_from_value()` auto-detects `datetime` objects (scalars, lists, dicts, records) and maps them to `DateTime64(3, 'UTC')`. For other APIs where data comes from strings (e.g., `create_object()` with explicit schema), use `ColumnInfo("DateTime64(3, 'UTC')")` — otherwise String is the default type.

**Data extraction**: All `DateTime64` UTC values are returned as timezone-aware `datetime` objects with `tzinfo=timezone.utc`.

## Column Metadata

**Implementation**: `aaiclick/data/object.py` — see `FIELDTYPE_SCALAR`, `FIELDTYPE_ARRAY`, `FIELDTYPE_DICT`

Each column gets a YAML comment with fieldtype: `'s'` (scalar), `'a'` (array), `'d'` (dict).

## Loading Data from URLs

**Implementation**: `aaiclick/data/url.py` — see `create_object_from_url()`

Loads data from HTTP URLs directly into ClickHouse using the `url()` table function — **zero Python memory footprint**.

**Parameters**: `url` (required), `columns` (required), `format` (default `"Parquet"`), `where`, `limit`.

**Supported formats**: Parquet, CSV, CSVWithNames, CSVWithNamesAndTypes, TSV, TSVWithNames, TSVWithNamesAndTypes, JSON, JSONEachRow, JSONCompactEachRow, ORC, Avro.

**Schema behavior**: Single column renamed to `value`; multiple columns preserve names; types inferred by ClickHouse.

**Snowflake IDs**: Generated as `base_id + row_number() OVER ()` since data never enters Python.

**Validation**: HTTP(S) only, `aai_id` reserved, no `;` in WHERE, format must be in supported list.

## Table Lifecycle Tracking

Tables are tracked via reference counting and dropped when no Objects reference them.

```
┌─────────────────────────────────────────────────────────────┐
│                    data_context()                             │
│                                                              │
│  enter  ──► Start LifecycleHandler                           │
│  exit   ──► Stop LifecycleHandler, cleanup                   │
│                                                              │
│  ┌──────────┐    incref()   ┌─────────────────────────────┐  │
│  │  Object  │──────────────►│                             │  │
│  │  __init__│               │    LifecycleHandler (ABC)   │  │
│  └──────────┘               │                             │  │
│                              │  Implementations:           │  │
│  ┌──────────┐    decref()   │  - LocalLifecycleHandler    │  │
│  │  Object  │──────────────►│    (TableWorker thread)     │  │
│  │  __del__ │               │  - PgLifecycleHandler       │  │
│  └──────────┘               │    (PostgreSQL refcounts)   │  │
│                              └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### LifecycleHandler ABC

**Implementation**: `aaiclick/data/lifecycle.py` — see `LifecycleHandler` class

Abstract interface: `start()`, `stop()`, `incref()`, `decref()`, `pin()` (no-op default), `claim()` (raises NotImplementedError default).

### Local Mode (chdb or ClickHouse server, no orchestration)

**Implementation**: `aaiclick/data/lifecycle.py` — see `LocalLifecycleHandler` class

Default handler when no lifecycle is injected. Wraps `TableWorker` (background thread in `aaiclick/data/table_worker.py`). Drops tables immediately on refcount 0. Works with both chdb and remote ClickHouse — the handler only needs a connection URL, not a specific backend.

Used when: running standalone scripts, interactive sessions, or tests without the orchestration layer.

### Distributed Mode (ClickHouse server + PostgreSQL)

**Implementation**: `aaiclick/orchestration/pg_lifecycle.py` — see `PgLifecycleHandler` class

Writes refcounts to PostgreSQL. Implements pin/claim for ownership transfer across workers. Does NOT drop tables — cleanup by `PgCleanupWorker`.

Used when: orchestration workers execute tasks across multiple processes/machines. The worker injects `PgLifecycleHandler` into `data_context()`.

See [Orchestration documentation](orchestration.md) — "Distributed Object Lifecycle" for the full design.

### Object Registration Flow

1. `create_object()` generates table name, calls `obj._register(ctx)` → `incref` (write-ahead, before CREATE TABLE)
2. `CREATE TABLE` in ClickHouse
3. On garbage collection, `Object.__del__` → `decref`
4. Views incref/decref the source Object's table (no new tables)

### __del__ Guard Clauses

| Guard                               | Scenario                                      |
|-------------------------------------|-----------------------------------------------|
| `sys.is_finalizing()`               | Interpreter shutdown — skip for thread safety  |
| `_ctx is None`                      | Object was never registered                    |
| `table.startswith("p_")`            | Persistent object — skip cleanup               |
