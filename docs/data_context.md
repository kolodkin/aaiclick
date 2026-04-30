# DataContext Documentation

## Overview

**Implementation**: `aaiclick/data/data_context.py` — see `data_context()`, `create_object()`, `create_object_from_value()`

The `DataContext` manages ClickHouse client lifecycle, Object tracking, and table lifecycle — the entry point for all data operations. Objects become stale when the context exits.

```python
async with data_context():
    obj = await create_object_from_value([1, 2, 3])
    result = await obj.sum()
    print(await result.data())  # 6
# obj and result are now stale — using them raises RuntimeError
```

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

`AAICLICK_CH_URL` selects the ClickHouse backend. Both satisfy the `ChClient` protocol, so all Object operations work identically.

=== "Local (default)"

    Embedded chdb — no server needed:

    ```bash
    export AAICLICK_CH_URL="chdb:///~/.aaiclick/chdb_data"
    export AAICLICK_SQL_URL="sqlite+aiosqlite:///~/.aaiclick/local.db"
    ```

=== "Distributed"

    Remote ClickHouse + PostgreSQL:

    ```bash
    export AAICLICK_CH_URL="clickhouse://user:pass@host:8123/db"
    export AAICLICK_SQL_URL="postgresql+asyncpg://user:pass@host:5432/db"
    ```

**Implementation**: `aaiclick/data/chdb_client.py` (local), `aaiclick/data/clickhouse_client.py` (distributed), `aaiclick/backend.py` (URL helpers)

chdb inserts use pyarrow's `Python()` table function. Type mapping: `_ch_type_to_pa()` in `chdb_client.py`.

See [Orchestration documentation](orchestration.md) — "Deployment Modes" for the full local/distributed comparison table.

## Object Lifecycle and Staleness

Objects become **stale** when the context exits — all async methods check via `self.checkstale()`.

!!! warning "Create and consume Objects within the same `data_context()` block"
    Using a stale Object raises `RuntimeError`. Persistent objects
    (created with `name=`) survive — reopen via `open_object()`.

## Scopes: temp, temp_named, job, global

Every Object belongs to one of four scopes, identified by the table-name prefix:

| Scope        | Prefix                  | Lifetime                                          | Created by                                                  |
|--------------|-------------------------|---------------------------------------------------|-------------------------------------------------------------|
| `temp`       | `t_<id>`                | Dropped at `data_context()` / `task_scope` exit   | Unnamed objects                                             |
| `temp_named` | `t_<name>_<id>`         | Dropped at `data_context()` / `task_scope` exit   | `create_object_from_value(..., name="x")` (default)         |
| `job`        | `j_<job_id>_<name>`     | Dropped when the owning job's TTL expires         | `create_object_from_value(..., name="x", scope="job")`      |
| `global`     | `p_<name>`              | Forever; only `delete_persistent_object()` drops  | `create_object_from_value(..., name="x", scope="global")`   |

**Default when `name` is set, no `scope=`**: `"temp_named"` everywhere — the table dies with the context, but the user-supplied `name` shows up in the table name (`t_<name>_<snowflake>`) for easier debugging in `system.tables`. Two callers passing `name="staging"` get two distinct tables — the snowflake disambiguates.

Persistent scopes (`"job"`, `"global"`) require an active `orch_context()` because they write a `table_registry` row that only the orch lifecycle handler maintains.

```python
# Bare data_context — name= produces a temp_named table.
async with data_context():
    staging = await create_object_from_value([1, 2, 3], name="staging")
    assert staging.table.startswith("t_staging_")
    assert staging.scope == "temp_named"
    assert staging.persistent is False

# Inside an orch task_scope — same default (temp_named).
async with task_scope(task_id=1, job_id=42, run_id=100):
    interim = await create_object_from_value([1, 2, 3], name="stage1")
    assert interim.scope == "temp_named"

    pinned = await create_object_from_value([1, 2, 3], name="stage1", scope="job")
    assert pinned.table == "j_42_stage1"

    shared = await create_object_from_value(
        [9, 9, 9],
        name="cross_job_catalog",
        scope="global",
    )
    assert shared.table == "p_cross_job_catalog"
```

`p_*` tables are **exempt** from job-TTL cleanup — the background worker never drops them, even for expired jobs. `j_<id>_*` tables are dropped only when the job itself expires (`AAICLICK_JOB_TTL_DAYS`), not by per-task refcount cleanup.

## Table Schema and Structure

Each Object gets a ClickHouse table `t{snowflake_id}` containing only the user-declared columns — no implicit ordering column. Row order is opt-in: callers pass `data(order_by=...)` or wrap in `View(order_by=...)` when they need it.

### Schema Patterns

| Data Type       | Columns                | Rows     |
|-----------------|------------------------|----------|
| Scalar          | `value {type}`         | 1        |
| Array/List      | `value {type}`         | N        |
| Dict of Scalars | `col1`, `col2`, ...    | 1        |
| Dict of Arrays  | `col1`, `col2`, ...    | N        |

Column names are backtick-quoted via `quote_identifier()` in `aaiclick/data/sql_utils.py`.

### Schema Storage

aaiclick stores per-table fieldtype + column metadata as a serialised `SchemaView` (Pydantic JSON) in the SQL `table_registry.schema_doc` column. `_get_table_schema(table)` reads that row to hydrate the in-memory `Schema` dataclass. There is no per-column ClickHouse `COMMENT` storage — DDL emits user columns only.

## Supported Data Types

**Auto-detected by `create_object_from_value()`**: Python `int` → `Int64`, `float` → `Float64`, `bool` → `UInt8`, `str` → `LowCardinality(String)`, `datetime` → `DateTime64(3, 'UTC')`.

**Datetime handling**: `create_object_from_value()` auto-detects `datetime` → `DateTime64(3, 'UTC')`. For explicit schemas, use `ColumnInfo("DateTime64(3, 'UTC')")`. All DateTime64 values are returned as timezone-aware `datetime(tzinfo=utc)`.

## Field Specifications

**Implementation**: `aaiclick/data/models.py` — see `FieldSpec`, `aaiclick/data/data_context.py` — see `_apply_field_specs()`

The `fields` parameter on `create_object_from_value()` overrides inferred column properties without requiring a full `Schema`. Pass a dict mapping column names to `FieldSpec` instances:

```python
from aaiclick import create_object_from_value, FieldSpec

# Make 'category' low-cardinality and 'score' nullable
obj = await create_object_from_value(
    {"category": ["a", "b", "a"], "score": [95.5, 88.0, 72.0]},
    fields={
        "category": FieldSpec(low_cardinality=True),
        "score": FieldSpec(nullable=True),
    },
)
obj.schema.columns["category"].ch_type()  # "LowCardinality(String)"
obj.schema.columns["score"].ch_type()     # "Nullable(Float64)"

# Override inferred type
obj = await create_object_from_value(
    {"price": [1, 2, 3]},
    fields={"price": FieldSpec(type="Float32")},
)

# Works with all input shapes: scalars, lists, dicts, records
obj = await create_object_from_value(
    [10, 20, 30],
    fields={"value": FieldSpec(nullable=True)},
)
```

| Attribute        | Default | Description                                                         |
|------------------|---------|---------------------------------------------------------------------|
| `nullable`       | `False` | Wrap column type in `Nullable()`                                    |
| `low_cardinality`| `False` | Wrap column type in `LowCardinality()`                              |
| `type`           | `None`  | Override the inferred base type (e.g., `'Float32'` instead of `'Float64'`) |

Passing an unknown column name in `fields` raises `ValueError`.

**Tests**: `aaiclick/data/object/test_field_spec.py`

## Column Metadata

Each column gets a YAML comment with fieldtype: `'s'` (scalar), `'a'` (array), `'d'` (dict).

## Table Lifecycle Tracking

Tables are reference-counted and dropped when unreferenced. On context exit, live objects are decreffed and stale-marked. `LocalLifecycleHandler` drops tables on refcount 0; in distributed mode, `OrchLifecycleHandler` defers cleanup to `BackgroundWorker`.

See [Orchestration documentation](orchestration.md) — "Distributed Object Lifecycle" for the full design.

## Preservation Modes

**Implementation**: `aaiclick/orchestration/models.py` — see `PreservationMode`

Each `Job` carries a `preservation_mode` that controls which tables survive cleanup after the job completes. Task execution is identical in both modes — only the cleanup step differs.

| Mode         | What survives after job              | Use case                       |
|--------------|--------------------------------------|--------------------------------|
| `NONE`       | Persistent tables only (default)     | Production runs (as today)     |
| `FULL`       | All tables until the job TTL expires | Development / debugging        |

The default can be set globally via `AAICLICK_DEFAULT_PRESERVATION_MODE` (values: `NONE`, `FULL`) and overridden per submission:

```python
from aaiclick.orchestration import PreservationMode
from aaiclick.orchestration.registered_jobs import run_job

await run_job(
    "debug_run",
    "myapp.pipelines.etl",
    preservation_mode=PreservationMode.FULL,
)
```
