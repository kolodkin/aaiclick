ObjectContext
---

# Overview

The `ObjectContext` is the entry point for all data operations: it owns the
ClickHouse client, tracks Objects and manages table lifecycle.

```python
async with object_context():
    obj = await create_object_from_value([1, 2, 3])
    print(await obj.sum().data())  # 6
# obj is now stale — using it raises RuntimeError
```

As a decorator it wraps a whole async function (the parentheses are required —
`object_context` is a factory):

```python
@object_context()
async def main():
    obj = await create_object_from_value([1, 2, 3])
    print(await obj.sum().data())  # 6
```

# Deployment Modes

`AAICLICK_CH_URL` selects the ClickHouse backend — embedded chdb by default, a
ClickHouse server in distributed mode. Object operations work identically on
both. URLs and setup: [Deployment — Deployment Modes](deployment.md#deployment-modes).

# Object Lifecycle and Staleness

Objects become **stale** when the context exits; every async method checks first.

!!! warning "Create and consume Objects within the same `object_context()` block"
    Using a stale Object raises `RuntimeError`. Persistent objects
    (created with `name=`) survive — reopen via `open_object()`.

# Scopes: temp, temp_named, job, global

Every Object belongs to one of four scopes, identified by the table-name prefix:

| Scope        | Prefix                  | Lifetime                                          | Created by                                                  |
|--------------|-------------------------|---------------------------------------------------|-------------------------------------------------------------|
| `temp`       | `t_<id>`                | Dropped at `object_context()` / `task_scope` exit | Unnamed objects                                             |
| `temp_named` | `t_<name>_<id>`         | Dropped at `object_context()` / `task_scope` exit | `create_object_from_value(..., name="x")` (default)         |
| `job`        | `j_<job_id>_<name>`     | Dropped when the owning job's TTL expires         | `create_object_from_value(..., name="x", scope="job")`      |
| `global`     | `p_<name>`              | Forever; only `delete_persistent_object()` drops  | `create_object_from_value(..., name="x", scope="global")`   |

With `name` and no `scope=`, the scope is `"temp_named"` everywhere: the table
dies with the context, but `name` shows in `system.tables` for debugging. Two
callers passing `name="staging"` get two distinct tables — the snowflake
disambiguates.

Persistent scopes (`"job"`, `"global"`) require an active `orch_context()`:
they write a `table_registry` row that only the orch lifecycle maintains.

```python
# Bare object_context — name= produces a temp_named table.
async with object_context():
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

`p_*` tables are **exempt** from job-TTL cleanup, even for expired jobs.
`j_<id>_*` tables drop only when the job expires (`AAICLICK_JOB_TTL_DAYS`), not
by per-task refcount cleanup.

# Table Schema and Structure

An Object's table holds only the user-declared columns — no implicit ordering
column. Row order is opt-in: pass `data(order_by=...)` or wrap in
`View(order_by=...)`.

## Schema Patterns

| Data Type                    | Columns                | Rows     |
|------------------------------|------------------------|----------|
| Scalar                       | `value {type}`         | 1        |
| Array/List                   | `value {type}`         | N        |
| Dict of Scalars              | `col1`, `col2`, ...    | 1        |
| Dict of Arrays (all lists)   | `col1`, `col2`, ...    | N        |
| Dict Mixing Scalars + Lists  | `col1`, `col2`, ...    | 1        |

A dict routes to parallel arrays (one row per element) only when **all** values
are lists; a mixed dict is a single record whose lists become `Array(T)`
columns (`{"id": 1, "tags": ["a"]}` → `id` Int64, `tags` Array(String)).

Column names are backtick-quoted in generated SQL.

# Supported Data Types

`create_object_from_value()` maps `int` → `Int64`, `float` → `Float64`, `bool` →
`UInt8`, `str` → `LowCardinality(String)`, `datetime` → `DateTime64(3, 'UTC')`.

For explicit schemas use `ColumnInfo("DateTime64(3, 'UTC')")`. DateTime64 values
return as timezone-aware `datetime(tzinfo=utc)`.

# Field Specifications

`fields=` on `create_object_from_value()` maps column names to `FieldSpec`s,
overriding inferred properties without a full `Schema`:

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

# nullable=True also admits missing / None leaf values in records
obj = await create_object_from_value(
    [{"a": 1, "score": 0.5}, {"a": 2}],
    fields={"score": FieldSpec(nullable=True)},
)
await obj.data()  # {"a": [1, 2], "score": [0.5, None]}
```

| Attribute         | Default | Description                                                    |
|-------------------|---------|----------------------------------------------------------------|
| `nullable`        | `False` | Wrap column type in `Nullable()`                               |
| `low_cardinality` | `False` | Wrap column type in `LowCardinality()`                         |
| `type`            | `None`  | Override the inferred base type (`'Float32'` vs. `'Float64'`)  |

An unknown column name in `fields` raises `ValueError`.

# Table Lifecycle Tracking

Tables are reference-counted. On context exit, live Objects are decreffed and
marked stale. Locally a table drops at refcount 0; in distributed mode the
background worker does the cleanup. Full design:
[orchestration](https://github.com/kolodkin/aaiclick/blob/main/docs/designs/orchestration.md) — "Distributed Object Lifecycle".

# Preservation Modes

A `Job`'s `preservation_mode` controls which tables survive cleanup after it
completes; task execution is identical in both modes.

| Mode         | What survives after job              | Use case                       |
|--------------|--------------------------------------|--------------------------------|
| `NONE`       | Persistent tables only (default)     | Production runs                |
| `FULL`       | All tables until the job TTL expires | Development / debugging        |

Set the default with `AAICLICK_DEFAULT_PRESERVATION_MODE` (`NONE` or `FULL`);
override it per submission:

```python
from aaiclick.orchestration import PRESERVATION_FULL
from aaiclick.orchestration.registered_jobs import run_job

await run_job(
    "debug_run",
    "myapp.pipelines.crawl",
    preservation_mode=PRESERVATION_FULL,
)
```
