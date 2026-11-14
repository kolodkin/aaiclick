Operation Log (oplog)
---

**Implementation**: `aaiclick/oplog/`

Operation provenance captured inside ClickHouse, with zero AI dependencies:
every object created or transformed is recorded for lineage tracing and
debugging.

# Storage

## Databases

The `AAICLICK_CH_URL` database holds only user objects. The internal tables
(`operation_log`, `task_logs`, `schema_migrations`) live beside it in
`<data_db>_internal` (`default_internal` under chdb), so grants, retention
and backups can treat them apart. Every internal query names it through
`internal_table()` in `aaiclick/backend.py`; migration scripts write
`{internal_db}`, which the runner substitutes.

## operation_log (ClickHouse)

Append-only audit log. Fields: `id` (Snowflake), `result_table`, `operation`,
`kwargs` (`Map(String, String)`), `sql_template`, `task_id`, `job_id`,
`run_id`, `created_at` (`DateTime64(3, 'UTC')`, like `task_logs.created_at`
and `schema_migrations.applied_at`). ORDER BY `(result_table, created_at)`.
Deleted with the owning job when it expires (`AAICLICK_JOB_TTL_DAYS`).

`kwargs` names every input (e.g. `{"left": ..., "right": ...}` for binary
ops). Only `p_` / `j_` / `t_` values count as tables, so a join's `how` can
sit beside them. Values must be strings — a non-string makes ClickHouse
silently drop the row, so recording raises `TypeError` and callers encode
lists at the call site (`",".join(...)`).

## table_registry (SQL)

Ownership metadata for every ClickHouse data table: `table_name` (PK) →
`(job_id, task_id, run_id, created_at)`. Written once at creation through the
lifecycle handler's queue; the background worker deletes a row when it drops
the table, and on job expiry or orphan cleanup for TTL'd rows.

It once lived in ClickHouse as an append-only MergeTree table. It moved to
SQL because every consumer is a keyed lookup or an owner join during
background cleanup — not append-only audit.

## Initialization

`init_oplog_tables` brings the CH schema up to date through the migration
runner (`aaiclick/oplog/migrate.py`): local mode applies pending migrations
directly; distributed mode raises, asking for `aaiclick migrate upgrade`.
Task startup also copies any pre-existing CH `table_registry` rows into SQL
once and drops the CH table (`aaiclick/orchestration/oplog_backfill.py`;
no-op on fresh installs).

# Recording

**Implementation**: `aaiclick/oplog/oplog_api.py`

Recording delegates to the active lifecycle handler. Under a bare
`object_context()` (local mode) it is a no-op. Inside a task, the
orchestration handler enqueues each record on the same FIFO queue as
incref/decref, and the queue's consumer writes it straight to
`operation_log` — one best-effort row per message, no buffer. Table creation
enqueues the `table_registry` row the same way.

# Instrumentation Points

| Operation                           | `operation`              | `kwargs`                                                     |
|-------------------------------------|--------------------------|--------------------------------------------------------------|
| `create_object()`                   | (none — registry only)   | —                                                            |
| `create_object_from_value()`        | `"create_from_value"`    | `{}`                                                         |
| Binary operators                    | `"add"`, `"sub"`, etc.   | `{"left": ..., "right": ...}`; a literal operand is left out |
| Aggregations, `nunique`, `group_by` | `"sum"`, `"group_by"`, … | `{"source": ...}`                                            |
| `isin`                              | `"isin"`                 | `{"source": ..., "other": ...}`                              |
| `join`                              | `"join"`                 | `left`, `right`, `left_on`, `right_on`, `how`                |
| `concat`                            | `"concat"`               | `{"source_0": ..., "source_1": ..., ...}`                    |
| `insert`                            | `"insert"`               | `{"source": ..., "target": ...}`                             |
| `copy()`                            | `"copy"`                 | `{"source": ...}`                                            |

`create_object()` only registers the table, never records an operation, to
avoid double-counting — the higher-level function records the operation that
produced the table.

# Graph Queries

Graph traversal over `operation_log` by recursive CTE traces upstream
lineage; the MCP server exposes the graph to agents — see
[Lineage via MCP](lineage.md).

# Table Lifecycle & Cleanup

All cleanup is job-driven. The per-job `preservation_mode` gates what cleanup
does:

| Mode   | Cleanup behavior                                                |
|--------|-----------------------------------------------------------------|
| `NONE` | Drop unpinned tables as soon as refs fall to zero (default).    |
| `FULL` | Skip the drop entirely — tables live until the job TTL expires. |

Jobs completed more than `AAICLICK_JOB_TTL_DAYS` ago lose all their data: CH
tables, oplog entries, SQL metadata.

# Environment Variables

| Variable                             | Default | Description                                                     |
|--------------------------------------|---------|-----------------------------------------------------------------|
| `AAICLICK_JOB_TTL_DAYS`              | `90`    | Days after job completion before all job data is deleted.       |
| `AAICLICK_DEFAULT_PRESERVATION_MODE` | `NONE`  | Mode for jobs that don't pass one explicitly: `NONE` or `FULL`. |
