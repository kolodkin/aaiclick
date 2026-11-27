Data Model
---

The shared SQL and ClickHouse data model, table naming and row order.

# DB Models

**Implementation**: `aaiclick/orchestration/models.py`

<!-- req: db-models -->
All entities use **Snowflake IDs** from ClickHouse [`generateSnowflakeID()`](https://clickhouse.com/docs/sql-reference/functions/uuid-functions#generateSnowflakeID) — distributed, time-ordered, no DB round-trip — stored as `BIGINT`.

## Status Enums

| Type                    | Values                                                                                                              |
|-------------------------|---------------------------------------------------------------------------------------------------------------------|
| `JobStatus`             | PENDING, RUNNING, COMPLETED, FAILED, CANCELLED                                                                      |
| `TaskStatus`            | PENDING, RUNNING, COMPLETED, FAILED, CANCELLED, PENDING_FAILURE_CLEANUP, PENDING_CANCELLED_CLEANUP, UPSTREAM_FAILED |
| `ExecutionWorkerStatus` | ACTIVE, STOPPING, STOPPED                                                                                           |
| `RunType`               | SCHEDULED, MANUAL, SANDBOX                                                                                          |

Three `TaskStatus` values are set by the background sweep, not the worker:

- `PENDING_FAILURE_CLEANUP` — transient; a failed or dead-worker run awaiting ref cleanup before it retries to PENDING or settles to FAILED.
- `PENDING_CANCELLED_CLEANUP` — transient; set by `cancel_job` and the fail-fast group-sibling abort. The worker kills the run; the sweep drops the attempt's refs and settles the task to CANCELLED once no worker owns it (`execution_worker_id` is NULL — released on report-back or by the expired-claim sweep).
- `UPSTREAM_FAILED` — terminal; a pending task whose transitive upstream failed or was cancelled.

## Entities

- **RegisteredJob** — job catalog entry (unique `name`, cron `schedule`, defaults, `next_run_at`); see [Registered Jobs](../designs/orchestration.md#registered-jobs).
- **Job** — one workflow run, optionally linked to its `RegisteredJob`.
- **Task** — single executable unit. `run_epoch` is a fencing token bumped on every reset or reclaim.
- **Group** — logical task grouping, nestable via `parent_group_id`.
- **Dependency** — composite PK `(previous_id, previous_type, next_id, next_type)`; each side is `'task'` or `'group'`, so all four combinations are supported.
- **ExecutionWorker** — a worker process with heartbeat and task counters.

## Task Parameter Serialization

Task kwargs and results are JSON: plain values are data; a dict carrying the reserved key `_aaiclick_type` is a ref (`upstream`, `object`, `native`, …) or a returned task DAG (`task`, `group`, `task_result`). Vocabulary, handles and return-value rules: `docs/designs/task_interface.md`; `aaiclick/data/object/refs.py` is the single source of truth.

# Storage

## Databases

<!-- req: internal-databases -->
The `AAICLICK_CH_URL` database holds only user objects. The internal tables
(`operation_log`, `task_logs`, `schema_migrations`) live beside it in
`<data_db>_internal` (`default_internal` under chdb), so grants, retention
and backups can treat them apart. A data database ending in `_internal` is
refused, so two deployments sharing a server can't collide. Every internal query names it through
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

# Table Lifecycle & Cleanup

<!-- req: job-driven-cleanup -->
All cleanup is job-driven. The per-job `preservation_mode` gates what cleanup
does:

| Mode   | Cleanup behavior                                                |
|--------|-----------------------------------------------------------------|
| `NONE` | Drop unpinned tables as soon as refs fall to zero (default).    |
| `FULL` | Skip the drop entirely — tables live until the job TTL expires. |

Jobs completed more than `AAICLICK_JOB_TTL_DAYS` ago lose all their data: CH
tables, oplog entries, SQL metadata.

# Schema Storage

<!-- req: schema-in-sql -->
Per-table fieldtype and column metadata live as a serialised `Schema`
(Pydantic JSON) in the SQL `table_registry.schema_doc` column, read back to
hydrate the in-memory `Schema`. There is no per-column ClickHouse `COMMENT`
storage — DDL emits user columns only.

`Schema` = "what a table is" and `ViewSchema` = "how to read it", and the
registry can never see the latter: `create_object()` is the registry's only
writer and rejects a `ViewSchema`. View operators (`where`, `rename`,
`with_columns`, `explode`, column selection) never create a table or a
registry row — only `copy()` does.

# Table Naming and Scopes

<!-- req: table-name-prefixes -->
Every ClickHouse table an Object owns carries its scope in its name, so Python
and SQL cleanup tell scopes apart by prefix alone (`aaiclick/data/scope.py`):

| Scope        | Table name                | Lifetime                   |
|--------------|---------------------------|----------------------------|
| `temp`       | `t_<snowflake_id>`        | the owning context or task |
| `temp_named` | `t_<name>_<snowflake_id>` | the owning context or task |
| `job`        | `j_<job_id>_<name>`       | the owning job's TTL       |
| `global`     | `p_<name>`                | until deleted              |

`p_*` and `j_*` tables are persistent: refcount cleanup never drops them; job
TTL cleanup drops `j_*` with its job.

# Row Order

<!-- req: no-implicit-order -->
No table has an implicit order column: an insert imposes no row order, and a
read is ordered only by its own `ORDER BY`. An Object created with
`aai_id=True` adds an `aai_id` `UInt64` column with
`DEFAULT generateSnowflakeID()`, which records creation order; `concat` and
`insert` copy it like any other column and never regenerate it.
