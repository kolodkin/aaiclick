Operation Log (oplog)
---

The `aaiclick/oplog/` module captures operation provenance inside ClickHouse with zero AI dependencies — every object created or transformed is recorded for lineage tracing and debugging.

---

# Storage

## operation_log (ClickHouse)

**Implementation**: `aaiclick/oplog/models.py` — see `OPERATION_LOG_DDL`, `init_oplog_tables()`

Append-only audit log. Fields: `id` (Snowflake), `result_table`, `operation`, `kwargs` (Map), `sql_template`, `task_id`, `job_id`, `created_at`. ORDER BY `(result_table, created_at)`. Cleaned up by `BackgroundWorker._cleanup_expired_jobs()` when the owning job expires (see `AAICLICK_JOB_TTL_DAYS`).

All inputs named via `kwargs` (e.g. `{"left": ..., "right": ...}` for binary ops).

## table_registry (SQL)

**Implementation**: `aaiclick/orchestration/lifecycle/db_lifecycle.py` — see `TableRegistry`

Ownership metadata for every ClickHouse data table: `table_name` (PK) → `(job_id, task_id, run_id, created_at)`. Written once at creation by the lifecycle handler's queued `OPLOG_TABLE` op; deleted by `BackgroundWorker._cleanup_unreferenced_tables()` when the table is dropped, and by `_cleanup_expired_jobs()` / `_cleanup_orphaned_resources()` for TTL'd rows.

Previously lived in ClickHouse as an append-only MergeTree table. Moved to SQL because every consumer is a keyed lookup or owner join during background cleanup — not append-only audit.

## Initialization

`init_oplog_tables(ch_client)` creates `operation_log` on CH idempotently and validates its schema via `_validate_schema()`. It also performs a one-time copy of any pre-existing CH `table_registry` rows into SQL and drops the CH side (no-op on fresh installs).

---

# OplogCollector

**Implementation**: `aaiclick/oplog/collector.py` — see `OplogCollector`, `get_oplog_collector()`

Buffer-based event sink. Collects `OperationEvent` objects in memory; batch-inserts to `operation_log` (CH) and `table_registry` (SQL) on `flush()`. Accessed via ContextVar `_oplog_collector`.

---

# Instrumentation Points

**Implementation**: `aaiclick/data/operators.py`, `aaiclick/data/ingest.py`, `aaiclick/data/object.py`, `aaiclick/data/data_context.py`

Each instrumentation is a 2-line addition: get collector from ContextVar, call `record()` if not None.

| Location                                  | Operation              | `kwargs`                                                      |
|-------------------------------------------|------------------------|---------------------------------------------------------------|
| `data_context.create_object()`            | (none — registry only) | —                                                             |
| `data_context.create_object_from_value()` | `"create_from_value"`  | `{}`                                                          |
| `operators._apply_operator_db()`          | `"add"`, `"sub"`, etc. | `{"left": left.table, "right": right.table}`                  |
| `operators._apply_aggregation()`          | `"sum"`, `"mean"`, etc.| `{"source": source.table}`                                    |
| `ingest.concat_objects_db()`              | `"concat"`             | `{"source_0": s0.table, "source_1": s1.table, ...}`          |
| `ingest.insert_objects_db()`              | `"insert"`             | `{"source": src.table, "target": tgt.table}`                  |
| `object.Object.copy()`                    | `"copy"`               | `{"source": self.table}`                                      |

`create_object()` only calls `record_table()` (populates `table_registry`), not `record()`, to avoid double-counting — higher-level functions record the operation that produced the table.

---

# Graph Queries

**Implementation**: `aaiclick/oplog/lineage.py` — see `backward_oplog()`, `forward_oplog()`, `oplog_subgraph()`, `OplogGraph.to_prompt_context()`

Graph traversal over `operation_log`. `backward_oplog()` traces upstream lineage via recursive CTE. `OplogGraph.to_prompt_context()` formats the graph as plain text for LLM consumption.

---

# Table Lifecycle & Cleanup ✅ IMPLEMENTED

**Implementation**: `aaiclick/oplog/cleanup.py` — see `lineage_aware_drop()`, `aaiclick/orchestration/background/background_worker.py` — see `BackgroundWorker._cleanup_unreferenced_tables()` and `BackgroundWorker._cleanup_expired_jobs()`

All cleanup is job-driven. The per-job `preservation_mode` gates what cleanup does:

| Mode         | Cleanup behavior                                                              |
|--------------|-------------------------------------------------------------------------------|
| `NONE`       | Drop unpinned tables as soon as refs fall to zero (default).                  |
| `FULL`       | Skip the drop entirely — tables live until the job TTL expires.               |

`BackgroundWorker._cleanup_expired_jobs()` deletes all job data (CH tables, oplog entries, SQL metadata) for jobs completed more than `AAICLICK_JOB_TTL_DAYS` ago.

---

# Environment Variables

| Variable                             | Default | Description                                                                   |
|--------------------------------------|---------|-------------------------------------------------------------------------------|
| `AAICLICK_JOB_TTL_DAYS`              | `90`    | Days after job completion before all job data is deleted.                     |
| `AAICLICK_DEFAULT_PRESERVATION_MODE` | `NONE`  | Default preservation mode for jobs that don't pass one explicitly. One of `NONE`, `FULL`. |
