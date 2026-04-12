Operation Log (oplog)
---

The `aaiclick/oplog/` module captures operation provenance inside ClickHouse with zero AI dependencies — every object created or transformed is recorded for lineage tracing, debugging, and post-job data sampling.

---

# ClickHouse Tables

**Implementation**: `aaiclick/oplog/models.py` — see `OPERATION_LOG_DDL`, `TABLE_REGISTRY_DDL`, `init_oplog_tables()`

## operation_log

Append-only audit log. Fields: `id` (Snowflake), `result_table`, `operation`, `kwargs` (Map), `kwargs_aai_ids` (Map), `result_aai_ids` (Array), `sql_template`, `task_id`, `job_id`, `created_at`. ORDER BY `created_at` (nullable `job_id` excluded from sorting key). Cleaned up by `BackgroundWorker._cleanup_expired_jobs()` when the owning job expires (see `AAICLICK_JOB_TTL_DAYS`).

All inputs named via `kwargs` (e.g. `{"left": ..., "right": ...}` for binary ops). `kwargs_aai_ids` and `result_aai_ids` track sampled row-level lineage by key.

## table_registry

Maps every table to its owning `job_id` for post-job cleanup. Both ephemeral (`t_`) and persistent (`p_`) tables are registered with the job that created them. All entries are deleted by `BackgroundWorker._cleanup_expired_jobs()` when the owning job expires.

## Initialization

`init_oplog_tables(ch_client)` creates both tables idempotently then validates schema via `_validate_schema()`. Raises `RuntimeError` on column name/type mismatch (stale table detection).

---

# OplogCollector

**Implementation**: `aaiclick/oplog/collector.py` — see `OplogCollector`, `get_oplog_collector()`

Buffer-based event sink. Collects `OperationEvent` objects in memory; batch-inserts to both `operation_log` and `table_registry` on `flush()`. Accessed via ContextVar `_oplog_collector`.

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
| `STRATEGY`   | Drop via `lineage_aware_drop()`; rows matched by the job's `sampling_strategy` are preserved in a `{table}_sample`. |

`lineage_aware_drop()` replaces an ephemeral table with a `{table}_sample` containing only the rows referenced in `kwargs_aai_ids` / `result_aai_ids` and registers the sample in `table_registry` with the owning job's metadata. When there are no referenced rows (the common `NONE` case) the table is dropped without creating a sample. `BackgroundWorker._cleanup_expired_jobs()` deletes all job data (CH tables, samples, oplog entries, SQL metadata) for jobs completed more than `AAICLICK_JOB_TTL_DAYS` ago.

---

# Sampling Strategy

**Implementation**: `aaiclick/oplog/sampling.py` — see `SamplingStrategy`, `apply_strategy()`

A `SamplingStrategy` is a `dict[str, str]` mapping fully-qualified table names to raw ClickHouse WHERE clauses:

```python
strategy: SamplingStrategy = {
    "p_kev_catalog": "cve_id = 'CVE-2024-001'",
    "t_merged": "vendor IS NULL",
}
```

`apply_strategy()` is invoked by the lifecycle queue whenever a `STRATEGY`-mode job records an operation. It translates each matched table's WHERE clause into a positional lookup and populates `kwargs_aai_ids` / `result_aai_ids` so later traversal can walk the exact rows the user asked about. Outside `STRATEGY` mode the function is never called and both arrays stay empty.

---

# Environment Variables

| Variable                             | Default | Description                                                                   |
|--------------------------------------|---------|-------------------------------------------------------------------------------|
| `AAICLICK_JOB_TTL_DAYS`              | `90`    | Days after job completion before all job data is deleted.                     |
| `AAICLICK_DEFAULT_PRESERVATION_MODE` | `NONE`  | Default preservation mode for jobs that don't pass one explicitly. One of `NONE`, `FULL`, `STRATEGY`. |
