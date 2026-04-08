Operation Log (oplog)
---

The `aaiclick/oplog/` module captures operation provenance inside ClickHouse with zero AI dependencies — every object created or transformed is recorded for lineage tracing, debugging, and post-job data sampling.

---

# ClickHouse Tables

**Implementation**: `aaiclick/oplog/models.py` — see `OPERATION_LOG_DDL`, `TABLE_REGISTRY_DDL`, `init_oplog_tables()`

## operation_log

Append-only audit log. Fields: `id` (Snowflake), `result_table`, `operation`, `kwargs` (Map), `kwargs_aai_ids` (Map), `result_aai_ids` (Array), `sql_template`, `task_id`, `job_id`, `created_at`. TTL via `AAICLICK_OPLOG_TTL_DAYS` (default 90d). ORDER BY `created_at` (nullable `job_id` excluded from sorting key).

All inputs named via `kwargs` (e.g. `{"left": ..., "right": ...}` for binary ops). `kwargs_aai_ids` and `result_aai_ids` track sampled row-level lineage by key.

## table_registry

Maps every ephemeral table to its owning `job_id` for post-job cleanup. Persistent tables (`p_` prefix) have no `job_id` and are excluded from cleanup. TTL-managed via `AAICLICK_OPLOG_TTL_DAYS` (default: 90 days), matching `operation_log`.

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

**Implementation**: `aaiclick/oplog/cleanup.py` — see `lineage_aware_drop()`, `aaiclick/orchestration/background/background_worker.py` — see `BackgroundWorker._cleanup_expired_samples()`

`lineage_aware_drop()` replaces an ephemeral table with a `{table}_sample` preserving lineage-referenced rows (fallback: random 10 rows). `BackgroundWorker._cleanup_expired_samples()` drops sample tables older than `AAICLICK_OPLOG_TTL_DAYS`.

---

# Environment Variables

| Variable                     | Default | Description                              |
|------------------------------|---------|------------------------------------------|
| `AAICLICK_OPLOG_TTL_DAYS`    | `90`    | TTL for `operation_log` rows in days     |
| `AAICLICK_OPLOG_SAMPLE_SIZE` | `10`    | Number of `aai_id`s sampled per operation|
