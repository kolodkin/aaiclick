Future Plans
---

Unimplemented features and planned work across aaiclick, ordered by priority.

---

# High Priority

## `join()` Operator

Distributed join of two Objects on a key column:

```python
basics.join(ratings, on="tconst", how="left")
```

Core data operation — table-stakes for any data framework.

## Insert Advisory Lock for Concurrent Workers

In orchestration mode, multiple workers may insert into the same persistent Object concurrently.
After the `_insert_source()` fix (generating fresh Snowflake IDs instead of preserving source
IDs), concurrent INSERTs within the same millisecond could produce interleaved IDs, mixing
rows from different logical inserts.

Use PostgreSQL advisory locks (`SELECT pg_advisory_lock(table_hash)`) to serialize inserts
per-table in distributed mode. SQLite mode is single-process and needs no lock.

## Progressive Tutorial

7-page tutorial using named snippets (`pymdownx.snippets` section markers) from existing
example files — 6 of 7 pages need zero new code. Pages: Your First Object, Operations,
Aggregations, Multi-Column Data, Views & Filters, Persistence, Orchestration. Add
`# --8<-- [start:name]` / `# --8<-- [end:name]` markers to example `.py` files, then
include specific sections in tutorial `.md` pages via snippet syntax.

Add "See Also" footers and cross-page links alongside the tutorial.

---

# Medium Priority

## Graceful Worker Stop via CLI

Add `aaiclick worker stop` — writes a stop request to the database so the worker finishes its current task then exits cleanly, instead of receiving `SIGTERM` directly.

## Schema-Aware Agent Context

Both `debug_agent` and `lineage_agent` build context from the oplog graph but never include table schemas, causing the LLM to hallucinate column names. Two improvements:

1. **Schema injection** — fetch `DESCRIBE TABLE` for every node before the agentic loop and include in the initial context message.
2. **`get_column_stats` tool** — replace `get_stats(table, column)` with a schema-first tool that returns stats for all columns without requiring the LLM to know column names upfront.

## Lineage: aai_id Uniqueness Awareness

Now that `insert()` and `concat()` generate fresh Snowflake IDs (instead of preserving source IDs),
the lineage agent should account for the fact that `aai_id` values differ between source and target
tables after insert/concat. Row-level tracing across insert/concat boundaries cannot rely on `aai_id`
matching — the agent needs to use data-value matching or oplog provenance metadata instead.

## Task Run IDs & Oplog Retry Isolation

Today tasks reuse the same `task_id` across retries. Oplog entries from failed and successful
attempts are mixed together. Add per-attempt tracking:

**Task model** — two parallel array columns:
```
run_ids:      [snowflake_1, snowflake_2, snowflake_3]
run_statuses: ["FAILED",    "FAILED",    "COMPLETED"]
```

Each attempt appends to both arrays. Current run is the last element. Full retry history in
one row — no extra table.

**operation_log** — add `run_id Nullable(UInt64)` alongside existing `task_id`/`job_id`.
`task_scope` generates a new Snowflake `run_id` per attempt and passes it to
`OrchLifecycleHandler`. On retry, oplog entries for the previous `run_id` can be cleaned up
or kept for debugging.

Requires: Alembic migration (Task model), ClickHouse schema change (operation_log), updates
to execution runner, task_scope, and OrchLifecycleHandler.

## Oplog Data Lifecycle

`table_registry` and `{table}_sample` tables have no automatic cleanup:

- **`table_registry`**: Add TTL matching `operation_log` (`AAICLICK_OPLOG_TTL_DAYS`, default 90)
- **`{table}_sample`**: Either add ClickHouse TTL on sample tables at creation, or have
  `BackgroundWorker` drop sample tables older than the oplog TTL (no lineage references remain)

---

# Deferred

Items deferred until preconditions are met.

## Comparison Page

`docs/comparison.md` — feature matrix comparing aaiclick vs Pandas, Spark, and Dask. Defer until the project has enough real-world usage to make meaningful claims.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
