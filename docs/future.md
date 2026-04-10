Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# High Priority

## `join()` Operator

Distributed join of two Objects on a key column:

```python
basics.join(ratings, on="tconst", how="left")
```

Core data operation — table-stakes for any data framework.

## Insert Advisory Lock for Concurrent Workers

Concurrent workers inserting into the same persistent Object can produce interleaved Snowflake IDs within the same millisecond.

Serialize via PostgreSQL advisory locks (`pg_advisory_lock(table_hash)`) per-table. SQLite mode is single-process and needs no lock.

## Progressive Tutorial

7-page tutorial using named snippets (`pymdownx.snippets` section markers) from existing
example files — 6 of 7 pages need zero new code. Pages: Your First Object, Operations,
Aggregations, Multi-Column Data, Views & Filters, Persistence, Orchestration. Add
`# --8<-- [start:name]` / `# --8<-- [end:name]` markers to example `.py` files, then
include specific sections in tutorial `.md` pages via snippet syntax.

Add "See Also" footers and cross-page links alongside the tutorial.

---

# Medium Priority

## `PENDING_CLEANUP` Task Status for Retry Lifecycle ✅ IMPLEMENTED

**Implementation**: `aaiclick/orchestration/models.py` — see `TaskStatus.PENDING_CLEANUP`

Task failure now transitions through `PENDING_CLEANUP` before reaching `PENDING` or `FAILED`:

```
RUNNING → PENDING_CLEANUP (on failure) → PENDING (after cleanup) or FAILED (no retries)
```

The background worker (`BackgroundWorker._process_pending_cleanup`) handles all ref cleanup:

1. Worker sets failed task to `PENDING_CLEANUP` — see `worker._set_pending_cleanup()`
2. Background worker finds `PENDING_CLEANUP` tasks, cleans `run_refs` via `clean_task_run(run_id)` and `pin_refs` via `clean_task_pins(task_id)`
3. Transitions to `PENDING` (retries remaining, with exponential backoff) or `FAILED` (exhausted)

Dead worker detection also uses `PENDING_CLEANUP` — orphaned tasks from crashed workers go through the same cleanup path instead of being marked `FAILED` directly.

## Deduplicate `_try_complete_job`

`_try_complete_job` exists in two places with identical logic (check if all tasks are terminal, mark job COMPLETED or FAILED):

- `worker._try_complete_job(job_id)` — uses ORM via `get_sql_session()`, requires active `orch_context`
- `BackgroundWorker._try_complete_job(session, job_id)` — uses raw SQL on a passed session, independent of `orch_context`

The background worker operates with its own engine outside `orch_context`, so it cannot call the worker version directly. Unify by extracting a shared session-accepting helper that both callers use.

## Schema-Aware Agent Context ✅ IMPLEMENTED

**Implementation**: `aaiclick/ai/agents/tools.py` — see `get_schemas_for_nodes()` and `get_column_stats()`

Both `debug_agent` and `lineage_agent` now include table schemas in their initial context via
`get_schemas_for_nodes()`, which fetches `DESCRIBE TABLE` for every node in the lineage graph.
The `get_stats(table, column)` tool has been replaced by `get_column_stats(table)`, which discovers
columns automatically and returns stats for all of them in a single round-trip.

## Lineage: aai_id Uniqueness Awareness ✅ IMPLEMENTED

`insert()` and `concat()` generate fresh Snowflake IDs, so `aai_id` values differ between source and target. Row-level tracing across these boundaries needs data-value matching or oplog provenance metadata instead of `aai_id` matching.


## Lineage: Three-Phase Debugging

Question-driven lineage debugging in three phases: graph structure (have today), targeted sampling via WHERE clauses derived from the user's question, and row-level trace using those targeted samples. Replaces random pre-sampling with on-demand, question-driven sampling.

**Design**: `docs/lineage_3_phases.md`

---

# Deferred

Items deferred until preconditions are met.

## Comparison Page

`docs/comparison.md` — feature matrix comparing aaiclick vs Pandas, Spark, and Dask. Defer until the project has enough real-world usage to make meaningful claims.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
