Oplog Lineage: Row-Level Sampling
---

Row-level lineage via sampled `aai_id` mappings per operation. For each operation, sample N
random row positions and record which source `aai_id`s produced which result `aai_id`s — stored
inline in `operation_log`.

# Motivation

Current oplog captures **table-level** lineage only: "table C was produced by `add(A, B)`". This is
enough to build a dependency graph but not enough to trace individual values. With row-level
sampling:

- An AI agent can trace a specific output row back through the full operation chain to its origins
- Cleanup preserves semantically meaningful rows (those that appear in lineage) instead of random ones
- Lineage graphs become walkable at row level, not just table level

# Design Decisions

## kwargs-only (drop `args` column) ✅ IMPLEMENTED

Every input gets a named key in `kwargs`. No positional `args` column.

| Operation       | Before                                  | After                                                        |
|-----------------|-----------------------------------------|--------------------------------------------------------------|
| Binary ops      | `kwargs={"left": A, "right": B}`        | Same                                                         |
| Aggregations    | `kwargs={"source": A}`                  | Same                                                         |
| isin            | `kwargs={"source": A, "other": B}`      | Same                                                         |
| concat          | `args=[A, B, C]`                        | `kwargs={"source_0": A, "source_1": B, "source_2": C}`      |
| insert          | `kwargs={"source": A, "target": B}`     | Same                                                         |
| copy            | `kwargs={"source": A}`                  | Same                                                         |
| create_from_val | `args=[], kwargs={}`                    | `kwargs={}` (leaf node, no lineage)                          |

## Lineage stored in `operation_log` as dedicated columns ✅ IMPLEMENTED

```sql
operation_log (
    id              UInt64 DEFAULT generateSnowflakeID(),
    result_table    String,
    operation       String,
    kwargs          Map(String, String),
    kwargs_aai_ids  Map(String, Array(UInt64)),
    result_aai_ids  Array(UInt64),
    sql_template    Nullable(String),
    task_id         Nullable(UInt64),
    job_id          Nullable(UInt64),
    created_at      DateTime64(3)
)
```

`kwargs_aai_ids` mirrors `kwargs` by key. For `C = A + B`:

```python
kwargs          = {"left": "table_A",   "right": "table_B"}
kwargs_aai_ids  = {"left": [123, 456],  "right": [321, 654]}
result_aai_ids  = [111, 222, 333]
```

Default `map()` / `[]` for operations without lineage (backward compatible).

## Oplog via OrchLifecycleHandler queue ✅ IMPLEMENTED

Oplog only exists in **orchestration mode** (`task_scope`). In local `data_context` mode,
the `LifecycleHandler` base class no-op methods handle calls silently.

`OrchLifecycleHandler` processes oplog on the **same FIFO queue** as `INCREF`/`DECREF`.
This guarantees lineage sampling queries run while source tables are still alive — before
any `DECREF` can drop them.

```
OPLOG_SAMPLE  {result: C, kwargs: {"left": A, "right": B}}   ← sampling query runs here
DECREF        A                                                ← table A still alive above
DECREF        B
```

Each oplog message is written to ClickHouse **immediately** when dequeued — no buffering,
no flush/discard. Failed tasks still have their oplog entries (useful for debugging).

### Architecture

```
oplog_record_sample(result, "add", kwargs={"left": A, "right": B})
  → get_data_lifecycle()
    → LocalLifecycleHandler.oplog_record_sample()   # no-op (pass)
    → OrchLifecycleHandler.oplog_record_sample()    # enqueues OPLOG_SAMPLE
      → _process_loop dequeues → sample_lineage() → write to CH
```

Module-level functions in `aaiclick/oplog/collector.py` delegate directly to the lifecycle
handler. No intermediate `OplogCollector` class — `OrchLifecycleHandler` already holds
`task_id`/`job_id` from construction.

## Sampling strategy: prefer lineage-connected rows ✅ IMPLEMENTED

**Implementation**: `aaiclick/oplog/sampling.py` — see `sample_lineage()`, `_pick_aai_ids()`

When sampling N `aai_id`s from a source table, prefer IDs that already appear in existing oplog
lineage entries. This maximizes chain connectivity — each new operation extends existing traced
rows rather than starting fresh random chains.

1. **First preference**: pick `aai_id`s from the source table that appear in previous
   `result_aai_ids` for that table
2. **Fallback**: if fewer than N matches found, fill remaining slots with random rows

Sample size N defaults to 10 (`AAICLICK_OPLOG_SAMPLE_SIZE` env var).

## Lineage-aware cleanup

When a table's refcount hits zero, instead of dropping immediately (or sampling 1000 random rows),
the worker checks if any of the table's `aai_id`s appear in `kwargs_aai_ids` or `result_aai_ids`
of existing `operation_log` entries. Those rows are preserved; the rest are dropped.

This replaces both the current random-sample-on-delete and the planned Phase 5 pinned-row sampling
with a single mechanism: **the lineage sample defines which rows survive**.

---

# Implementation Plan

## Phase 1: Schema Migration ✅ IMPLEMENTED

- Removed `args Array(String)` column from DDL and expected-columns dict
- Added `kwargs_aai_ids Map(String, Array(UInt64))` and `result_aai_ids Array(UInt64)` columns
- Updated `_validate_schema()` expected columns
- Migrated concat from `args=[t1, t2]` to `kwargs={"source_0": t1, "source_1": t2}`
- Updated all lineage queries, AI agents, tests, and docs to kwargs-only

## Phase 2: Oplog via Lifecycle Handler Queue ✅ IMPLEMENTED

- `OrchLifecycleHandler` handles `OPLOG_RECORD`, `OPLOG_SAMPLE`, `OPLOG_TABLE` on its existing
  FIFO queue alongside `INCREF`/`DECREF`/`PIN`
- Immediate writes to ClickHouse per message — no buffer, no flush/discard
- `LifecycleHandler` base has no-op oplog methods — `LocalLifecycleHandler` inherits them
- Module-level functions (`oplog_record`, `oplog_record_sample`, `oplog_record_table`) delegate
  directly to `get_data_lifecycle()` — no intermediate `OplogCollector` class
- Lineage sampling logic in `aaiclick/oplog/sampling.py`

## Phase 3: Instrument Operations ✅ IMPLEMENTED

- Operations with sources use `oplog_record_sample()` (binary ops, aggregations, nunique, isin,
  concat, insert, copy)
- Leaf operations (`create_from_value`) use `oplog_record()`
- `oplog_record_table()` for table registry

## Phase 4: Lineage-Aware Cleanup

**Objective**: When dropping a table, preserve rows referenced by lineage instead of random sampling.

- On DECREF reaching zero, before DROP:
  1. Query `operation_log` for all `kwargs_aai_ids` and `result_aai_ids` entries referencing
     the table (via `kwargs` values matching the table name)
  2. Collect the set of referenced `aai_id`s
  3. `CREATE TABLE {table}_sample AS SELECT * FROM {table} WHERE aai_id IN ({referenced_ids})`
  4. Drop original table
- If no referenced IDs found, fall back to `LIMIT 10` random sample
- Persistent tables (`p_` prefix) are excluded as before

## Phase 5: Update Lineage Queries

**Objective**: Update `backward_oplog()` / `forward_oplog()` / `oplog_subgraph()` to leverage
row-level lineage.

- `OplogNode` already includes `kwargs_aai_ids` and `result_aai_ids` (done in Phase 1)
- Add `backward_oplog_row(table, aai_id)` — trace a specific row backward through the chain
- Update `OplogGraph.to_prompt_context()` to include sample data when available

---

# Resolved Questions

1. **Sample size N**: `AAICLICK_OPLOG_SAMPLE_SIZE` env var, default 10. Global, not per-operation.
2. **Aggregation sampling**: Same N=10, no special case. Shows example input rows for context.
3. **Filter/where operations**: Record oplog entry (operation + kwargs) but no lineage sampling
   (empty `kwargs_aai_ids` / `result_aai_ids`). Identity mapping is implicit from operation type.
4. **Pool size for sampling query**: Not needed — the "prefer lineage-connected rows" strategy
   uses targeted `IN` lookups, not full-table scans. Random fallback uses `ORDER BY rand() LIMIT N`.
5. **OplogCollector**: Removed. Module-level functions delegate directly to the lifecycle handler.
   `OrchLifecycleHandler` holds task_id/job_id from construction.
6. **Oplog in local mode**: No-op. `LifecycleHandler` base class has empty oplog methods.
   `LocalLifecycleHandler` inherits them. No ContextVar gate needed.
7. **Buffering vs immediate writes**: Immediate. Each oplog message is written to ClickHouse
   when dequeued from the FIFO. No buffer, no flush, no discard.
