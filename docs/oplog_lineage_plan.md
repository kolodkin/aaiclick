Oplog Lineage: Row-Level Sampling
---

Row-level lineage via sampled `aai_id` mappings per operation. For each operation, sample N random
row positions and record which source `aai_id`s produced which result `aai_id`s — stored inline in
`operation_log`.

# Motivation

Current oplog captures **table-level** lineage only: "table C was produced by `add(A, B)`". This is
enough to build a dependency graph but not enough to trace individual values. With row-level
sampling:

- An AI agent can verify "3.14 + 2.71 = 5.85" on concrete rows
- Cleanup preserves semantically meaningful rows (those that appear in lineage) instead of random ones
- Debugging becomes surgical: trace a specific output row back through the operation chain

# Design Decisions

## kwargs-only (drop `args` column)

Every input gets a named key in `kwargs`. No positional `args` column.

| Operation       | Today                                   | New                                                          |
|-----------------|-----------------------------------------|--------------------------------------------------------------|
| Binary ops      | `kwargs={"left": A, "right": B}`        | Same                                                         |
| Aggregations    | `kwargs={"source": A}`                  | Same                                                         |
| isin            | `kwargs={"source": A, "other": B}`      | Same                                                         |
| concat          | `args=[A, B, C]`                        | `kwargs={"source_0": A, "source_1": B, "source_2": C}`      |
| insert          | `kwargs={"source": A, "target": B}`     | Same                                                         |
| copy            | `kwargs={"source": A}`                  | Same                                                         |
| create_from_val | `args=[], kwargs={}`                    | `kwargs={}` (leaf node, no lineage)                          |

## Lineage stored in `operation_log` as dedicated columns

```sql
operation_log (
    id              UInt64 DEFAULT generateSnowflakeID(),
    result_table    String,
    operation       String,
    kwargs          Map(String, String),
    kwargs_aai_ids  Map(String, Array(UInt64)),   -- parallel to kwargs keys
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

## Unified worker queue (oplog sampling + table lifecycle)

Lineage sampling requires source tables to be alive. Today `AsyncTableWorker` processes
DECREF messages that may drop tables. If sampling runs on a separate queue, a race exists:
decref could drop a source before the sampling query runs.

**Solution**: single FIFO queue processes both sampling and lifecycle messages. Operation code
enqueues an `OPLOG_SAMPLE` message, then later when Python GC collects source Objects, DECREF
messages arrive. The worker processes them in order — sampling always runs before drops.

```
OPLOG_SAMPLE  {result: C, sources: {"left": A, "right": B}, n: 10}
DECREF        A
DECREF        B
```

## Lineage-aware cleanup

When a table's refcount hits zero, instead of dropping immediately (or sampling 1000 random rows),
the worker checks if any of the table's `aai_id`s appear in `kwargs_aai_ids` or `result_aai_ids`
of existing `operation_log` entries. Those rows are preserved; the rest are dropped.

This replaces both the current random-sample-on-delete and the planned Phase 5 pinned-row sampling
with a single mechanism: **the lineage sample defines which rows survive**.

---

# Implementation Plan

## Phase 1: Schema Migration

**Objective**: Update `operation_log` schema — drop `args`, add lineage columns.

- Remove `args Array(String)` column from DDL and expected-columns dict
- Add `kwargs_aai_ids Map(String, Array(UInt64)) DEFAULT map()` column
- Add `result_aai_ids Array(UInt64) DEFAULT []` column
- Create Alembic migration for the schema change (ClickHouse `ALTER TABLE`)
- Update `_validate_schema()` expected columns
- Update `_OPLOG_COLS` and `_OPLOG_TYPE_NAMES` in collector
- Migrate all `args`-based call sites to kwargs:
  - `concat_objects_db()`: `args=[t1, t2]` → `kwargs={"source_0": t1, "source_1": t2}`

**Deliverables**: Schema updated, all existing oplog tests pass, no functional change yet.

## Phase 2: Unify Worker Queue

**Objective**: Merge oplog sampling into `AsyncTableWorker` so ordering is guaranteed.

- Retire `OplogCollector` (`aaiclick/oplog/collector.py`) — all buffering moves into the worker
- Add `OPLOG_SAMPLE` and `OPLOG_RECORD` ops to `TableOp` enum
- Extend `TableMessage` to carry sampling context (result table, source tables, N)
  and plain oplog metadata (for operations without sampling, e.g. filters, create_from_value)
- Add `_sample_lineage()` method to the worker — runs the sampling query via `self._ch_client`
- Worker buffers sampled `(kwargs_aai_ids, result_aai_ids)` tuples internally
- Add `OPLOG_FLUSH` op that batch-inserts buffered oplog entries to `operation_log`
- Ensure SHUTDOWN drains pending samples before stopping
- Expose a module-level `oplog_enqueue()` function (replaces `oplog_record()`) that puts
  messages on the worker queue via `call_soon_threadsafe`

**Sampling strategy** (for binary op `C = A + B`):

1. For each source table, find `aai_id`s already in oplog lineage (prefer connected chains):
   ```sql
   SELECT aai_id FROM {source_table}
   WHERE aai_id IN (
       SELECT arrayJoin(result_aai_ids) FROM operation_log WHERE result_table = '{source_table}'
       UNION ALL
       SELECT arrayJoin(arrayJoin(mapValues(kwargs_aai_ids))) FROM operation_log
       WHERE hasAny(mapValues(kwargs), ['{source_table}'])
   )
   ```
2. If fewer than N matches, fill with random: `ORDER BY rand() LIMIT {remaining}`
3. Use the selected source `aai_id`s' positions to find corresponding result `aai_id`s via
   `row_number() OVER (ORDER BY aai_id)` join

Sample size N defaults to 10 (`AAICLICK_OPLOG_SAMPLE_SIZE` env var).

**Deliverables**: Single worker handles both lifecycle and oplog. Sampling queries run off the
main execution path.

## Phase 3: Instrument Operations

**Objective**: Each operation enqueues an `OPLOG_SAMPLE` message (non-blocking) instead of
calling `oplog_record()`.

- Replace `oplog_record()` calls with queue enqueue at each instrumentation point:
  - `_apply_operator_db()` — binary ops
  - `_apply_aggregation()` — sum, mean, std, etc.
  - `_apply_nunique()` — nunique
  - `_apply_isin()` — isin
  - `concat_objects_db()` — concat
  - `insert_objects_db()` — insert
  - `Object.copy()` — copy
  - `create_object_from_value()` — leaf (no sampling, just metadata)
- Each call site specifies the role→table mapping; worker builds the sampling query from it
- `oplog_record()` / `oplog_record_table()` remain for backward compat but delegate to the queue

**Deliverables**: All operations produce lineage samples. Zero blocking on the main execution path.

## Phase 4: Lineage-Aware Cleanup

**Objective**: When dropping a table, preserve rows referenced by lineage instead of random sampling.

- On DECREF reaching zero, before DROP:
  1. Query `operation_log` for all `kwargs_aai_ids` and `result_aai_ids` entries referencing
     the table (via `kwargs` values matching the table name)
  2. Collect the set of referenced `aai_id`s
  3. `CREATE TABLE {table}_sample AS SELECT * FROM {table} WHERE aai_id IN ({referenced_ids})`
  4. Drop original table
- If no referenced IDs found, fall back to `LIMIT 10` random sample (same as current Phase 3 plan)
- Persistent tables (`p_` prefix) are excluded as before

**Deliverables**: Cleanup preserves lineage-relevant rows. AI agents can walk the full lineage
chain through samples.

## Phase 5: Update Lineage Queries

**Objective**: Update `backward_oplog()` / `forward_oplog()` / `oplog_subgraph()` to leverage
row-level lineage.

- Update `OplogNode` to include `kwargs_aai_ids` and `result_aai_ids`
- Add `backward_oplog_row(table, aai_id)` — trace a specific row backward through the chain
- Update `OplogGraph.to_prompt_context()` to include sample data when available
- Update recursive CTE in `backward_oplog()` to use `kwargs` (no `args`) for source resolution
- Remove `args`-based edge construction from `oplog_subgraph()`

**Deliverables**: Lineage queries return row-level samples. AI agents get concrete examples.

## Phase 6: Activate Oplog in data_context (existing Phase 3 from future.md)

**Objective**: Wire everything into `data_context()` so oplog activates automatically.

- Add `oplog` parameter to `data_context()` (`bool`)
- On context entry: start the unified worker with oplog capability
- On clean exit: flush oplog via `OPLOG_FLUSH` message, await completion
- On exception: discard buffered samples (skip `OPLOG_FLUSH`)
- Orchestration's `task_scope()` passes `task_id`/`job_id` to the worker directly

**Deliverables**: Oplog + lineage active by default. No user code changes needed.

## Sampling Strategy: Prefer Lineage-Connected Rows

When sampling N `aai_id`s from a source table, prefer IDs that already appear in existing oplog
lineage entries (`result_aai_ids` or `kwargs_aai_ids`). This maximizes chain connectivity — each
new operation extends existing traced rows rather than starting fresh random chains.

1. **First preference**: pick `aai_id`s from the source table that appear in previous oplog entries
   for that table (as `result_aai_ids` or in `kwargs_aai_ids`)
2. **Fallback**: if fewer than N matches found, fill remaining slots with random rows

This also makes pool-size concerns irrelevant — the primary path is a targeted `IN` lookup against
a small set of known IDs, not a full-table `row_number()` scan.

---

# Resolved Questions

1. **Sample size N**: `AAICLICK_OPLOG_SAMPLE_SIZE` env var, default 10. Global, not per-operation.
2. **Aggregation sampling**: Same N=10, no special case. Shows example input rows for context.
3. **Filter/where operations**: Record oplog entry (operation + kwargs) but no lineage sampling
   (empty `kwargs_aai_ids` / `result_aai_ids`). Identity mapping is implicit from operation type.
4. **Pool size for sampling query**: Not needed — the "prefer lineage-connected rows" strategy
   uses targeted `IN` lookups, not full-table scans. Random fallback uses `ORDER BY rand() LIMIT N`.

# Resolved Questions (continued)

5. **OplogCollector**: Retire entirely. The unified worker handles buffering, sampling, and
   flushing. `oplog_record()` / `oplog_record_table()` are replaced by direct queue enqueue.
   Remove `aaiclick/oplog/collector.py` in Phase 2.
