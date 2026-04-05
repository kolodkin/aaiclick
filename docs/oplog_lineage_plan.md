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

- Add `OPLOG_SAMPLE` op to `TableOp` enum
- Extend `TableMessage` to carry sampling context (result table, source tables, N)
- Add `_sample_lineage()` method to the worker — runs the sampling query via `self._ch_client`
- Worker buffers sampled `(kwargs_aai_ids, result_aai_ids)` tuples internally
- Add `OPLOG_FLUSH` op that batch-inserts buffered oplog entries to `operation_log`
  (replaces current `OplogCollector.flush()`)
- Ensure SHUTDOWN drains pending samples before stopping

**Sampling query** (for binary op `C = A + B`):

```sql
SELECT r.aai_id, a.aai_id, b.aai_id
FROM (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM C LIMIT {pool}) r
JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM A LIMIT {pool}) a ON r.rn = a.rn
JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM B LIMIT {pool}) b ON r.rn = b.rn
ORDER BY rand() LIMIT {n}
```

Where `pool` caps the row_number scan for large tables. `n` is the sample size (default 10).

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

- Add `oplog` parameter to `data_context()` (`bool | OplogCollector`)
- On context entry: start the unified worker with oplog capability
- On clean exit: flush oplog via `OPLOG_FLUSH` message, await completion
- On exception: discard buffered samples (skip `OPLOG_FLUSH`)
- Orchestration's `task_scope()` passes pre-configured collector with `task_id`/`job_id`

**Deliverables**: Oplog + lineage active by default. No user code changes needed.

---

# Open Questions

1. **Sample size N** — Default 10? Configurable per-operation or globally via env var?
2. **Aggregation sampling** — For `sum(A)`, sample N source `aai_id`s mapping to 1 result. Is N=10
   enough to be diagnostically useful?
3. **Filter/where operations** — `aai_id` passes through unchanged (identity mapping). Record
   lineage (redundant but complete) or skip (implicit from operation type)?
4. **Pool size for sampling query** — For very large tables, should `row_number()` scan be capped?
   e.g., sample from first 10K rows only to bound query cost.
5. **OplogCollector vs unified worker** — Phase 2 merges oplog into the table worker. Should
   `OplogCollector` be retired entirely, or kept as a lightweight facade?
