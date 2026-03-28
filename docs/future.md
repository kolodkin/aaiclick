Future Plans
---

Unimplemented features and planned work across aaiclick. See individual spec docs for context.

---

# Orchestration

## Graceful Worker Stop via CLI

Add a `aaiclick worker stop` CLI command that requests a graceful shutdown by writing a stop request to the database (e.g. a `worker_commands` table or a flag on the worker record). The worker polls for this signal between tasks and exits cleanly after finishing its current task.

Motivated by `imdb_dataset_builder.sh` which currently sends `SIGTERM` directly to the worker process. A DB-backed stop request avoids abrupt kills, lets the worker finish in-flight tasks, and works cleanly in distributed deployments where the worker may be on a different host.

## flatMap() and join() Operators

Planned custom operators for the orchestration layer, parallel to the existing `map()` and `reduce()` helpers in `aaiclick/orchestration/orch_helpers.py`:

- `flatMap()` — like `map()` but each callback returns multiple rows, flattened into the output Object
- `join()` — distributed join of two Objects across partitions

**Motivation** (`imdb_dataset_builder`): the IMDb dataset builder curates `title.basics` but
cannot enrich movies with vote counts or average ratings from `title.ratings` because aaiclick
does not yet support joining two Objects on a key column. Once implemented, the pipeline could do:

```python
enriched = await basics.join(ratings, on="tconst", how="left")
clean = enriched.where(enriched["numVotes"] >= 100)
```

This would enable filtering out obscure movies with fewer than 100 votes and including
`averageRating` in the published dataset.

## Operation Provenance Integration (Phase 3)

Wire `OplogCollector` into `execute_task()` so all jobs automatically capture provenance with no user code changes. Spec: `docs/ai_layer_plan.md` Phase 3, `docs/ai.md`.

**Tasks**:

1. **Wire OplogCollector into `execute_task()`**
   - Always creates `OplogCollector(task_id=task.id, job_id=job.id)`
   - Passes as `data_context(oplog=collector)` — no Job-level flag needed
   - Collector auto-flushes on context exit

2. **AI agents as `@task` wrappers** — lazy import, participates in normal DAG dependencies

3. **Integration tests** — job execution → verify `operation_log` populated

**Target**: `aaiclick/orchestration/execution.py` — wire into `execute_task()`

---

# AI Agents

## Schema-Aware Agent Context

Both `debug_agent` and `lineage_agent` build an initial context from the oplog graph but never
include table schemas. The LLM has no knowledge of actual column names, so it guesses — which
causes `get_stats` calls with hallucinated columns like `created_at` (seen in CI, fixed with a
try/except guard, but the root cause remains).

Two complementary improvements:

### 1. Schema injection into initial context

Before the agentic loop, fetch `DESCRIBE TABLE` for every table in the oplog graph and include
the result in the context block that is sent in the first user message. The LLM then knows the
exact column names before making any tool calls.

```python
# debug_agent.py — augment context building
for node in nodes:
    schema = await get_schema(node.table)          # existing tool function
    context += f"\n\nSchema of `{node.table}`:\n{schema}"
```

`lineage_agent.py` already calls `sample_table()` per node in a similar loop — schema injection
follows the same pattern.

**Expected outcome**: LLM stops hallucinating column names; `get_stats` calls become accurate on
the first attempt rather than relying on error recovery.

### 2. `get_column_stats` — stats for all real columns

Add a new tool (or replace `get_stats`) that reads the schema first and returns stats for every
column in the table, without requiring the LLM to know column names in advance:

```python
async def get_column_stats(table: str) -> str:
    """Return count, non-null count, min, and max for every column in a table."""
    schema_result = await ch_client.query(f"DESCRIBE TABLE {table_escaped}")
    columns = [row[0] for row in schema_result.result_rows]
    ...
```

This eliminates the `column` parameter entirely and makes the tool self-contained. The LLM calls
`get_column_stats(table)` and receives a full profile — no schema knowledge required upfront.

**Trade-off**: May be verbose for wide tables; `get_stats(table, column)` remains useful for
targeted drill-downs. Both tools can coexist.

---

# Oplog

## Table Lifecycle & Cleanup (Phase 3)

On job completion (COMPLETED / FAILED / CANCELLED), a cleanup worker replaces each ephemeral table with a 10-row sample, keeping `operation_log` references valid:

```python
result = await ch_client.query(
    "SELECT table_name FROM table_registry WHERE job_id = {job_id:UInt64}"
)
for (table,) in result.result_rows:
    await ch_client.command(f"CREATE TABLE {table}_sample AS {table}")
    await ch_client.command(f"INSERT INTO {table}_sample SELECT * FROM {table} LIMIT 10")
    await ch_client.command(f"DROP TABLE {table}")
    await ch_client.command(f"RENAME TABLE {table}_sample TO {table}")
await ch_client.command("DELETE FROM table_registry WHERE job_id = {job_id:UInt64}")
```

`CREATE TABLE new AS source` copies ENGINE, ORDER BY, and codecs without data. Renaming back to the original name keeps `operation_log` references valid. AI agents calling `sample_table()` on historical nodes transparently return the preserved sample.

Persistent tables (`p_` prefix) excluded — no `job_id` in registry.

**Deliverables**:
- Post-job table sampling preserves lineage-accessible data
- Cleanup worker integrated into `PgCleanupWorker` or standalone background service

## Pinned Row Sampling (Phase 5)

Allow user-defined predicates that ensure matching rows always survive cleanup. Phase 3 preserves an arbitrary 10 rows; Phase 5 lets you guarantee semantically important rows are included.

```python
pin_rows("my_table", where="value < 5")
```

Rules are WHERE clause predicates registered during task execution (before job completion triggers cleanup). Cleanup prioritises matching rows, fills remainder up to 10 with arbitrary rows.

---

# Data / Object API

## `literal()` Computed Helper

Add a `literal(value, type)` factory function to `aaiclick/data/transforms.py` that creates a `Computed` constant column — the counterpart to `cast()` and `split_by_char()`.

**Motivation**: constant tag columns appear repeatedly when labelling rows during union/concat operations, e.g. in `cyber_threat_feeds/consolidated.py`:

```python
# Current — raw Computed:
.with_columns({
    "source": Computed("String", "'kev'"),
    "is_kev": Computed("Bool", "true"),
})

# With literal():
.with_columns({
    "source": literal("kev", "String"),
    "is_kev": literal(True, "Bool"),
})
```

Python value → SQL literal mapping: `str` → `'value'`, `bool` → `true`/`false`, `int`/`float` → bare numeric. Export from `aaiclick` top-level alongside `cast` and `split_by_char`.

