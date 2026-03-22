Future Plans
---

Unimplemented features and planned work across aaiclick. See individual spec docs for context.

---

# Orchestration

## flatMap() and join() Operators

Planned custom operators for the orchestration layer, parallel to the existing `map()` and `reduce()` helpers in `aaiclick/orchestration/orch_helpers.py`:

- `flatMap()` — like `map()` but each callback returns multiple rows, flattened into the output Object
- `join()` — distributed join of two Objects across partitions

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

