Move `table_registry` from ClickHouse to SQL
---

# Context

`table_registry` maps every ClickHouse data table aaiclick creates to its owning `(job_id, task_id, run_id)`. Today it lives in ClickHouse alongside `operation_log`, but it is **cleanup metadata, not append-only audit**:

- Every read is a keyed lookup (`WHERE table_name = ?`, `WHERE job_id = ?`) or a join against owning jobs.
- Cleanup already joins three SQL ref tables (`table_context_refs`, `table_pin_refs`, `table_run_refs`); `table_registry` is the only piece still on CH, forcing the background worker to split each cleanup decision across two engines.
- Row-level mutation uses CH `ALTER TABLE ... DELETE` — async, heavy, eventually consistent. SQL `DELETE` is immediate.
- CH has no migration framework; DDL changes on `table_registry` are silent no-ops on existing installs.

Moving it to SQL collapses `_cleanup_unreferenced_tables` to a single cross-table join, drops CH from every metadata read path (only `DROP TABLE` stays), and puts the table under Alembic's schema management.

This plan covers only the `table_registry` move. It does **not** consolidate with `table_context_refs` — registry is 1:1 ownership (immutable, outlives all refs); `table_context_refs` is N:1 refcount (mutable, deleted on decref). Cleanup needs both.

# Goals

- One SQL table replaces the CH `table_registry`.
- All reads in `background_worker.py` are pure SQL; the CH client is used only to issue `DROP TABLE` on the actual data tables.
- Write path stays queued through `OrchLifecycleHandler` — object creation does not block on metadata I/O.
- Existing installs are migrated transparently; the CH-side table is removed at the end.

# Scope

Eight files touched. No change to public API or object semantics.

| File | Change |
|------|--------|
| `aaiclick/orchestration/lifecycle/db_lifecycle.py`                      | Add `TableRegistry` SQLModel |
| `aaiclick/orchestration/migrations/versions/<new>_move_table_registry_to_sql.py` | Create SQL table on top of head `bfb0653578fb` |
| `aaiclick/orchestration/orch_context.py`                                | `_write_table_registry_row()` → SQL insert via `get_sql_session()` |
| `aaiclick/orchestration/background/background_worker.py`                | Rewrite 4 read sites to SQL; drop CH metadata queries |
| `aaiclick/oplog/models.py`                                              | Remove CH DDL + validator; add one-time backfill helper |
| `aaiclick/oplog/cleanup.py`                                             | `TableOwner` unchanged (source changes only) |
| `aaiclick/oplog/test_collector.py`                                      | Retarget registry assertion at SQL |
| `docs/oplog.md`, `docs/future.md`                                       | Remove future.md entry; update oplog.md |

# Design

## New SQL table

```python
class TableRegistry(SQLModel, table=True):
    __tablename__: ClassVar[str] = "table_registry"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    job_id: int | None = Field(sa_column=Column(BigInteger, nullable=True, index=True))
    task_id: int | None = Field(sa_column=Column(BigInteger, nullable=True))
    run_id: int | None = Field(sa_column=Column(BigInteger, nullable=True))
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
```

`table_name` is the PK (registry is strictly 1:1 — existing CH schema had no PK because MergeTree can't). Indexes on `job_id` (hot path for `_delete_job_data`) and `created_at` (orphan sweep).

## Write path

`OrchLifecycleHandler._write_table_registry_row()` swaps the `get_ch_client().insert()` call for:

```python
async with get_sql_session() as session:
    await session.execute(
        text(
            "INSERT INTO table_registry (table_name, job_id, task_id, run_id, created_at) "
            "VALUES (:table_name, :job_id, :task_id, :run_id, :created_at) "
            "ON CONFLICT (table_name) DO NOTHING"
        ),
        {...},
    )
    await session.commit()
```

Stays inside the same `DBLifecycleOp.OPLOG_TABLE` branch of `_process_loop` — the write remains fire-and-forget from the caller's perspective via the lifecycle queue. No advisory-lock pattern needed (PK serializes concurrent inserts; ON CONFLICT handles re-register).

## Read paths in `background_worker.py`

Four sites rewritten:

1. **`_lookup_table_owners(table_names)`** → single SQL `SELECT ... WHERE table_name IN (...)`.
2. **`_cleanup_unreferenced_tables()`** → one query that joins `table_context_refs` (anti-joined with pin/run refs) against the new `table_registry` and `jobs`, filtered by `jobs.preservation_mode != 'FULL'` in-database. Python loop reduces to "drop table + remove refs".
3. **`_delete_job_data(job_id)`** → SQL `SELECT table_name FROM table_registry WHERE job_id = :job_id` for the drop list; SQL `DELETE FROM table_registry WHERE job_id = :job_id` replaces the CH `ALTER TABLE ... DELETE`.
4. **`_cleanup_orphaned_resources(ttl_days)`** → SQL `WHERE job_id IS NULL AND created_at < :cutoff` for both the drop list and the delete.

CH client is used solely for:
- `DROP TABLE IF EXISTS <data_table>` on the actual data tables.
- `ALTER TABLE operation_log DELETE WHERE ...` for operation_log cleanup (unchanged; that table stays on CH).

## One-time data migration

Alembic only creates the SQL table. The CH → SQL row copy runs in `init_oplog_tables()` on first startup after upgrade:

1. `SHOW TABLES LIKE 'table_registry'` on CH — if absent, skip (fresh install).
2. `SELECT count() FROM table_registry` (SQL) — if non-zero, skip (already migrated).
3. Otherwise: `SELECT table_name, job_id, task_id, run_id, created_at FROM table_registry` on CH, bulk `INSERT ... ON CONFLICT DO NOTHING` into SQL.
4. `DROP TABLE IF EXISTS table_registry` on CH.

Rationale for doing this at startup (not in Alembic):

- Alembic migrations run in a sync context; the CH client is async, and chdb has no independent sync path. Doing CH I/O inside Alembic would require a sync fallback for one migration only.
- `init_oplog_tables()` already does CH DDL work at startup and has both the async CH client and the SQL session available.
- The two gate queries (`SHOW TABLES`, `count()`) make the path a cheap no-op on every subsequent startup.

## Preservation mode filter in-database

Current `_cleanup_unreferenced_tables`:

```
1. SQL: find tables with zero refs
2. CH:  lookup owners in table_registry
3. SQL: lookup preservation modes from jobs
4. Python: merge, filter, drop
```

New:

```sql
SELECT DISTINCT tcr.table_name, tr.job_id, tr.task_id, tr.run_id
FROM   table_context_refs tcr
LEFT   JOIN table_registry tr  ON tr.table_name = tcr.table_name
LEFT   JOIN jobs            j  ON j.id          = tr.job_id
WHERE  tcr.table_name NOT LIKE 'p\_%'
  AND  NOT EXISTS (SELECT 1 FROM table_pin_refs tpr WHERE tpr.table_name = tcr.table_name)
  AND  NOT EXISTS (SELECT 1 FROM table_run_refs trr WHERE trr.table_name = tcr.table_name)
  AND  (j.preservation_mode IS DISTINCT FROM 'FULL')
```

Single round-trip.

# Phases

1. **Schema** — `TableRegistry` SQLModel + Alembic migration (`upgrade`: `create_table`; `downgrade`: `drop_table`).
2. **Writer** — flip `_write_table_registry_row()` to SQL, keep queue plumbing unchanged.
3. **Readers** — rewrite the four sites in `background_worker.py`; delete `escape_sql_string` usage for registry.
4. **Backfill + CH teardown** — add backfill step to `init_oplog_tables()`, remove `TABLE_REGISTRY_DDL` / `TABLE_REGISTRY_EXPECTED_COLUMNS` / `_validate_schema("table_registry", ...)`, drop CH-side table post-copy.
5. **Tests + docs** — update `test_collector.py` to query SQL; update `docs/oplog.md`; remove the item from `docs/future.md`.

# Critical files / helpers to reuse

- `aaiclick/orchestration/sql_context.py` — `get_sql_session()` is the SQL access pattern.
- `aaiclick/orchestration/lifecycle/db_lifecycle.py` — existing ref-table SQLModels set the style.
- `aaiclick/orchestration/background/handler.py` — `in_clause()` helper for `IN (...)` parameter binding.
- `aaiclick/snowflake_id.get_snowflake_id` — no new ID generation needed; preserve existing values.

# Risks and watch-outs

- **Distributed startup race** on the backfill: two workers boot at once, both see "SQL empty + CH has rows". `ON CONFLICT DO NOTHING` makes the inserts idempotent; the final `DROP TABLE IF EXISTS` is idempotent. Safe.
- **Write hot path**: must stay inside the `DBLifecycleOp` queue — never move the SQL INSERT inline into `oplog_record_table()`.
- **Nullable `job_id`**: must remain nullable; never include in a PK or NOT NULL constraint. Orphan sweep relies on it.
- **Migration ordering**: new migration's `down_revision = "bfb0653578fb"`.
- **`created_at` on backfill**: preserves original timestamp from CH (needed so orphan TTL logic still works correctly for pre-upgrade rows).

# Verification

1. `alembic upgrade head` on a fresh SQLite DB → `table_registry` exists, zero rows.
2. `alembic upgrade head` on an existing DB with populated CH `table_registry` → no-op schema-wise; first `init_oplog_tables()` call copies rows, drops CH table.
3. `alembic downgrade -1` → drops the SQL table cleanly.
4. `pytest aaiclick/oplog` — `test_collector.py` asserts against SQL.
5. `pytest aaiclick/orchestration` — full cleanup flows (pending cleanup, unreferenced tables, expired jobs, orphaned resources) pass.
6. Smoke run: create a Job, emit a few tables, let TTL expire, confirm `background_worker` cleans up using only SQL metadata reads + CH `DROP`.
7. GitHub Actions CI passes on both the local (chdb + SQLite) and distributed backends via the `check-pr` skill.
