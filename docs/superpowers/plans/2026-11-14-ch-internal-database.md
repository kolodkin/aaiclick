# ClickHouse Internal Database Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task (CLAUDE.md: always inline). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `operation_log`, `task_logs`, `schema_migrations` into a sibling `<data_db>_internal` ClickHouse database so the `AAICLICK_CH_URL` database holds only user objects.

**Architecture:** One helper derives the internal database name from the CH URL; every internal-table query qualifies its table through it. The CH migration runner creates the database and substitutes an `{internal_db}` placeholder in a single squashed baseline. Same client, same session.

**Tech Stack:** Python 3, ClickHouse / chdb, clickhouse-connect, pytest (async).

**Spec:** `docs/superpowers/specs/2026-11-14-ch-internal-database-design.md`

## Global Constraints

- Internal database name: `f"{data_db}_internal"`; `data_db` is `"default"` under chdb, else `parse_ch_url()["database"]`.
- No env var, no upgrade path, no backward compatibility; `0002`/`0003` are deleted.
- Imports at top of file; no `Any`; tests follow `python-testing-style`.
- Run tests with the default (chdb + SQLite) backend: `uv run pytest <path> -x -q`.

## Review Focus

- `ch_client.insert` with a qualified name on **real** ClickHouse (clickhouse-connect) — must land in the internal DB, not error on the dot. Covered by CI's distributed job running `test_tasks_logs.py` / `test_collector.py`; if it fails, split into `database=` + `table=` in `ClickHouseClient.insert`.
- Per-xdist-worker real-CH databases: `default_gw0_internal` must be created by `ch_upgrade` and dropped in `worker_ch_database` teardown, or workers leak/collide.
- Test reset must truncate internal tables too, else logs/oplog rows bleed between tests (Task 4 test).
- Data DB after `ch_upgrade` contains no tables at all (Task 2 test).
- Purge on job expiry still deletes from the internal tables (existing `test_cleanup.py` tests, re-pointed in Task 3).

---

### Task 1: Name helpers

**Files:**
- Modify: `aaiclick/backend.py`
- Test: `aaiclick/test_backend.py` (create if absent; else add to existing backend test file)

**Interfaces:**
- Produces: `internal_db() -> str`, `internal_table(name: str) -> str` returning `` `<db>`.`<name>` `` (backtick-quoted both parts).

- [ ] **Step 1: Write the failing test** — parametrize over URLs:

```python
@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("chdb:///tmp/x/chdb_data", "default_internal"),
        ("clickhouse://u:p@h:8123/warehouse", "warehouse_internal"),
        ("clickhouse://h:8123", "default_internal"),
    ],
)
def test_internal_db(monkeypatch, url, expected):
    monkeypatch.setenv("AAICLICK_CH_URL", url)
    assert internal_db() == expected
    assert internal_table("task_logs") == f"`{expected}`.`task_logs`"
```

- [ ] **Step 2:** Run `uv run pytest aaiclick/test_backend.py -x -q` → FAIL (ImportError).
- [ ] **Step 3:** Implement both functions in `aaiclick/backend.py` below `parse_ch_url`; read the URL at call time (no module-level caching).
- [ ] **Step 4:** Re-run → PASS.
- [ ] **Step 5:** Commit `feat: internal CH database name helpers`.

### Task 2: Migrations into the internal database

**Files:**
- Modify: `aaiclick/oplog/migrations/0001_baseline.sql` (final schema, placeholder-qualified)
- Delete: `aaiclick/oplog/migrations/0002_utc_timestamps.sql`, `0003_drop_task_logs_level.sql`
- Modify: `aaiclick/oplog/migrate.py`, `aaiclick/oplog/models.py`
- Test: `aaiclick/oplog/test_migrate.py`

**Interfaces:**
- Consumes: `internal_db()`, `internal_table()` (Task 1).
- Produces: after `ch_upgrade(ch)` the internal DB holds `operation_log`, `task_logs`, `schema_migrations`; `get_column_types` reads `system.columns WHERE database = '<internal_db>'`.

- [ ] **Step 1: Update tests in `test_migrate.py`:**
  - Every raw SQL reference to the three tables uses `internal_table(...)` (drops, creates, inserts, `EXISTS TABLE`).
  - Expected shipped versions are `["0001"]` wherever tests assert on the shipped set.
  - New test:

```python
async def test_upgrade_leaves_data_database_empty(orch_ctx):
    ch = get_ch_client()
    await ch_upgrade(ch)
    assert await list_ch_tables(ch) == set()
    for table in ("operation_log", "task_logs", "schema_migrations"):
        assert (await ch.query(f"EXISTS TABLE {internal_table(table)}")).result_rows[0][0] == 1
```

- [ ] **Step 2:** Run `uv run pytest aaiclick/oplog/test_migrate.py -x -q` → FAIL.
- [ ] **Step 3: Implement:**
  - `0001_baseline.sql`: `CREATE TABLE IF NOT EXISTS {internal_db}.operation_log` / `{internal_db}.task_logs` with `created_at DateTime64(3, 'UTC')`, no `level` column; keep header comment short.
  - `migrate.py`: `ch_applied_versions` first runs `CREATE DATABASE IF NOT EXISTS` on the backticked `internal_db()`; `SCHEMA_MIGRATIONS_DDL`, its `SELECT`, and the version `INSERT` use `internal_table("schema_migrations")`. `ch_upgrade` substitutes `` `{internal_db()}` `` for the literal `{internal_db}` in file text (`str.replace`, not `format` — SQL may contain braces).
  - `models.py`: `get_column_types` filters on `database = '<internal_db()>'` via `quote_sql_literal`.
- [ ] **Step 4:** Re-run → PASS.
- [ ] **Step 5:** Commit `feat: create internal CH tables in <db>_internal`.

### Task 3: Qualify runtime call sites

**Files:**
- Modify: `aaiclick/orchestration/logging.py` (insert ~221, reads ~240, ~257)
- Modify: `aaiclick/orchestration/orch_context.py` (~232 oplog insert)
- Modify: `aaiclick/oplog/lineage.py` (~185, ~219, ~237)
- Modify: `aaiclick/orchestration/background/background_worker.py` (~366 purge loop, ~438 orphan delete)
- Test: `aaiclick/orchestration/background/test_cleanup.py`, `aaiclick/oplog/test_collector.py`

**Interfaces:**
- Consumes: `internal_table()` (Task 1). `get_column_types` keeps taking the **bare** name (`"task_logs"`); only SQL text and `insert` targets get qualified.

- [ ] **Step 1:** Re-point raw SQL in `test_cleanup.py` (lines ~293–323, incl. the `throwIf` replacement table) and `test_collector.py` (~27, ~92) to `internal_table(...)`.
- [ ] **Step 2:** Run both files → FAIL (tables not found in data DB).
- [ ] **Step 3:** Replace each hardcoded table name in the listed modules with `internal_table(...)`; inserts pass `internal_table("task_logs")` / `internal_table("operation_log")` as the table argument.
- [ ] **Step 4:** Run `uv run pytest aaiclick/orchestration aaiclick/oplog aaiclick/internal_api -x -q` → PASS.
- [ ] **Step 5:** `grep -rnE "(FROM|INTO|TABLE) +(operation_log|task_logs|schema_migrations)\b" aaiclick --include=*.py` → no hits.
- [ ] **Step 6:** Commit `feat: read and write internal CH tables via <db>_internal`.

### Task 4: Test harness covers both databases

**Files:**
- Modify: `aaiclick/testing.py` (`drop_all_ch_tables`, `_matches_migrated_schema`, `worker_ch_database`)
- Test: `aaiclick/test_testing.py` (or the existing test module for `testing.py`; locate with `grep -rln drop_all_ch_tables aaiclick/*test*`)

**Interfaces:**
- Consumes: `internal_db()`, `internal_table()`.
- `list_ch_tables` / `ch_table_ddl` keep meaning "data DB" (tests use them for object tables).

- [ ] **Step 1: Write the failing test:**

```python
async def test_reset_truncates_internal_tables(orch_ctx):
    ch = get_ch_client()
    await drop_all_ch_tables()
    await ch.command(
        f"INSERT INTO {internal_table('task_logs')} (task_id, job_id, run_id, seq, stream, line, created_at) "
        "VALUES (1, 1, 1, 0, 'stdout', 'x', now64(3))"
    )
    await drop_all_ch_tables()
    assert (await ch.query(f"SELECT count() FROM {internal_table('task_logs')}")).result_rows[0][0] == 0
```

- [ ] **Step 2:** Run it → FAIL (row survives).
- [ ] **Step 3: Implement:** the migrated-schema snapshot and the drop/truncate sweep cover both `currentDatabase()` and `internal_db()`, keyed by qualified name; `schema_migrations` stays excluded from truncation; full-rebuild path unchanged otherwise. `worker_ch_database` teardown also drops `` `{db_name}_internal` ``.
- [ ] **Step 4:** Run full suite `uv run pytest -x -q -n auto` → PASS.
- [ ] **Step 5:** Commit `test: reset internal CH database between tests`.

### Task 5: Docs and cleanup

**Files:**
- Modify: `docs/designs/oplog.md`, `docs/designs/task_log_retirement.md`, `docs/designs/deployment.md` (grant note for `<db>_internal`), plus any other of `docs/designs/{api_server,auth,frontend,java-sdk,kubernetes_runner,orchestration,ui}.md`, `docs/user_guide/orchestration.md` whose text says these tables share the data DB.
- Delete: this plan and the spec.

- [ ] **Step 1:** Edit docs per `markdown-style` skill: one short section in `oplog.md` stating the layout (`<data_db>` = user objects only, `<data_db>_internal` = `operation_log`, `task_logs`, `schema_migrations`).
- [ ] **Step 2:** Delete `docs/superpowers/plans/2026-11-14-ch-internal-database.md` and `docs/superpowers/specs/2026-11-14-ch-internal-database-design.md`; `grep -rn "2026-11-14-ch-internal" .` → no hits.
- [ ] **Step 3:** Commit `docs: internal CH database`, push, run `check-pr`.
