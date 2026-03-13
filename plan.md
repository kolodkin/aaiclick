# Plan: Dual-Backend Support (Local Dev vs Distributed)

## Overview

Add env-var driven backend selection:
- **Local dev** (default): SQLite + chdb — zero infrastructure, instant setup
- **Distributed** (production): PostgreSQL + ClickHouse — current behavior

Single env var `AAICLICK_BACKEND=local|distributed` (default: `local`).

---

## Phase 1: Backend Selection & Env Vars

### 1a. New backend config module: `aaiclick/backend.py`

- Read `AAICLICK_BACKEND` env var (default `"local"`)
- Export `is_local() -> bool` helper
- When `local`: no PG/CH env vars needed, everything is in-process

### 1b. Update `aaiclick/orchestration/env.py`

- When `local`: return `sqlite+aiosqlite:///path/to/local.db` from `get_db_url()` (rename from `get_pg_url()`)
- When `distributed`: current PostgreSQL URL behavior unchanged
- Default SQLite path: `~/.aaiclick/local.db`

### 1c. Update `aaiclick/orchestration/context.py`

- Use `get_db_url()` instead of `get_pg_url()`
- No other changes needed — SQLAlchemy engine is dialect-agnostic

### 1d. Update `aaiclick/data/env.py` and `aaiclick/data/data_context.py`

- When `local`: `_create_ch_client()` returns a chdb-backed client adapter instead of `clickhouse-connect` AsyncClient
- Adapter wraps `chdb.session.Session` with the same interface used by data_context: `command()`, `query()`, `insert()`
- New file: `aaiclick/data/chdb_client.py` — thin adapter class

### 1e. Update `CLAUDE.md` env vars section

- Document `AAICLICK_BACKEND` and new defaults

---

## Phase 2: SQLite Orchestration Adapter

### 2a. `DbHandler` abstraction for backend-specific SQL

New file: `aaiclick/orchestration/db_handler.py`

- Define `DbHandler` Protocol with static methods for backend-specific operations
- `PgDbHandler` — writable CTEs, `FOR UPDATE SKIP LOCKED`, `.with_for_update()`
- `SqliteDbHandler` — sequential SELECT + UPDATE, no row locking (single-writer)

Methods:
- `claim_next_task(session, worker_id, now)` — full claim logic (PG: writable CTE, SQLite: sequential)
- `lock_query(query)` — PG: `.with_for_update()`, SQLite: no-op (returns query unchanged)

All methods are static — no instance state needed.

### 2b. Store handler in `OrchCtxState`

- Add `db_handler: PgDbHandler | SqliteDbHandler` to `OrchCtxState`
- Initialize in `orch_context()` based on `is_local()`
- Add `get_db_handler()` helper to retrieve from context

### 2c. Simplify `claiming.py`

- Remove all `if is_local()` / `if not is_local()` checks
- `claim_next_task()` delegates to `handler.claim_next_task(session, ...)`
- `update_task_status()`, `update_job_status()`, `cancel_job()` use `handler.lock_query(query)`
- Remove `from aaiclick.backend import is_local` from claiming.py

### 2b. Update `pg_lifecycle.py` → `db_lifecycle.py`

- `ON CONFLICT ... DO UPDATE` works on both SQLite and PG — no changes to SQL needed
- Rename file to reflect it's no longer PG-specific

### 2c. SQLite migration strategy

- For `local` backend: use `SQLModel.metadata.create_all(engine)` instead of Alembic
- Alembic migrations remain for `distributed` backend only
- Simple and correct — local dev doesn't need migration history

### 2d. Update `conftest.py` for orchestration tests

- When `local` backend: use in-memory SQLite (`sqlite+aiosqlite://`) — no psycopg2 setup/teardown
- When `distributed` backend: current PG isolation per xdist worker

---

## Phase 3: chdb Client Adapter

### 3a. New file: `aaiclick/data/chdb_client.py`

Adapter class that wraps `chdb.session.Session` to match the `clickhouse-connect` `AsyncClient` interface used in the codebase. Methods needed (based on `data_context.py` usage):

- `command(query)` → `session.query(query)` (DDL, INSERT with VALUES)
- `query(query)` → `session.query(query)` returning result with `.result_rows` and `.column_names`
- `insert(table, data, column_names)` → generate `INSERT INTO ... VALUES ...` from data

All sync under the hood (chdb is embedded), wrapped in async interface.

### 3b. Update `_create_ch_client()` in `data_context.py`

- When `local`: return `ChdbClient()` instance
- When `distributed`: current `get_async_client()` behavior

---

## Phase 4: Setup Script & Local Dev Experience

### 4a. Setup command: `python -m aaiclick setup`

- For `local` backend: create `~/.aaiclick/` dir, initialize SQLite DB with `create_all()`, verify chdb import
- For `distributed` backend: verify PG/CH connectivity, run Alembic migrations
- Idempotent — safe to run multiple times

### 4b. Update `CLAUDE.md`

- Add section: "Local Development Setup" with `python -m aaiclick setup`
- Update "Test Execution Strategy" — tests CAN run locally now with `local` backend
- Agent guidelines: run `python -m aaiclick setup` before first test, run tests locally before commit

---

## Phase 4.5: Pull Main & Run Local Tests

Before moving to CI/CD, ensure everything integrates cleanly with the latest main branch.

### 4.5a. Pull latest main

- `git pull origin main` to merge any upstream changes
- Resolve conflicts if any

### 4.5b. Run local tests

- `python -m aaiclick setup` (ensure local backend is initialized)
- `AAICLICK_BACKEND=local pytest aaiclick/` — run full test suite with local backend
- Fix any failures before proceeding to CI/CD phase

---

## Phase 5: CI/CD Matrix

### 5a. Update `.github/workflows/test.yaml`

Add backend dimension to test matrix:

```yaml
matrix:
  include:
    # Local backend — no services needed
    - group: data-local
      title: Data Tests (Local/chdb)
      backend: local
      install: ".[test,local]"
      test-paths: "aaiclick/data/ aaiclick/test_snowflake.py"
      needs-postgres: false
      needs-clickhouse: false

    - group: orch-local
      title: Orchestration Tests (Local/SQLite)
      backend: local
      install: ".[orch,test,local]"
      test-paths: "aaiclick/orchestration/"
      needs-postgres: false
      needs-clickhouse: false

    # Distributed backend — current behavior
    - group: data
      title: Data Tests (Distributed)
      backend: distributed
      install: ".[test]"
      test-paths: "aaiclick/data/ aaiclick/test_snowflake.py aaiclick/benchmarks/"
      needs-postgres: false
      needs-clickhouse: true

    - group: orch
      title: Orchestration Tests (Distributed)
      backend: distributed
      install: ".[orch,test]"
      test-paths: "aaiclick/orchestration/"
      needs-postgres: true
      needs-clickhouse: true
```

- Local matrix entries: no `services:` block needed, set `AAICLICK_BACKEND=local`
- Distributed entries: current services block, set `AAICLICK_BACKEND=distributed`
- Conditionally start services based on `needs-postgres`/`needs-clickhouse`

### 5b. Add `[local]` extra to `pyproject.toml`

```toml
[project.optional-dependencies]
local = ["chdb", "aiosqlite"]
```

---

## Dependencies to Add

| Package     | Purpose                        | Extra group |
|-------------|--------------------------------|-------------|
| `chdb`      | Embedded ClickHouse            | `local`     |
| `aiosqlite` | Async SQLite driver for SQLAlchemy | `local` |

---

## Files Changed Summary

| File | Change |
|------|--------|
| `aaiclick/backend.py` | **New** — backend detection |
| `aaiclick/data/chdb_client.py` | **New** — chdb adapter |
| `aaiclick/data/env.py` | Conditional CH creds vs chdb |
| `aaiclick/data/data_context.py` | `_create_ch_client()` dispatches by backend |
| `aaiclick/orchestration/env.py` | `get_db_url()` returns PG or SQLite URL |
| `aaiclick/orchestration/context.py` | Use `get_db_url()` |
| `aaiclick/orchestration/db_handler.py` | **New** — `PgDbHandler` / `SqliteDbHandler` with shared interface |
| `aaiclick/orchestration/claiming.py` | Simplified — delegates to `db_handler` from context |
| `aaiclick/orchestration/pg_lifecycle.py` | Rename to `db_lifecycle.py`, update imports |
| `aaiclick/orchestration/conftest.py` | SQLite path for local tests |
| `aaiclick/__main__.py` | Add `setup` subcommand |
| `pyproject.toml` | Add `[local]` extra |
| `.github/workflows/test.yaml` | Matrix with local/distributed |
| `CLAUDE.md` | Updated guidelines |
