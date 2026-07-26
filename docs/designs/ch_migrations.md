ClickHouse Migration Framework
---

A dbmate-inspired, forward-only migration runner for aaiclick's internal
ClickHouse tables (`operation_log`, `task_logs`, and future internal tables).
Replaces the `CREATE TABLE IF NOT EXISTS` + column-validator approach in
`aaiclick/oplog/models.py`, which cannot alter existing installs.

Tracked in `docs/designs/future.md` ("ClickHouse Migration Framework") until
implemented; remove that entry when this lands.

---

# Problem

ClickHouse tables are created with `CREATE TABLE IF NOT EXISTS` on task-scope
entry. Any DDL change in the Python source is silently a no-op on installs
where the table already exists — this has already bitten the `operation_log`
`ORDER BY` change. The column validator (`_validate_schema`) detects some
drift but its only remedy is "drop the table", which loses data.

The SQL side has Alembic; the ClickHouse side has nothing.

---

# Design decisions

| Decision       | Choice                                                            |
|----------------|-------------------------------------------------------------------|
| Scope          | aaiclick-internal CH tables only; runtime `p_*`/`t_*`/`j_*` data tables are out of scope |
| File format    | Pure SQL files, no Python migrations                              |
| Direction      | Forward-only — no down migrations; a bad migration is fixed by a new forward one |
| Versioning     | Sequential zero-padded numbers (`0001`, `0002`, …), not timestamps — single-repo authorship makes collisions a PR-review concern |
| Unification    | The existing `aaiclick migrate` command drives Alembic and the CH runner together |
| Baseline       | `0001_baseline.sql` recreates today's schema with `CREATE TABLE IF NOT EXISTS` — safe on both fresh and existing installs, no stamp/detect logic |
| Validation     | Version tracking replaces column validation; `_validate_schema` and the `EXPECTED_COLUMNS` dicts are deleted |
| Startup, local | Auto-apply pending migrations (zero-ops, single-process — mirrors SQLite) |
| Startup, distributed | Never applies; raises "schema behind, run `aaiclick migrate upgrade`" (mirrors Postgres) |

The migration history is the schema's source of truth — hand-edited drift is
no longer auto-detected, the same contract Alembic gives the SQL side.

---

# Migration files

Location: `aaiclick/oplog/migrations/NNNN_description.sql`, mirroring the
Alembic layout under `aaiclick/orchestration/migrations/`.

- The whole file is the up script — no `-- migrate:up` marker, since there is
  no down section.
- Statements are separated by `;` at end of line. `--` line comments are
  allowed and stripped before execution.
- Write scripts idempotently where ClickHouse allows (`IF NOT EXISTS`,
  `IF EXISTS`) — see failure recovery under Runner.

`0001_baseline.sql` contains the current `OPERATION_LOG_DDL` and
`TASK_LOGS_DDL` verbatim.

---

# Tracking table

`schema_migrations` in the target ClickHouse database (dbmate's name):

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    String,
    applied_at DateTime64(3)
) ENGINE = MergeTree ORDER BY version
```

A version is the file's `NNNN` prefix. A migration is applied iff its version
has a row.

---

# Runner

`aaiclick/oplog/migrate.py` — async, built on the `ChClient` protocol so chdb
and remote ClickHouse behave identically.

- `ch_pending(ch_client) -> list[str]` — ensures `schema_migrations` exists,
  returns versions present on disk but not applied. Raises on gaps (an
  unapplied version older than an applied one) and on applied versions with no
  matching file.
- `ch_upgrade(ch_client, dry_run=False) -> list[str]` — applies pending files
  in order: strip comments, split statements, execute each via
  `ch_client.command()`, then insert the version row. Returns the versions
  applied. `dry_run=True` returns the pending versions and logs their
  statements without executing anything.

ClickHouse has no DDL transactions: a mid-script failure leaves the version
unrecorded, and re-running the (idempotent) script after fixing it completes
the migration.

Concurrency: no locking. Local mode is single-process; in distributed mode
`aaiclick migrate` is an operator-run one-shot, and startup never writes.

---

# `aaiclick migrate` integration

In `aaiclick/internal_api/setup.py` (`migrate()`):

| Action                   | Behavior                                                     |
|--------------------------|--------------------------------------------------------------|
| `upgrade`                | Alembic upgrade, then `ch_upgrade()`                         |
| `current`                | Alembic current + latest applied CH version                  |
| `history`                | Alembic history + all CH versions with applied/pending state |
| `downgrade`/`show`/`heads` | Alembic-only, unchanged                                    |

`MigrationResult` grows `ch_versions_applied: list[str]`.

---

# Startup

`init_oplog_tables(ch_client)` in `aaiclick/oplog/models.py` becomes:

- Local mode (`is_local()`): `await ch_upgrade(ch_client)` — a no-op costing
  one `SELECT` on `schema_migrations` when nothing is pending.
- Distributed mode: `await ch_pending(ch_client)`; if non-empty, raise
  `RuntimeError("ClickHouse schema is behind (pending: 0003, 0004). Run: aaiclick migrate upgrade")`.

`_validate_schema`, `OPERATION_LOG_EXPECTED_COLUMNS`, and
`TASK_LOGS_EXPECTED_COLUMNS` are deleted. The DDL constants move into
`0001_baseline.sql`; call sites that imported them (e.g.
`orchestration/orch_context.py`, `orchestration/execution/log_flush.py`) are
updated accordingly.

---

# Testing

Per `docs/designs/testing.md` and the `python-testing-style` skill.

- Unit (no DB): filename parsing and ordering, statement splitting, comment
  stripping, gap and unknown-version detection.
- chdb-backed: fresh apply records all versions; second run applies nothing;
  baseline over pre-existing tables succeeds; mid-script failure leaves the
  version unrecorded and a re-run completes it; distributed-mode startup with
  pending migrations raises the run-migrate error (mode simulated, not a real
  remote server).

---

# Out of scope

Recorded here so they are deliberate omissions, not gaps:

- Down migrations / rollback.
- Python migration files and shadow-table rebuild helpers (revisit when a
  migration actually needs an `ALTER` ClickHouse cannot express; see the
  execution-strategy note in `docs/designs/future.md`).
- A `migrate new` scaffolding command.
- Adopting Alembic for local SQLite (today: `create_all` at setup) — separate
  discussion.
