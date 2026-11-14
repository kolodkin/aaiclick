ClickHouse Internal Database — Design
---

# Intent

Separate aaiclick's internal ClickHouse tables from the data warehouse. Today
`AAICLICK_CH_URL` names one database holding both user objects (`t_*`, `p_*`)
and internal tables. After this change the URL's database holds **only user
objects**; internal tables live in a sibling database on the same server.

# Goals / Non-goals

- **Goal**: `task_logs`, `operation_log`, `schema_migrations` live in
  `<data_db>_internal`; the data database contains only object tables.
- **Goal**: existing installs move over in place, with no data copy.
- **Non-goal**: a separate server or URL for logs (chdb allows one session per
  process, so it would break local mode).
- **Non-goal**: per-database retention/TTL or grants — the split enables them;
  operators configure them on the server.

# Approaches

| Option                             | Trade-off                                            | Picked |
|------------------------------------|------------------------------------------------------|--------|
| A. All internal tables move        | Clean warehouse DB; touches oplog + logs + migrations | ✅      |
| B. Only `task_logs` moves          | Smallest diff; oplog still mixed with user data      |        |
| Separate server / URL              | Breaks chdb local mode                               | ruled out |

# Design

**Name**: derived, not configured — `f"{data_db}_internal"`, where `data_db`
is the URL's database (`default` for chdb). Derivation keeps the per-xdist-worker
test databases (`default_<worker>`) isolated with zero fixture work, and
several deployments sharing one server stay separate. No new env var
(*builder's call*; add one later only if someone needs a custom name).

**Access**: one helper, e.g. `internal_db()` in `aaiclick/backend.py`, and every
internal-table query qualifies its table: `{internal_db()}.task_logs`.
Same client, same session — no second connection.

**Call sites to qualify**:

- `aaiclick/orchestration/logging.py` — insert + reads of `task_logs`
- `aaiclick/oplog/lineage.py`, `aaiclick/oplog/models.py` — `operation_log`
  reads/inserts and its `system.columns` lookup
- `aaiclick/orchestration/background/background_worker.py` — the two purge
  `ALTER TABLE ... DELETE` statements
- `aaiclick/oplog/migrate.py` — `schema_migrations` DDL, reads, inserts
- `aaiclick/testing.py` — reset helpers must cover both databases

**Migration** (existing and fresh installs follow one path):

1. `ch_applied_versions` runs `CREATE DATABASE IF NOT EXISTS <internal>`. If
   `<internal>.schema_migrations` is missing but `<data>.schema_migrations`
   exists, it `RENAME`s it across first — the tracking table must move before
   the runner reads it, so this lives in code, not a `.sql` file.
2. New `0004_internal_database.sql` renames `operation_log` and `task_logs`
   into the internal database. Migration files get a `{internal_db}`
   placeholder the runner substitutes (they can't hardcode a derived name).
3. `0001`–`0003` stay unchanged: on fresh installs they create tables in the
   data DB, and `0004` moves them.

`RENAME TABLE` across databases is a metadata move on Atomic databases;
verified on chdb (`default` → `default_internal`, rows intact).

**Lineage sandbox**: unchanged — its scope is graph object tables in
`currentDatabase()`, which never included internal tables.

# Left to the builder

- Exact helper name and whether qualified names are built via `quote_identifier`.
- Test shape (follow `python-testing-style`): one migration test on a DB
  seeded with the pre-0004 layout, plus the existing suites passing.
- Docs: update `docs/designs/oplog.md`, `task_log_retirement.md`,
  `deployment.md` (grants for `<db>_internal`).
