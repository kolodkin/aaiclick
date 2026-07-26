# ClickHouse Migration Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A dbmate-inspired, forward-only SQL migration runner for aaiclick's internal ClickHouse tables, wired into the existing `aaiclick migrate` command and startup path.

**Architecture:** Numbered `NNNN_*.sql` files under `aaiclick/oplog/migrations/` are applied in order by an async runner (`ch_pending` / `ch_upgrade`) that records applied versions in a `schema_migrations` ClickHouse table. `init_oplog_tables` auto-applies in local mode and raises "run `aaiclick migrate upgrade`" in distributed mode. The sync `internal_api.setup.migrate()` bridges to the async runner via `asyncio.run`.

**Tech Stack:** Python 3.12 async, `ChClient` protocol (chdb + clickhouse-connect), pytest with the repo's `orch_ctx` fixture family, Alembic (untouched, composed with).

**Spec:** `docs/designs/ch_migrations.md` — read it before starting any task.

## Global Constraints

- All imports at top of file; three groups (stdlib / external / `aaiclick`). No inline imports except documented circular-dep last resorts.
- No `Any` typing shortcuts. Prefer `NamedTuple` over plain tuples in APIs. Prefer `Literal` over enums.
- No `__all__` in `__init__.py`.
- Tests follow the `python-testing-style` skill (read it in every task that writes tests): flat structure, async tests without `@pytest.mark.asyncio` (asyncio_mode=auto), no testing of trivia.
- Migration `.sql` scripts must be idempotent (`IF NOT EXISTS` / `IF EXISTS`) where ClickHouse allows.
- Forward-only: no down-migration support anywhere.
- Commit after every task with the repo's footer convention (committer `Claude <noreply@anthropic.com>`).
- Run the full check before each commit: `uv run pytest aaiclick/oplog/ -x -q` at minimum; the final task runs the wider suite.

---

### Task 1: Runner pure functions — file discovery and statement splitting

**Files:**
- Create: `aaiclick/oplog/migrate.py`
- Test: `aaiclick/oplog/test_migrate.py`

**Interfaces:**
- Produces:
  - `class MigrationFile(NamedTuple): version: str; path: Path` — `version` is the zero-padded `NNNN` prefix.
  - `list_migration_files(migrations_dir: Path) -> list[MigrationFile]` — sorted by version; raises `ValueError` on duplicate versions or files not matching `NNNN_name.sql`.
  - `split_statements(sql: str) -> list[str]` — strips `--` line comments, splits on `;` at end of line, drops blanks.
  - `MIGRATIONS_DIR: Path` — `Path(__file__).parent / "migrations"` module constant, the default for later tasks.

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for the ClickHouse migration runner's pure file/SQL handling."""

from __future__ import annotations

from pathlib import Path

import pytest

from aaiclick.oplog.migrate import MigrationFile, list_migration_files, split_statements


def _write(tmp_path: Path, name: str, content: str = "SELECT 1;") -> Path:
    path = tmp_path / name
    path.write_text(content)
    return path


def test_list_migration_files_sorted_by_version(tmp_path):
    _write(tmp_path, "0002_add_column.sql")
    _write(tmp_path, "0001_baseline.sql")

    files = list_migration_files(tmp_path)

    assert [f.version for f in files] == ["0001", "0002"]
    assert files[0] == MigrationFile(version="0001", path=tmp_path / "0001_baseline.sql")


def test_list_migration_files_empty_dir(tmp_path):
    assert list_migration_files(tmp_path) == []


def test_list_migration_files_rejects_duplicate_versions(tmp_path):
    _write(tmp_path, "0001_a.sql")
    _write(tmp_path, "0001_b.sql")

    with pytest.raises(ValueError, match="duplicate"):
        list_migration_files(tmp_path)


def test_list_migration_files_rejects_bad_names(tmp_path):
    _write(tmp_path, "baseline.sql")

    with pytest.raises(ValueError, match="baseline.sql"):
        list_migration_files(tmp_path)


def test_split_statements_on_trailing_semicolon():
    sql = "CREATE TABLE a (x UInt64) ENGINE = MergeTree ORDER BY x;\nCREATE TABLE b (y String) ENGINE = Memory;\n"

    assert split_statements(sql) == [
        "CREATE TABLE a (x UInt64) ENGINE = MergeTree ORDER BY x",
        "CREATE TABLE b (y String) ENGINE = Memory",
    ]


def test_split_statements_strips_comments_and_blanks():
    sql = "-- create the table\nCREATE TABLE a (\n    x UInt64  -- the key\n) ENGINE = Memory;\n\n"

    assert split_statements(sql) == ["CREATE TABLE a (\n    x UInt64\n) ENGINE = Memory"]


def test_split_statements_last_statement_without_semicolon():
    assert split_statements("SELECT 1") == ["SELECT 1"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/oplog/test_migrate.py -v`
Expected: FAIL — `ModuleNotFoundError` / `ImportError` for `aaiclick.oplog.migrate`.

- [ ] **Step 3: Write the implementation**

```python
"""
aaiclick.oplog.migrate - dbmate-inspired forward-only migration runner for
aaiclick's internal ClickHouse tables.

Migration scripts live in ``aaiclick/oplog/migrations/NNNN_description.sql``
and are applied in version order; applied versions are recorded in the
``schema_migrations`` ClickHouse table. See ``docs/designs/ch_migrations.md``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import NamedTuple

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

_FILENAME_RE = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")
_COMMENT_RE = re.compile(r"--.*$", re.MULTILINE)


class MigrationFile(NamedTuple):
    version: str
    path: Path


def list_migration_files(migrations_dir: Path) -> list[MigrationFile]:
    """Return migration files sorted by version.

    Raises ``ValueError`` for files not named ``NNNN_description.sql`` and
    for duplicate ``NNNN`` versions.
    """
    files: list[MigrationFile] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        match = _FILENAME_RE.match(path.name)
        if match is None:
            raise ValueError(f"Migration file {path.name} does not match NNNN_description.sql")
        files.append(MigrationFile(version=match.group(1), path=path))

    versions = [f.version for f in files]
    if len(versions) != len(set(versions)):
        raise ValueError(f"duplicate migration versions in {migrations_dir}: {versions}")
    return files


def split_statements(sql: str) -> list[str]:
    """Split a migration script into statements on ``;`` at end of line.

    ``--`` line comments are stripped first, so a semicolon inside a comment
    never splits. Semicolons inside string literals are not supported —
    internal DDL scripts don't need them.
    """
    without_comments = _COMMENT_RE.sub("", sql)
    statements = re.split(r";\s*$", without_comments, flags=re.MULTILINE)
    return [stmt.strip().rstrip(";").strip() for stmt in statements if stmt.strip()]
```

Note: after stripping a trailing comment, `CREATE TABLE a (\n    x UInt64  -- the key\n)` leaves trailing spaces before the newline — the test expects `x UInt64\n`. Normalize per-line trailing whitespace after comment stripping:

```python
    without_comments = "\n".join(line.rstrip() for line in _COMMENT_RE.sub("", sql).splitlines())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest aaiclick/oplog/test_migrate.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/oplog/migrate.py aaiclick/oplog/test_migrate.py
git commit -m "Add CH migration file discovery and statement splitting"
```

---

### Task 2: Async runner — `schema_migrations`, `ch_pending`, `ch_upgrade`

**Files:**
- Modify: `aaiclick/oplog/migrate.py`
- Test: `aaiclick/oplog/test_migrate.py` (append)

**Interfaces:**
- Consumes: Task 1's `list_migration_files`, `split_statements`, `MIGRATIONS_DIR`; `ChClient` protocol from `aaiclick.data.data_context` (`command()`, `query()` → `.result_rows`).
- Produces:
  - `ch_pending(ch_client: ChClient, migrations_dir: Path = MIGRATIONS_DIR) -> list[str]`
  - `ch_upgrade(ch_client: ChClient, migrations_dir: Path = MIGRATIONS_DIR, dry_run: bool = False) -> list[str]`
  - `ch_applied_versions(ch_client: ChClient) -> list[str]` — sorted applied versions (used by `migrate current`/`history` in Task 5).

- [ ] **Step 1: Write the failing tests**

Append to `aaiclick/oplog/test_migrate.py`. These use the repo-global `orch_ctx` fixture (chdb) and `get_ch_client()`, the same pattern as `aaiclick/oplog/test_collector.py`. Add to the imports at top of file:

```python
from aaiclick.data.data_context.ch_client import get_ch_client
from aaiclick.oplog.migrate import ch_applied_versions, ch_pending, ch_upgrade
```

```python
async def test_ch_upgrade_applies_pending_in_order(orch_ctx, tmp_path):
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;")
    _write(
        tmp_path,
        "0002_b.sql",
        "CREATE TABLE IF NOT EXISTS mig_b (y UInt64) ENGINE = Memory;\nINSERT INTO mig_b VALUES (1);",
    )
    ch = get_ch_client()

    applied = await ch_upgrade(ch, migrations_dir=tmp_path)

    assert applied == ["0001", "0002"]
    assert await ch_pending(ch, migrations_dir=tmp_path) == []
    assert await ch_applied_versions(ch) == ["0001", "0002"]
    rows = (await ch.query("SELECT y FROM mig_b")).result_rows
    assert rows == [(1,)]


async def test_ch_upgrade_second_run_is_noop(orch_ctx, tmp_path):
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;")
    ch = get_ch_client()
    await ch_upgrade(ch, migrations_dir=tmp_path)

    assert await ch_upgrade(ch, migrations_dir=tmp_path) == []


async def test_ch_upgrade_applies_only_new_versions(orch_ctx, tmp_path):
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;")
    ch = get_ch_client()
    await ch_upgrade(ch, migrations_dir=tmp_path)
    _write(tmp_path, "0002_b.sql", "CREATE TABLE IF NOT EXISTS mig_b (y UInt64) ENGINE = Memory;")

    assert await ch_upgrade(ch, migrations_dir=tmp_path) == ["0002"]


async def test_ch_pending_rejects_gap(orch_ctx, tmp_path):
    """An unapplied version older than an applied one means history was rewritten."""
    _write(tmp_path, "0002_b.sql", "CREATE TABLE IF NOT EXISTS mig_b (y UInt64) ENGINE = Memory;")
    ch = get_ch_client()
    await ch_upgrade(ch, migrations_dir=tmp_path)
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;")

    with pytest.raises(RuntimeError, match="0001"):
        await ch_pending(ch, migrations_dir=tmp_path)


async def test_ch_pending_rejects_unknown_applied_version(orch_ctx, tmp_path):
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;")
    ch = get_ch_client()
    await ch_upgrade(ch, migrations_dir=tmp_path)
    (tmp_path / "0001_a.sql").unlink()

    with pytest.raises(RuntimeError, match="0001"):
        await ch_pending(ch, migrations_dir=tmp_path)


async def test_ch_upgrade_mid_script_failure_leaves_version_unrecorded(orch_ctx, tmp_path):
    _write(
        tmp_path,
        "0001_a.sql",
        "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;\nSELECT broken syntax here;",
    )
    ch = get_ch_client()

    with pytest.raises(Exception):
        await ch_upgrade(ch, migrations_dir=tmp_path)

    assert await ch_applied_versions(ch) == []
    # Fix the script; the idempotent first statement re-runs harmlessly.
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_a (x UInt64) ENGINE = Memory;")
    assert await ch_upgrade(ch, migrations_dir=tmp_path) == ["0001"]


async def test_ch_upgrade_dry_run_executes_nothing(orch_ctx, tmp_path):
    _write(tmp_path, "0001_a.sql", "CREATE TABLE IF NOT EXISTS mig_dry (x UInt64) ENGINE = Memory;")
    ch = get_ch_client()

    pending = await ch_upgrade(ch, migrations_dir=tmp_path, dry_run=True)

    assert pending == ["0001"]
    assert await ch_applied_versions(ch) == []
    exists = (await ch.query("EXISTS TABLE mig_dry")).result_rows
    assert exists[0][0] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/oplog/test_migrate.py -v`
Expected: new tests FAIL with `ImportError` (`ch_pending` etc. undefined); Task 1 tests still PASS.

- [ ] **Step 3: Write the implementation**

Append to `aaiclick/oplog/migrate.py`. Add to the imports: `import logging`, `from aaiclick.data.data_context import ChClient` (external/`aaiclick` group placement per Global Constraints), and `from aaiclick.datetime_utils import utc_now` — check that helper exists first (`grep -n "def utc_now" aaiclick/datetime_utils.py`); if it doesn't, use `datetime.now(timezone.utc)` with a top-of-file `from datetime import datetime, timezone`.

```python
logger = logging.getLogger(__name__)

SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    String,
    applied_at DateTime64(3)
) ENGINE = MergeTree ORDER BY version
"""


async def ch_applied_versions(ch_client: ChClient) -> list[str]:
    """Ensure ``schema_migrations`` exists and return applied versions, sorted."""
    await ch_client.command(SCHEMA_MIGRATIONS_DDL)
    result = await ch_client.query("SELECT version FROM schema_migrations ORDER BY version")
    return [row[0] for row in result.result_rows]


async def ch_pending(ch_client: ChClient, migrations_dir: Path = MIGRATIONS_DIR) -> list[str]:
    """Return versions present on disk but not yet applied, in apply order.

    Raises ``RuntimeError`` when the applied set is inconsistent with the
    files on disk: an applied version with no file, or an unapplied file
    older than an applied version (a gap — history was rewritten).
    """
    files = list_migration_files(migrations_dir)
    file_versions = [f.version for f in files]
    applied = await ch_applied_versions(ch_client)

    unknown = sorted(set(applied) - set(file_versions))
    if unknown:
        raise RuntimeError(
            f"schema_migrations contains versions with no migration file: {unknown}. "
            f"The installed code is older than the database schema."
        )

    pending = [v for v in file_versions if v not in set(applied)]
    if applied and pending:
        newest_applied = applied[-1]
        gaps = [v for v in pending if v < newest_applied]
        if gaps:
            raise RuntimeError(
                f"Migration gap: versions {gaps} are unapplied but older than "
                f"already-applied {newest_applied}. Renumber the new migration(s) past {newest_applied}."
            )
    return pending


async def ch_upgrade(
    ch_client: ChClient,
    migrations_dir: Path = MIGRATIONS_DIR,
    dry_run: bool = False,
) -> list[str]:
    """Apply pending migrations in order; return the versions applied.

    With ``dry_run=True``, log each pending script's statements and return
    the pending versions without executing anything.

    ClickHouse has no DDL transactions: a mid-script failure leaves the
    version unrecorded, and re-running the (idempotent) script after fixing
    it completes the migration.
    """
    pending = await ch_pending(ch_client, migrations_dir)
    files_by_version = {f.version: f for f in list_migration_files(migrations_dir)}

    for version in pending:
        migration = files_by_version[version]
        statements = split_statements(migration.path.read_text())
        if dry_run:
            logger.info("[dry-run] %s would execute %d statement(s):", migration.path.name, len(statements))
            for stmt in statements:
                logger.info("[dry-run] %s", stmt)
            continue
        logger.info("Applying CH migration %s", migration.path.name)
        for stmt in statements:
            await ch_client.command(stmt)
        await ch_client.command(
            "INSERT INTO schema_migrations (version, applied_at) VALUES ({version:String}, now64(3))",
            parameters={"version": version},
        )
    return pending
```

Note on the `INSERT`: parameter binding goes through `ch_client.command(query, parameters=...)` per the `ChClient` protocol. If chdb's `command` path rejects `{name:Type}` binding for INSERT statements (verify with the tests), fall back to `await ch_client.insert("schema_migrations", [[version, utc_now()]], column_names=["version", "applied_at"])` — the protocol's `insert()` is supported by both backends.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest aaiclick/oplog/test_migrate.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/oplog/migrate.py aaiclick/oplog/test_migrate.py
git commit -m "Add async CH migration runner with schema_migrations tracking"
```

---

### Task 3: Baseline migration `0001` + packaging

**Files:**
- Create: `aaiclick/oplog/migrations/0001_baseline.sql`
- Modify: `pyproject.toml` (package-data section, near line 86)
- Test: `aaiclick/oplog/test_migrate.py` (append)

**Interfaces:**
- Consumes: `ch_upgrade` with its default `migrations_dir=MIGRATIONS_DIR` now resolving to a real directory.
- Produces: the shipped baseline — later tasks assume `ch_upgrade(ch)` with no `migrations_dir` argument creates `operation_log` and `task_logs`.

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/oplog/test_migrate.py`:

```python
async def test_baseline_creates_oplog_tables(orch_ctx):
    """The shipped 0001 baseline creates operation_log and task_logs."""
    ch = get_ch_client()

    await ch_upgrade(ch)

    for table in ("operation_log", "task_logs"):
        exists = (await ch.query(f"EXISTS TABLE {table}")).result_rows
        assert exists[0][0] == 1, table
    assert "0001" in await ch_applied_versions(ch)


async def test_baseline_is_safe_on_existing_tables(orch_ctx):
    """Pre-existing tables (an install predating the framework) are untouched."""
    ch = get_ch_client()
    await ch.command("CREATE TABLE IF NOT EXISTS operation_log (id UInt64) ENGINE = Memory")
    await ch.command("INSERT INTO operation_log VALUES (7)")

    await ch_upgrade(ch)

    rows = (await ch.query("SELECT id FROM operation_log")).result_rows
    assert rows == [(7,)]
    assert "0001" in await ch_applied_versions(ch)
```

(The second test exploits `IF NOT EXISTS`: the baseline must not error or clobber an existing table, even one with a different shape.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/oplog/test_migrate.py -v -k baseline`
Expected: FAIL — `MIGRATIONS_DIR` doesn't exist yet, so `0001` never applies (whether that surfaces as an empty glob or an error, the assertion fails). Do not add missing-directory handling to `list_migration_files`: after this task the directory ships with the package, and its absence is a packaging bug that should raise.

- [ ] **Step 3: Create the baseline migration**

`aaiclick/oplog/migrations/0001_baseline.sql` — the DDL bodies are copied **verbatim** from `OPERATION_LOG_DDL` and `TASK_LOGS_DDL` in `aaiclick/oplog/models.py` (including the `ORDER BY` clauses):

```sql
-- Baseline: the internal ClickHouse tables as they exist before the
-- migration framework. IF NOT EXISTS makes this safe on installs that
-- already have them; fresh installs get the full schema.

CREATE TABLE IF NOT EXISTS operation_log (
    id              UInt64 DEFAULT generateSnowflakeID(),
    result_table    String,
    operation       String,
    kwargs          Map(String, String),
    sql_template    Nullable(String),
    task_id         Nullable(UInt64),
    job_id          Nullable(UInt64),
    run_id          Nullable(UInt64),
    created_at      DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (result_table, created_at);

CREATE TABLE IF NOT EXISTS task_logs (
    task_id     UInt64,
    job_id      UInt64,
    run_id      UInt64,
    seq         UInt64,
    stream      String,
    level       String,
    line        String,
    created_at  DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (task_id, run_id, seq);
```

Then add the package-data entry in `pyproject.toml` next to the existing Alembic entries:

```toml
[tool.setuptools.package-data]
"aaiclick.orchestration" = ["alembic.ini"]
"aaiclick.orchestration.migrations" = ["script.py.mako"]
"aaiclick.oplog.migrations" = ["*.sql"]
```

`aaiclick/oplog/migrations/` needs no `__init__.py` — `[tool.setuptools.packages.find] include = ["aaiclick*"]` only finds packages, so also verify the data files get picked up: `uv build 2>/dev/null && unzip -l dist/*.whl | grep 0001_baseline` (then `rm -rf dist`). If the `.sql` is missing from the wheel, add an `__init__.py`-less dir workaround: `"aaiclick.oplog" = ["migrations/*.sql"]` instead.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest aaiclick/oplog/test_migrate.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/oplog/migrations/0001_baseline.sql pyproject.toml aaiclick/oplog/test_migrate.py
git commit -m "Add 0001 baseline CH migration and package the .sql files"
```

---

### Task 4: Rewire startup — `init_oplog_tables` runs the migrator; delete the validator

**Files:**
- Modify: `aaiclick/oplog/models.py` (most of it is deleted)
- Modify: `aaiclick/orchestration/orch_context.py` (import at line 21, `_OPLOG_TYPE_NAMES` at line 44)
- Modify: `aaiclick/orchestration/execution/log_flush.py` (import at line 19, ensure-call at line 41)
- Test: `aaiclick/oplog/test_migrate.py` (append)

**Interfaces:**
- Consumes: `ch_upgrade`, `ch_pending` from Task 2; `is_local()` from `aaiclick.backend`.
- Produces:
  - `init_oplog_tables(ch_client: ChClient) -> None` — same name/signature as today, new behavior.
  - `OPERATION_LOG_COLUMN_TYPES: dict[str, str]` in `aaiclick/oplog/models.py` — the renamed `OPERATION_LOG_EXPECTED_COLUMNS` (insert type names for the oplog flush; content unchanged).
  - Deleted (no longer importable): `OPERATION_LOG_DDL`, `TASK_LOGS_DDL`, `OPERATION_LOG_EXPECTED_COLUMNS`, `TASK_LOGS_EXPECTED_COLUMNS`, `_validate_schema`.

- [ ] **Step 1: Write the failing tests**

Append to `aaiclick/oplog/test_migrate.py`. Add imports: `from aaiclick.oplog import models as oplog_models` and `from aaiclick.oplog.models import init_oplog_tables`.

```python
async def test_init_oplog_tables_local_auto_migrates(orch_ctx):
    """Local mode: init applies pending migrations (zero-ops)."""
    ch = get_ch_client()
    await ch.command("DROP TABLE IF EXISTS operation_log")
    await ch.command("DROP TABLE IF EXISTS schema_migrations")

    await init_oplog_tables(ch)

    exists = (await ch.query("EXISTS TABLE operation_log")).result_rows
    assert exists[0][0] == 1


async def test_init_oplog_tables_distributed_raises_when_behind(orch_ctx, monkeypatch):
    """Distributed mode: init never applies; it names the pending versions."""
    ch = get_ch_client()
    await ch.command("DROP TABLE IF EXISTS schema_migrations")
    monkeypatch.setattr(oplog_models, "is_local", lambda: False)

    with pytest.raises(RuntimeError, match=r"aaiclick migrate upgrade"):
        await init_oplog_tables(ch)


async def test_init_oplog_tables_distributed_ok_when_current(orch_ctx, monkeypatch):
    ch = get_ch_client()
    await ch_upgrade(ch)
    monkeypatch.setattr(oplog_models, "is_local", lambda: False)

    await init_oplog_tables(ch)  # must not raise
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/oplog/test_migrate.py -v -k init_oplog`
Expected: `test_init_oplog_tables_distributed_raises_when_behind` FAILS (`is_local` not imported in models / no such attribute, or no error raised); the local test may already pass via the old CREATE-IF-NOT-EXISTS path — that's fine.

- [ ] **Step 3: Rewrite `aaiclick/oplog/models.py`**

Replace the entire file with:

```python
"""
aaiclick.oplog.models - startup entry point for the internal ClickHouse
schema (``operation_log``, ``task_logs``), driven by the migration runner
in ``aaiclick.oplog.migrate``. DDL lives in ``aaiclick/oplog/migrations/``.
"""

from __future__ import annotations

from aaiclick.backend import is_local
from aaiclick.data.data_context import ChClient

from .migrate import ch_pending, ch_upgrade

OPERATION_LOG_COLUMN_TYPES: dict[str, str] = {
    "id": "UInt64",
    "result_table": "String",
    "operation": "String",
    "kwargs": "Map(String, String)",
    "sql_template": "Nullable(String)",
    "task_id": "Nullable(UInt64)",
    "job_id": "Nullable(UInt64)",
    "run_id": "Nullable(UInt64)",
    "created_at": "DateTime64(3)",
}
# Insert type names for the oplog flush (orch_context._OPLOG_TYPE_NAMES) —
# must stay in sync with the operation_log DDL in the migration scripts.


async def init_oplog_tables(ch_client: ChClient) -> None:
    """Bring the internal CH schema up to date, or fail asking for a migrate.

    Local mode (chdb + SQLite) applies pending migrations directly —
    single-process, zero-ops, mirrors SQLite's create-on-setup. Distributed
    mode never writes: the operator runs ``aaiclick migrate upgrade``.
    """
    if is_local():
        await ch_upgrade(ch_client)
        return

    pending = await ch_pending(ch_client)
    if pending:
        raise RuntimeError(
            f"ClickHouse schema is behind (pending: {', '.join(pending)}). Run: aaiclick migrate upgrade"
        )
```

Then update the two call sites:

`aaiclick/orchestration/orch_context.py` line 21:
```python
from aaiclick.oplog.models import OPERATION_LOG_COLUMN_TYPES, init_oplog_tables
```
and line 44:
```python
_OPLOG_TYPE_NAMES = [OPERATION_LOG_COLUMN_TYPES[c] for c in _OPLOG_COLS]
```

`aaiclick/orchestration/execution/log_flush.py` — replace the import at line 19 with `from aaiclick.oplog.models import init_oplog_tables`, and in `flush_shell_logs_inline` replace the ensure block:

```python
    # A shell-only job on a fresh DB may not have run task_scope's
    # init_oplog_tables yet; bring the schema up before writing.
    try:
        await init_oplog_tables(get_ch_client())
    except Exception:
        logger.error("Failed to ensure task_logs for task %s run %s", task_id, run_id, exc_info=True)
        return
```

Check for any other importers of the deleted names before moving on:

```bash
grep -rn "OPERATION_LOG_DDL\|TASK_LOGS_DDL\|EXPECTED_COLUMNS\|_validate_schema" aaiclick/ docs/
```

Fix whatever it finds (docs references are handled in Task 6 — only fix code here).

- [ ] **Step 4: Run the oplog + orchestration test suites**

Run: `uv run pytest aaiclick/oplog/ aaiclick/orchestration/ -x -q`
Expected: PASS. Pay attention to tests that relied on `_validate_schema` errors — delete any test that only tested the removed validator (check `git grep -l validate_schema aaiclick/`).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/oplog/models.py aaiclick/orchestration/orch_context.py aaiclick/orchestration/execution/log_flush.py aaiclick/oplog/test_migrate.py
git commit -m "Drive CH startup schema through the migration runner"
```

---

### Task 5: `aaiclick migrate` integration — one command, both databases

**Files:**
- Modify: `aaiclick/oplog/migrate.py` (standalone-client helpers)
- Modify: `aaiclick/view_models.py` (`MigrationResult`, new `ChVersionStatus`, near line 245)
- Modify: `aaiclick/internal_api/setup.py` (`migrate()`, near line 121)
- Modify: `aaiclick/cli_renderers.py` (`render_migration_result`, near line 302)
- Test: `aaiclick/internal_api/test_setup.py` (extend existing migrate tests)

**Interfaces:**
- Consumes: `ch_upgrade`, `ch_pending`, `ch_applied_versions`, `list_migration_files`, `MIGRATIONS_DIR` from Tasks 1-2; `create_ch_client` from `aaiclick.data.data_context.ch_client` (async factory; clients have `async close()`).
- Produces:
  - In `aaiclick/oplog/migrate.py`:
    - `class ChVersionState(NamedTuple): version: str; applied: bool`
    - `ch_upgrade_standalone(dry_run: bool = False) -> list[str]` — **sync**; creates a client via `asyncio.run`, upgrades, closes.
    - `ch_status_standalone() -> list[ChVersionState]` — **sync**; all disk versions with applied state.
  - In `aaiclick/view_models.py`:
    - `class ChVersionStatus(BaseModel): version: str; applied: bool`
    - `MigrationResult` gains `ch_versions_applied: list[str] = []` and `ch_versions: list[ChVersionStatus] = []`.

**Circular-import constraint (do not deviate):** `aaiclick/oplog/migrate.py` must NOT import `aaiclick.view_models` — `view_models` imports `orchestration.models`, whose package init loads `orch_context` → `oplog.models` → `oplog.migrate`, closing a cycle. The runner returns the `ChVersionState` NamedTuple; `internal_api/setup.py` (which already imports both sides) converts to the pydantic `ChVersionStatus`.

- [ ] **Step 1: Write the failing tests**

Extend `aaiclick/internal_api/test_setup.py`. The existing tests monkeypatch `setup.command.upgrade` etc.; follow the same style for the CH side (the runner is exercised for real in Task 2's tests — here we only verify the wiring). Add to imports: nothing new beyond what the file has (it imports `setup` as a module).

```python
def test_migrate_upgrade_runs_alembic_then_ch(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "upgrade", lambda config, revision: calls.append(("alembic", revision)))
    monkeypatch.setattr(setup, "ch_upgrade_standalone", lambda: calls.append(("ch",)) or ["0001"])

    result = setup.migrate(MIGRATE_UPGRADE)

    assert calls == [("alembic", "head"), ("ch",)]
    assert result.ch_versions_applied == ["0001"]


def test_migrate_current_reports_ch_versions(monkeypatch):
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "current", lambda config, verbose: None)
    monkeypatch.setattr(
        setup,
        "ch_status_standalone",
        lambda: [ChVersionState(version="0001", applied=True), ChVersionState(version="0002", applied=False)],
    )

    result = setup.migrate(MIGRATE_CURRENT)

    assert result.ch_versions == [
        ChVersionStatus(version="0001", applied=True),
        ChVersionStatus(version="0002", applied=False),
    ]


def test_migrate_history_reports_ch_versions(monkeypatch):
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "history", lambda config, verbose: None)
    monkeypatch.setattr(
        setup,
        "ch_status_standalone",
        lambda: [ChVersionState(version="0001", applied=True)],
    )

    result = setup.migrate(MIGRATE_HISTORY)

    assert result.ch_versions == [ChVersionStatus(version="0001", applied=True)]


def test_migrate_downgrade_stays_alembic_only(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(setup, "get_alembic_config", lambda: object())
    monkeypatch.setattr(setup.command, "downgrade", lambda config, revision: calls.append(("alembic", revision)))
    monkeypatch.setattr(setup, "ch_upgrade_standalone", lambda: calls.append(("ch",)))

    result = setup.migrate(MIGRATE_DOWNGRADE, "-1")

    assert calls == [("alembic", "-1")]
    assert result.ch_versions_applied == []
```

Add `ChVersionStatus`, `MIGRATE_CURRENT`, and `MIGRATE_HISTORY` to the test file's imports from `aaiclick.view_models` (the file already imports the `MIGRATE_*` constants it uses — extend that import) and `from aaiclick.oplog.migrate import ChVersionState` to the aaiclick import group.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/internal_api/test_setup.py -v -k migrate`
Expected: new tests FAIL (`ChVersionStatus` import error / `ch_upgrade_standalone` attribute error); old migrate tests still PASS.

- [ ] **Step 3: Implement**

**`aaiclick/view_models.py`** — next to `MigrationResult`:

```python
class ChVersionStatus(BaseModel):
    """One ClickHouse migration version and whether it has been applied."""

    version: str
    applied: bool


class MigrationResult(BaseModel):
    """Response from ``internal_api.migrate`` — describes what was run.

    Alembic commands emit their own output to stdout; this model captures the
    request shape plus the ClickHouse-side outcome so callers can format /
    serialise the invocation itself.
    """

    action: MigrationAction
    revision: str | None = None
    ch_versions_applied: list[str] = []
    ch_versions: list[ChVersionStatus] = []
```

**`aaiclick/oplog/migrate.py`** — append the sync bridge helpers. Imports to add at top: `import asyncio`, `from aaiclick.data.data_context.ch_client import create_ch_client`. Do NOT import `aaiclick.view_models` here (see the circular-import constraint above).

```python
class ChVersionState(NamedTuple):
    version: str
    applied: bool


async def _ch_upgrade_with_client(dry_run: bool) -> list[str]:
    ch_client = await create_ch_client()
    try:
        return await ch_upgrade(ch_client, dry_run=dry_run)
    finally:
        await ch_client.close()


async def _ch_status_with_client() -> list[ChVersionState]:
    ch_client = await create_ch_client()
    try:
        applied = set(await ch_applied_versions(ch_client))
    finally:
        await ch_client.close()
    return [
        ChVersionState(version=f.version, applied=f.version in applied)
        for f in list_migration_files(MIGRATIONS_DIR)
    ]


def ch_upgrade_standalone(dry_run: bool = False) -> list[str]:
    """Sync entry for ``aaiclick migrate``: own client, own event loop."""
    return asyncio.run(_ch_upgrade_with_client(dry_run))


def ch_status_standalone() -> list[ChVersionState]:
    """Sync entry for ``aaiclick migrate current/history``."""
    return asyncio.run(_ch_status_with_client())
```

**`aaiclick/internal_api/setup.py`** — add `from aaiclick.oplog.migrate import ch_status_standalone, ch_upgrade_standalone` to the imports and `ChVersionStatus` to the existing `aaiclick.view_models` import. Update `migrate()`:

```python
def _ch_versions() -> list[ChVersionStatus]:
    return [ChVersionStatus(version=s.version, applied=s.applied) for s in ch_status_standalone()]
```

```python
    if action == MIGRATE_UPGRADE:
        target = revision or "head"
        command.upgrade(config, target)
        ch_applied = ch_upgrade_standalone()
        return MigrationResult(action=action, revision=target, ch_versions_applied=ch_applied)

    if action == MIGRATE_CURRENT:
        command.current(config, verbose=True)
        return MigrationResult(action=action, ch_versions=_ch_versions())

    if action == MIGRATE_HISTORY:
        command.history(config, verbose=True)
        return MigrationResult(action=action, ch_versions=_ch_versions())
```

(`downgrade`/`heads`/`show` branches unchanged.) Also update the module docstring's "SQL migrations" mention and `migrate()`'s docstring to say it drives Alembic **and** the ClickHouse runner.

**`aaiclick/cli_renderers.py`** — extend `render_migration_result`:

```python
def render_migration_result(result: MigrationResult) -> None:
    """Confirm upgrade/downgrade success — alembic logs the rest on its own."""
    if result.action == MIGRATE_UPGRADE:
        print(f"Database upgraded to {result.revision}")
        if result.ch_versions_applied:
            print(f"ClickHouse migrations applied: {', '.join(result.ch_versions_applied)}")
        else:
            print("ClickHouse schema up to date")
    elif result.action == MIGRATE_DOWNGRADE:
        print(f"Database downgraded to {result.revision}")
    if result.ch_versions:
        for v in result.ch_versions:
            print(f"ClickHouse {v.version}: {'applied' if v.applied else 'pending'}")
```

(Match the existing function body layout — check the current `elif` chain before editing; keep any branches this snippet doesn't mention.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest aaiclick/internal_api/test_setup.py aaiclick/oplog/test_migrate.py -v`
Expected: all PASS.

- [ ] **Step 5: Smoke-test the CLI end to end against a temp root**

```bash
AAICLICK_LOCAL_ROOT=$(mktemp -d) uv run python -m aaiclick migrate upgrade
```
Expected output includes `Database upgraded to head` and either `ClickHouse migrations applied: 0001` or (if something pre-created the schema) `ClickHouse schema up to date`. Then:
```bash
AAICLICK_LOCAL_ROOT=$(mktemp -d) uv run python -m aaiclick migrate history
```
Expected: alembic history plus `ClickHouse 0001: pending` (history never applies).

- [ ] **Step 6: Commit**

```bash
git add aaiclick/view_models.py aaiclick/oplog/migrate.py aaiclick/internal_api/setup.py aaiclick/cli_renderers.py aaiclick/internal_api/test_setup.py
git commit -m "Drive ClickHouse migrations through aaiclick migrate"
```

---

### Task 6: Documentation + future.md cleanup + full suite

**Files:**
- Modify: `docs/designs/ch_migrations.md` (implementation references)
- Modify: `docs/designs/future.md` (remove the "ClickHouse Migration Framework" section)
- Modify: `docs/user_guide/oplog.md` (the `init_oplog_tables` / `_validate_schema` description at ~line 12 and 28)
- Modify: `aaiclick/__main__.py` (`_MIGRATE_HELP` text, ~line 303)

**Interfaces:** none — docs and help text only.

- [ ] **Step 1: Update the docs**

Read the `markdown-style` skill first (setext title, `#`/`##` only, implementation references by symbol name, aligned tables).

1. `docs/designs/future.md`: delete the entire `## ClickHouse Migration Framework` section under `# Medium Priority` (lines ~10-24). Keep the note about per-change execution strategies? No — the spec's Out-of-scope section already records it; delete the whole section.
2. `docs/designs/ch_migrations.md`: remove the "Tracked in docs/designs/future.md ... remove that entry when this lands" paragraph; add implementation references (by symbol, not line number):
   - Runner section: `**Implementation**: aaiclick/oplog/migrate.py — see ch_pending(), ch_upgrade()`
   - Startup section: `**Implementation**: aaiclick/oplog/models.py — see init_oplog_tables()`
   - Integration section: `**Implementation**: aaiclick/internal_api/setup.py — see migrate()`
3. `docs/user_guide/oplog.md`: rewrite the two spots that describe `CREATE TABLE IF NOT EXISTS` + `_validate_schema` to describe the migration runner instead; reference `docs/designs/ch_migrations.md`.
4. `aaiclick/__main__.py` `_MIGRATE_HELP`: mention that `upgrade` also applies ClickHouse migrations and that `current`/`history` report both databases (keep it to two short lines; the command list itself is unchanged).

- [ ] **Step 2: Grep for stragglers**

```bash
grep -rn "OPERATION_LOG_DDL\|TASK_LOGS_DDL\|EXPECTED_COLUMNS\|_validate_schema\|drop the table" aaiclick/ docs/ --include="*.py" --include="*.md" | grep -v ch_migrations.md | grep -v plans/
```
Expected: only `OPERATION_LOG_COLUMN_TYPES` hits (the renamed dict) and historical notes that are accurate. Fix anything stale.

- [ ] **Step 3: Run the full test suite**

Run: `uv run pytest -q -p no:cacheprovider -x`
Expected: PASS (same set of failures/skips as on the base branch, if any pre-exist — verify with `git stash && uv run pytest -q ... ` only if something fails unexpectedly).

- [ ] **Step 4: Commit and push**

```bash
git add docs/ aaiclick/__main__.py
git commit -m "Document the CH migration framework; retire the future.md entry"
git push -u origin claude/clickhouse-migration-framework-ub0l3w
```

---

## Self-Review Notes (author-verified)

- Spec coverage: files+naming (T1/T3), tracking table (T2), runner semantics incl. gap/unknown/dry-run/mid-failure (T2), baseline (T3), startup local/distributed + validator removal + `OPERATION_LOG_COLUMN_TYPES` rename + log_flush rewire (T4), `migrate` integration + `MigrationResult` fields + renderer (T5), docs/future.md (T6). Testing section of the spec maps to T1-T4 test lists.
- `dry_run` is exposed on `ch_upgrade`/`ch_upgrade_standalone` but not yet on the CLI — the spec lists `--dry-run` CLI plumbing nowhere (future.md's old sketch mentioned it; the spec's runner section defines `dry_run` on the runner only). Deliberate.
- Type consistency: the runner speaks `ChVersionState` (NamedTuple, `oplog/migrate.py`); the API/renderer speak `ChVersionStatus` (pydantic, `view_models.py`); `internal_api/setup.py._ch_versions()` is the only conversion point. `MigrationFile.version: str` everywhere; `ch_*` functions take `ChClient` first.
