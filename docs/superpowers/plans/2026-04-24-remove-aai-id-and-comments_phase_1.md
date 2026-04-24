# Phase 1 — Registry Schema & Pydantic Model

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Prepare the target data shape — extend `ColumnView`/`SchemaView` with `fieldtype` fields and add a `schema_doc` column to the SQL `table_registry` — without changing any runtime behaviour yet.

**Depends on:** nothing. Execute first.

**Unlocks:** Phase 2 (which starts reading `schema_doc` back out).

---

## File Structure

| File                                                                                              | Role                                                                      |
|---------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| `aaiclick/data/view_models.py`                                                                    | Modify — add `fieldtype` to `ColumnView` and `SchemaView`.                |
| `aaiclick/data/test_view_models.py`                                                               | Modify — add round-trip tests covering the new fields.                    |
| `aaiclick/orchestration/lifecycle/db_lifecycle.py`                                                | Modify — add `schema_doc: str \| None` column on `TableRegistry`.        |
| `aaiclick/orchestration/migrations/versions/<new>_add_schema_doc_to_table_registry.py`           | Create — Alembic migration.                                               |
| `aaiclick/orchestration/test_db_lifecycle.py` (create if missing)                                 | Modify/create — assert the column is writable + readable.                 |

---

### Task 1.1: Add `fieldtype` to `ColumnView` and `SchemaView`

**Files:**
- Modify: `aaiclick/data/view_models.py` — `ColumnView` class and `SchemaView` class.
- Modify: `aaiclick/data/test_view_models.py` — add round-trip test.

**Background for the engineer:** `ColumnView` / `SchemaView` are Pydantic v2 models already used as the API response shape. After this phase they will *also* be the persistence shape stored in `table_registry.schema_doc`. `fieldtype` is a single lowercase letter — `"s"` (scalar), `"a"` (array), `"d"` (dict). Per-column it can only be `"s"` or `"a"`. `EngineType` is already imported from `.models`.

- [ ] **Step 1: Write the failing test** in `aaiclick/data/test_view_models.py` (append at end of file; keep existing imports):

```python
def test_schema_view_round_trip_with_fieldtype():
    sv = SchemaView(
        columns=[
            ColumnView(name="title", type="String", fieldtype="s"),
            ColumnView(name="votes", type="Int64", fieldtype="a"),
        ],
        order_by="(title)",
        engine="MergeTree",
        fieldtype="d",
    )
    dumped = sv.model_dump_json()
    restored = SchemaView.model_validate_json(dumped)
    assert restored == sv
    assert restored.fieldtype == "d"
    assert restored.columns[0].fieldtype == "s"
    assert restored.columns[1].fieldtype == "a"


def test_column_view_fieldtype_rejects_invalid():
    with pytest.raises(ValidationError):
        ColumnView(name="x", type="Int64", fieldtype="d")  # "d" not allowed on a column


def test_schema_view_fieldtype_rejects_invalid():
    with pytest.raises(ValidationError):
        SchemaView(fieldtype="x")
```

Make sure the file's imports include (add any that are missing):

```python
import pytest
from pydantic import ValidationError
from aaiclick.data.view_models import ColumnView, SchemaView
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/data/test_view_models.py -v -k "fieldtype"`
Expected: FAIL — `ColumnView.__init__()` got unexpected keyword `fieldtype` (and similar for `SchemaView`).

- [ ] **Step 3: Extend the Pydantic models**

In `aaiclick/data/view_models.py`:

```python
# Near the top of the file, add to imports:
from typing import Literal

# ... then modify ColumnView (currently around line 21):
class ColumnView(BaseModel):
    """Single column description used inside ``SchemaView``."""

    name: str
    type: str
    nullable: bool = False
    array_depth: int = 0
    low_cardinality: bool = False
    fieldtype: Literal["s", "a"] = "s"


# ... and modify SchemaView (currently around line 31):
class SchemaView(BaseModel):
    """Table schema representation used inside ``ObjectDetail``."""

    columns: list[ColumnView] = Field(default_factory=list)
    order_by: str | None = None
    engine: EngineType | None = None
    fieldtype: Literal["s", "a", "d"] = "s"
```

The defaults are deliberately permissive (`"s"` for both) — existing callers that don't pass `fieldtype` keep working, and Phase 3 will populate real values at create time.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/data/test_view_models.py -v`
Expected: all tests pass, including the three new ones.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/view_models.py aaiclick/data/test_view_models.py
git commit -m "$(cat <<'EOF'
feature: add fieldtype to ColumnView and SchemaView

Extends the existing Pydantic schema models with fieldtype
(Literal["s","a"] per-column; Literal["s","a","d"] object-level)
so SchemaView can also serve as the persistence shape in
table_registry.schema_doc.
EOF
)"
```

---

### Task 1.2: Add `schema_doc` column to `TableRegistry` SQLModel

**Files:**
- Modify: `aaiclick/orchestration/lifecycle/db_lifecycle.py` — `TableRegistry` class.
- Create or modify: `aaiclick/orchestration/test_db_lifecycle.py` — round-trip test.

**Background:** `TableRegistry` is a SQLModel table. We're adding one nullable `Text` column. Nullable is the right choice for the migration step because rows created by older code paths between deploy and Phase-3 cutover won't have it. Phase 3 makes writes always populate it; Phase 2 reads it and raises a clear error if it's missing.

**Important — the writes are raw SQL, not ORM**. `TableRegistry` rows are not currently inserted via the ORM; they are written by a raw SQL INSERT in `DBLifecycleHandler._write_table_registry_row` (`aaiclick/orchestration/orch_context.py` ~lines 213-240) and a second raw INSERT in `aaiclick/orchestration/oplog_backfill.py:74`. The ORM-level column addition in this task is necessary but **not sufficient** — Phase 3 Task 3.2 updates those raw INSERTs. Also: the test helper `aaiclick/orchestration/background/conftest.py::insert_table_registry` writes raw SQL; it stays happy with the new nullable column today, but Phase 3 will extend it so background tests can assert on `schema_doc`.

- [ ] **Step 1: Write the failing test**

**No `sql_session` fixture exists in this project** — every orchestration test opens a session via `AsyncSession(engine)` directly, typically against the `bg_db` fixture in `aaiclick/orchestration/background/conftest.py`. Mirror that pattern here. Create `aaiclick/orchestration/test_db_lifecycle.py`:

```python
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry


async def test_table_registry_accepts_schema_doc(bg_db):
    async with AsyncSession(bg_db) as session:
        session.add(
            TableRegistry(
                table_name="t_test_1",
                job_id=None,
                task_id=None,
                run_id=None,
                schema_doc='{"columns":[],"fieldtype":"s"}',
            )
        )
        await session.commit()

    async with AsyncSession(bg_db) as session:
        result = await session.execute(
            select(TableRegistry).where(TableRegistry.table_name == "t_test_1")
        )
        assert result.scalar_one().schema_doc == '{"columns":[],"fieldtype":"s"}'


async def test_table_registry_schema_doc_is_optional(bg_db):
    async with AsyncSession(bg_db) as session:
        session.add(TableRegistry(table_name="t_test_2"))
        await session.commit()

    async with AsyncSession(bg_db) as session:
        result = await session.execute(
            select(TableRegistry).where(TableRegistry.table_name == "t_test_2")
        )
        assert result.scalar_one().schema_doc is None
```

The `bg_db` fixture is defined in `aaiclick/orchestration/background/conftest.py`. Placing the new test file at `aaiclick/orchestration/test_db_lifecycle.py` means you need to import it — either move the fixture to a more broadly-scoped conftest (preferred), or write a thin local fixture that mirrors `bg_db`. Simpler: place the new test file at `aaiclick/orchestration/background/test_db_lifecycle.py` so `bg_db` resolves automatically.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_db_lifecycle.py -v`
Expected: FAIL with `TypeError: 'schema_doc' is an invalid keyword argument for TableRegistry`.

- [ ] **Step 3: Add the column**

In `aaiclick/orchestration/lifecycle/db_lifecycle.py`, modify `TableRegistry` (around line 115):

```python
class TableRegistry(SQLModel, table=True):
    """..."""  # keep existing docstring

    __tablename__: ClassVar[str] = "table_registry"

    table_name: str = Field(sa_column=Column(String, primary_key=True))
    job_id: int | None = Field(sa_column=Column(BigInteger, nullable=True, index=True))
    task_id: int | None = Field(sa_column=Column(BigInteger, nullable=True))
    run_id: int | None = Field(sa_column=Column(BigInteger, nullable=True))
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime, nullable=False, index=True),
    )
    schema_doc: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
    )
```

Ensure `Text` is imported at the top of the file alongside `String`, `BigInteger`, `DateTime`, `Column`. If not, add:

```python
from sqlalchemy import Column, Text  # merge with existing sqlalchemy imports
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/orchestration/test_db_lifecycle.py -v`
Expected: both tests PASS. (The test fixtures create tables via SQLModel metadata, so no migration is needed for the in-memory test DB.)

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/lifecycle/db_lifecycle.py aaiclick/orchestration/test_db_lifecycle.py
git commit -m "feature: add nullable schema_doc column to TableRegistry"
```

---

### Task 1.3: Alembic migration for `schema_doc`

**Files:**
- Create: `aaiclick/orchestration/migrations/versions/<auto_generated>_add_schema_doc_to_table_registry.py`

**Background:** Migrations go through Alembic's `revision` command. The latest existing revision is `c8f4a2b91e57` (`move_table_registry_to_sql`); the new one chains off it. (Earlier drafts of this plan said `f3a8b1c42d5e` — that revision already has a child, `b7d3e2f19a4c`, so chaining off it forks the tree. Don't.) Verify the current head with `alembic -c aaiclick/orchestration/migrations/alembic.ini heads` before you run the `revision` command. `schema_doc` is added as nullable so the migration is additive and safe to run against a live DB.

- [ ] **Step 1: Generate the migration skeleton**

From the repo root:

```bash
alembic -c aaiclick/orchestration/migrations/alembic.ini revision -m "add schema_doc to table_registry"
```

(Adjust the `-c` path if the project's `alembic.ini` is at the repo root — check with `ls alembic.ini` first.) Alembic picks a new revision id like `a1b2c3d4e5f6`. Record it.

- [ ] **Step 2: Fill in `upgrade()` and `downgrade()`**

Open the newly created file. Replace the `upgrade`/`downgrade` bodies:

```python
"""add schema_doc to table_registry

Revision ID: <generated>
Revises: c8f4a2b91e57
Create Date: 2026-04-24 ...

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "<generated>"
down_revision: str | Sequence[str] | None = "c8f4a2b91e57"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "table_registry",
        sa.Column("schema_doc", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("table_registry", "schema_doc")
```

Leave the `revision` string as Alembic generated it; only confirm `down_revision = "c8f4a2b91e57"` (the current head as of this plan being written; run `alembic heads` if unsure).

- [ ] **Step 3: Apply the migration locally and check status**

```bash
alembic -c aaiclick/orchestration/migrations/alembic.ini upgrade head
alembic -c aaiclick/orchestration/migrations/alembic.ini current
```

Expected `current` output: shows the new revision id. If the repo uses a helper script (e.g. `scripts/migrate.sh`), prefer that — check `CLAUDE.md` and `docs/future.md` for project conventions.

- [ ] **Step 4: Round-trip the downgrade then re-upgrade**

```bash
alembic -c aaiclick/orchestration/migrations/alembic.ini downgrade -1
alembic -c aaiclick/orchestration/migrations/alembic.ini upgrade head
```

Expected: both succeed, `alembic current` again shows the new revision. This proves `downgrade()` is correct.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/migrations/versions/<generated>_add_schema_doc_to_table_registry.py
git commit -m "feature: alembic migration adding schema_doc to table_registry"
```

---

## Phase 1 Complete

At this point:

- `ColumnView.fieldtype` and `SchemaView.fieldtype` exist with safe defaults.
- `TableRegistry.schema_doc` is a nullable `Text` column at the ORM level **and** in the migration history.
- No runtime code writes or reads `schema_doc` yet — that's Phases 2 and 3.

Run the whole test suite (`pytest aaiclick/`) before moving on; nothing in production paths should have broken because the new fields are purely additive with defaults.
