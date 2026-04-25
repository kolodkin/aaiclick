Phase 1 — Foundation
---

> Parent plan: `2026-04-25-simplify-orchestration-lifecycle.md` · Spec: `docs/superpowers/specs/2026-04-25-simplify-orchestration-lifecycle-design.md`

**Goal:** Land the type alias, exception class, and SQL schema changes (`preserve` columns, drop unused tables, new `task_name_locks`, add `preserved` column to `table_registry`) — all behind a single Alembic migration. After this phase, the schema is ready but no behavior has changed.

**Why this order:** The migration must exist before code references new columns. We add the columns first (with tolerant defaults) so existing code keeps working; later phases hook them up.

---

## Task 1: Branch sanity & baseline

**Files:**
- Verify only — no edits.

- [ ] **Step 1: Confirm current branch**

```bash
git -C /home/user/aaiclick branch --show-current
```

Expected: `claude/simplify-orchestration-lifecycle-gwqt4`. If different, switch:

```bash
git -C /home/user/aaiclick switch claude/simplify-orchestration-lifecycle-gwqt4
```

- [ ] **Step 2: Run baseline test suite**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/ -x --no-cov -q
```

Expected: PASS (record baseline pass count for later comparison).

---

## Task 2: Add `Preserve` type alias and `TableNameCollision` exception

**Files:**
- Modify: `aaiclick/orchestration/models.py`
- Modify: `aaiclick/orchestration/lifecycle/db_lifecycle.py`

- [ ] **Step 1: Add `Preserve` type alias to `models.py`**

Add near the top, after existing imports:

```python
from typing import Literal

Preserve = list[str] | Literal["*"] | None
"""Job-level table preservation declaration.

- ``None`` — nothing preserved (default; pure task-local semantics).
- ``["foo", "bar"]`` — these named tables survive the run; dropped at job completion.
- ``"*"`` — every ``j_<id>_<name>`` created during the job survives the run.
- ``[]`` — explicit ``no preservation``; does NOT fall through to RegisteredJob default.
"""
```

- [ ] **Step 2: Add `TableNameCollision` exception**

Append to `aaiclick/orchestration/lifecycle/db_lifecycle.py` (or create a new sibling module if exceptions live elsewhere — search for existing `class .*Error\b` in `aaiclick/orchestration/` to align):

```python
class TableNameCollision(Exception):
    """Raised when a task takes a non-preserved name held by another live task in the same job."""

    def __init__(self, name: str, held_by_task_id: int):
        self.name = name
        self.held_by_task_id = held_by_task_id
        super().__init__(
            f"non-preserved table name {name!r} is held by live task id={held_by_task_id}"
        )
```

- [ ] **Step 3: Re-export from package init if other exceptions are**

```bash
grep -n "Error\|Exception" /home/user/aaiclick/aaiclick/orchestration/__init__.py
```

If existing exceptions are re-exported, add `TableNameCollision` to the same import block. Otherwise skip.

- [ ] **Step 4: Run targeted test to ensure imports work**

```bash
cd /home/user/aaiclick && python -c "from aaiclick.orchestration.models import Preserve; from aaiclick.orchestration.lifecycle.db_lifecycle import TableNameCollision; print('ok')"
```

Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/models.py aaiclick/orchestration/lifecycle/db_lifecycle.py
git commit -m "$(cat <<'EOF'
feature: Preserve type alias and TableNameCollision exception

Foundation for the simplified orchestration lifecycle. Preserve is a
type alias for list[str] | Literal["*"] | None used on Job and
RegisteredJob in later commits. TableNameCollision is raised by the
new task_name_locks coordinator when two live tasks try to take the
same non-preserved name.
EOF
)"
```

---

## Task 3: Add `preserve` field to `Job` and `RegisteredJob` models

**Files:**
- Modify: `aaiclick/orchestration/models.py`
- Test: existing model tests (`test_orchestration_factories.py` will fail until Phase 2; that is expected — but the model itself must import cleanly).

- [ ] **Step 1: Add `preserve` JSON column to `Job`**

In `aaiclick/orchestration/models.py`, locate the `Job` SQLModel class. Add:

```python
from sqlalchemy import Column
from sqlalchemy.types import JSON

# ... inside class Job(SQLModel, table=True):
preserve: Preserve = Field(
    default=None,
    sa_column=Column(JSON, nullable=True),
)
```

The existing `preservation_mode` column stays — Phase 6 deletes it.

- [ ] **Step 2: Add `preserve` JSON column to `RegisteredJob`**

Same pattern in the `RegisteredJob` class:

```python
preserve: Preserve = Field(
    default=None,
    sa_column=Column(JSON, nullable=True),
)
```

- [ ] **Step 3: Verify imports + class loads**

```bash
cd /home/user/aaiclick && python -c "from aaiclick.orchestration.models import Job, RegisteredJob; j = Job(); print(j.preserve)"
```

Expected: `None`.

- [ ] **Step 4: Commit (no functional change yet — column added but no migration applied)**

```bash
git add aaiclick/orchestration/models.py
git commit -m "$(cat <<'EOF'
feature: preserve column on Job and RegisteredJob models

Adds the ORM-side definition. Migration in the next commit creates
the underlying column; resolve_preserve() and create_job(preserve=...)
land in Phase 2.
EOF
)"
```

---

## Task 4: Generate Alembic migration skeleton

**Files:**
- Create: `aaiclick/orchestration/migrations/versions/<auto>_simplify_lifecycle.py`

- [ ] **Step 1: Generate the empty migration file**

```bash
cd /home/user/aaiclick && alembic -c aaiclick/orchestration/alembic.ini revision -m "simplify lifecycle: preserve column, drop run_refs and context_refs, add task_name_locks"
```

Capture the generated filename — call it `MIG_FILE` for the rest of this task. The full path will be `aaiclick/orchestration/migrations/versions/<rev>_simplify_lifecycle.py`.

- [ ] **Step 2: Confirm `down_revision` matches current head**

```bash
cd /home/user/aaiclick && alembic -c aaiclick/orchestration/alembic.ini heads
```

The generated file's `down_revision` must equal the head returned. If multiple heads exist, stop and ask — do not invent a merge.

- [ ] **Step 3: Commit the empty skeleton**

```bash
git add aaiclick/orchestration/migrations/versions/*_simplify_lifecycle.py
git commit -m "refactor: alembic migration skeleton for lifecycle simplification"
```

---

## Task 5: Implement migration `upgrade()`

**Files:**
- Modify: the migration file generated in Task 4.

- [ ] **Step 1: Write `upgrade()`**

Replace the empty `def upgrade()` body with:

```python
def upgrade() -> None:
    # 1. Add `preserve` JSON column to `jobs` and `registered_jobs`
    op.add_column("jobs", sa.Column("preserve", sa.JSON(), nullable=True))
    op.add_column("registered_jobs", sa.Column("preserve", sa.JSON(), nullable=True))

    # 2. Backfill preserve from preservation_mode where the column exists.
    #    NONE -> NULL (default already), FULL -> '"*"'.
    op.execute(
        "UPDATE jobs SET preserve = '\"*\"' WHERE preservation_mode = 'FULL'"
    )
    op.execute(
        "UPDATE registered_jobs SET preserve = '\"*\"' WHERE preservation_mode = 'FULL'"
    )

    # 3. Drop preservation_mode columns
    op.drop_column("jobs", "preservation_mode")
    op.drop_column("registered_jobs", "preservation_mode")

    # 4. Drop obsolete tables
    op.drop_table("table_run_refs")
    op.drop_table("table_context_refs")

    # 5. Trim `table_registry`: drop `run_id`, add `preserved`
    op.drop_column("table_registry", "run_id")
    op.add_column(
        "table_registry",
        sa.Column("preserved", sa.Boolean(), nullable=False, server_default=sa.false()),
    )

    # 6. Create `task_name_locks`
    op.create_table(
        "task_name_locks",
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("task_id", sa.BigInteger(), nullable=False),
        sa.Column("acquired_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("job_id", "name"),
    )
    op.create_index(
        "ix_task_name_locks_task_id",
        "task_name_locks",
        ["task_id"],
    )
```

Adjust column types (e.g. `BigInteger` vs `Integer`) to match the actual existing schema — check the previous migration files in `aaiclick/orchestration/migrations/versions/` for the convention.

- [ ] **Step 2: Run `alembic upgrade head` against a fresh test database**

```bash
cd /home/user/aaiclick && rm -f .pytest_aaiclick.sqlite && AAICLICK_SQL_URL=sqlite:///.pytest_aaiclick.sqlite alembic -c aaiclick/orchestration/alembic.ini upgrade head
```

Expected: succeeds with no error. If it errors, fix the migration before continuing.

- [ ] **Step 3: Commit**

```bash
git add aaiclick/orchestration/migrations/versions/*_simplify_lifecycle.py
git commit -m "feature: alembic upgrade for lifecycle simplification"
```

---

## Task 6: Implement migration `downgrade()`

**Files:**
- Modify: same migration file.

- [ ] **Step 1: Write `downgrade()`**

```python
def downgrade() -> None:
    # 6. Drop task_name_locks
    op.drop_index("ix_task_name_locks_task_id", "task_name_locks")
    op.drop_table("task_name_locks")

    # 5. Restore table_registry: drop `preserved`, add `run_id`
    op.drop_column("table_registry", "preserved")
    op.add_column("table_registry", sa.Column("run_id", sa.BigInteger(), nullable=True))

    # 4. Recreate table_run_refs and table_context_refs (empty — historical data
    #    is unrecoverable; the goal is to allow ORM imports against the prior
    #    schema, not to round-trip data).
    op.create_table(
        "table_run_refs",
        sa.Column("table_name", sa.String(length=255), nullable=False),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("table_name", "run_id"),
    )
    op.create_table(
        "table_context_refs",
        sa.Column("table_name", sa.String(length=255), nullable=False),
        sa.Column("context_id", sa.BigInteger(), nullable=False),
        sa.Column("advisory_id", sa.BigInteger(), nullable=True),
        sa.Column("job_id", sa.BigInteger(), nullable=True),
        sa.PrimaryKeyConstraint("table_name", "context_id"),
    )

    # 3. Restore preservation_mode columns
    op.add_column("jobs", sa.Column("preservation_mode", sa.String(length=16), nullable=False, server_default="NONE"))
    op.add_column("registered_jobs", sa.Column("preservation_mode", sa.String(length=16), nullable=False, server_default="NONE"))

    # 2. Backfill preservation_mode from preserve. Only "*" round-trips to FULL;
    #    list values cannot be expressed and trigger an explicit failure.
    op.execute(
        "UPDATE jobs SET preservation_mode = 'FULL' WHERE preserve = '\"*\"'"
    )
    op.execute(
        "UPDATE registered_jobs SET preservation_mode = 'FULL' WHERE preserve = '\"*\"'"
    )

    # Validate no list-shaped preserve values remain — fail loud if they do.
    bind = op.get_bind()
    bad_jobs = bind.execute(
        sa.text(
            "SELECT id, preserve FROM jobs WHERE preserve IS NOT NULL AND preserve <> '\"*\"'"
        )
    ).fetchall()
    if bad_jobs:
        raise RuntimeError(
            f"Cannot downgrade: jobs have list-shaped preserve values that don't map to PreservationMode: {bad_jobs!r}"
        )
    bad_reg = bind.execute(
        sa.text(
            "SELECT id, preserve FROM registered_jobs WHERE preserve IS NOT NULL AND preserve <> '\"*\"'"
        )
    ).fetchall()
    if bad_reg:
        raise RuntimeError(
            f"Cannot downgrade: registered_jobs have list-shaped preserve values: {bad_reg!r}"
        )

    # 1. Drop preserve columns
    op.drop_column("jobs", "preserve")
    op.drop_column("registered_jobs", "preserve")
```

- [ ] **Step 2: Test the round-trip on a fresh DB**

```bash
cd /home/user/aaiclick && rm -f .pytest_aaiclick.sqlite && AAICLICK_SQL_URL=sqlite:///.pytest_aaiclick.sqlite alembic -c aaiclick/orchestration/alembic.ini upgrade head && AAICLICK_SQL_URL=sqlite:///.pytest_aaiclick.sqlite alembic -c aaiclick/orchestration/alembic.ini downgrade -1 && AAICLICK_SQL_URL=sqlite:///.pytest_aaiclick.sqlite alembic -c aaiclick/orchestration/alembic.ini upgrade head
```

Expected: all three commands succeed.

- [ ] **Step 3: Commit**

```bash
git add aaiclick/orchestration/migrations/versions/*_simplify_lifecycle.py
git commit -m "feature: alembic downgrade for lifecycle simplification"
```

---

## Task 7: Phase 1 sanity check

- [ ] **Step 1: Run the full orchestration test suite**

```bash
cd /home/user/aaiclick && pytest aaiclick/orchestration/ -x --no-cov -q
```

Expected: PASS at the same level as the baseline (Task 1 Step 2). Some tests may now skip if they relied on `preservation_mode` column existing in a way that was already loose; if any *new* failures appear, fix before moving to Phase 2.

- [ ] **Step 2: Push the branch**

```bash
git -C /home/user/aaiclick push -u origin claude/simplify-orchestration-lifecycle-gwqt4
```

---

# Done When

- The migration applies and round-trips on a fresh SQLite DB.
- `Job.preserve` and `RegisteredJob.preserve` exist on ORM models.
- `Preserve` type alias and `TableNameCollision` exception are importable.
- `pytest aaiclick/orchestration/ -x` is green.
