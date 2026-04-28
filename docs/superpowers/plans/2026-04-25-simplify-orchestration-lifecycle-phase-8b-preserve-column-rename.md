Phase 8b — Replace the `preserve` JSON column with `preserve_all` boolean
---

> Parent plan: `2026-04-25-simplify-orchestration-lifecycle.md` · Prerequisite: Phase 8a (`...-phase-8a-collapse-preserve-api.md`)

**Goal:** Now that `Preserve` is `Literal["*"] | None`, the JSON column is over-engineered. Replace `jobs.preserve JSON` and `registered_jobs.preserve JSON` with `jobs.preserve_all BOOLEAN NOT NULL DEFAULT FALSE` (and the same on `registered_jobs`).

**Why:** A two-state field stored as JSON costs serialization on every read, complicates `_parse_preserve`-style decoders that have to handle "is this a string or already a Python value?", and reads less clearly than a boolean. With Phase 8a's collapse to `Literal["*"] | None` there's nothing left in the JSON column except `'"*"'` or `NULL`, which is just a clumsy boolean.

**Net delta:** ~30 lines deleted (boolean handling is shorter than JSON-aware accessor); reads are dialect-uniform on Postgres and SQLite without `json.loads`.

---

# Tasks

## Task 1: Add `preserve_all` to the SQLModels

**Files:** `aaiclick/orchestration/models.py`.

- [ ] Replace `preserve: Preserve = Field(default=None, sa_column=Column(JSON, nullable=True))` on both `Job` and `RegisteredJob` with `preserve_all: bool = Field(default=False, sa_column=Column(Boolean, nullable=False, server_default="0"))`.
- [ ] Drop the `Preserve` type alias entirely if no other call sites use it; otherwise leave it and add a deprecation note.

## Task 2: Update `resolve_preserve` and the public API

**Files:** `aaiclick/orchestration/factories.py`, `aaiclick/orchestration/registered_jobs.py`, `aaiclick/orchestration/decorators.py`.

- [ ] Rename `resolve_preserve(explicit, registered) -> Preserve` to `resolve_preserve_all(explicit: bool | None, registered: bool) -> bool` (or similar). Sentinel collapses to `bool | None`.
- [ ] `create_job`, `register_job`, `upsert_registered_job`, `run_job`, and `JobFactory.__call__` accept `preserve_all: bool | None` instead of `preserve`. Default `None` (inherit), `True` (preserve everything), `False` (default cleanup).

Alternative naming: keep the parameter `preserve` but type it `bool | None`. The CLI / docs win is purely cosmetic — pick whichever reads better in `docs/orchestration.md#declaring-preserved-tables`.

## Task 3: Update the BG worker

**Files:** `aaiclick/orchestration/background/background_worker.py`.

- [ ] Drop the `_parse_preserve` helper (already removed in 8a, but verify).
- [ ] In any code that reads `Job.preserve_all`, use the column directly — no JSON decode.

## Task 4: Migration

**Files:** new alembic revision.

```python
def upgrade() -> None:
    for table in ("jobs", "registered_jobs"):
        op.add_column(
            table,
            sa.Column("preserve_all", sa.Boolean(), nullable=False, server_default=sa.false()),
        )
        op.execute(
            f"UPDATE {table} SET preserve_all = TRUE WHERE preserve IS NOT NULL AND preserve != 'null'"
        )
        op.drop_column(table, "preserve")


def downgrade() -> None:
    for table in ("jobs", "registered_jobs"):
        op.add_column(table, sa.Column("preserve", sa.JSON(), nullable=True))
        op.execute(f"UPDATE {table} SET preserve = '\"*\"' WHERE preserve_all = TRUE")
        op.drop_column(table, "preserve_all")
```

## Task 5: Tests + docs

**Files:** `test_preserve_resolution.py`, `test_orchestration_factories.py`, `test_registered_jobs.py`, `docs/orchestration.md`, `docs/data_context.md`, `docs/lineage.md`.

- [ ] Update tests to pass `preserve_all=True` / `preserve_all=False` (or just `True` / leave default) instead of `preserve="*"` / `preserve=None`.
- [ ] Update docs: drop the `preserve` parameter table; show `preserve_all=True` examples for full-replay; explain that named tables are always preserved by virtue of being named.

---

# Done When

- `jobs.preserve` and `registered_jobs.preserve` JSON columns are gone.
- `jobs.preserve_all` and `registered_jobs.preserve_all` are `BOOLEAN NOT NULL DEFAULT FALSE`.
- `Preserve` type alias and `_parse_preserve` decoder no longer exist.
- `pytest aaiclick/ -x` is green on both local and dist matrices.
- Migration round-trips on a fresh SQLite DB.
- `docs/orchestration.md` reflects the boolean form.
