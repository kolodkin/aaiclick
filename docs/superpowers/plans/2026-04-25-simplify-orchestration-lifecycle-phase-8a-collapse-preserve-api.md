Phase 8a — Collapse the `preserve` API and delete `task_name_locks`
---

> Parent plan: `2026-04-25-simplify-orchestration-lifecycle.md` · Follow-up: Phase 8b (`...-phase-8b-preserve-column-rename.md`)

**Goal:** Replace the three-form `preserve` parameter (`None` / `list[str]` / `"*"`) with two states (`None` / `"*"`) under a single rule: **anonymous tables are scratch; named tables are job-scoped and preserved.** Delete `task_name_locks` and every coordinator that exists only to enforce single-owner semantics for the (now nonexistent) "named but not preserved" category.

**Why:** During the Phase 7 review the user observed that "preserved name" vs "non-preserved name" doesn't carry weight in practice — the only reason to give a table a name is to refer to it stably, which already implies job-scoped persistence. Concurrent `create_object(scope="job", name="x")` calls become naturally idempotent via `CREATE TABLE IF NOT EXISTS`; the lock falls away.

**Net delta:** ~150 lines deleted; user mental model becomes one sentence.

---

# Behavior summary (after this phase)

| Table type                    | Survives task exit | Survives job |
|-------------------------------|--------------------|--------------|
| `t_<id>` (anonymous)          | No                 | No (BG sweep)|
| `t_<id>` with `preserve="*"`  | Yes                | Yes (BG worker drops at job completion) |
| `j_<id>_<name>`               | Yes                | Yes (BG worker drops at job completion) |
| `p_<name>`                    | Yes                | Yes — never dropped (user-managed)      |

`preserve` collapses to:

| `preserve` | Meaning |
|------------|---------|
| `None` (default) | Named tables survive the run; anonymous `t_*` are scratch. |
| `"*"`            | Same plus anonymous `t_*` survive too — full replay / Tier 2 debugging. |

---

# Tasks

## Task 1: Narrow `Preserve` and update `resolve_preserve`

**Files:** `aaiclick/orchestration/models.py`, `aaiclick/orchestration/factories.py`, `aaiclick/orchestration/test_preserve_resolution.py`.

- [ ] Change `Preserve = list[str] | Literal["*"] | None` → `Preserve = Literal["*"] | None` in `models.py`.
- [ ] In `resolve_preserve`, drop the `isinstance(explicit, list)` branch and the defensive list-copy. Validation collapses to "must be `'*'` or `None`".
- [ ] Update `test_preserve_resolution.py`: drop the list-form tests; keep precedence + sentinel coverage.

Commit: `refactor: collapse Preserve to Literal["*"] | None`.

## Task 2: Delete the lock coordinator

**Files:** `aaiclick/orchestration/lifecycle/db_lifecycle.py`, `aaiclick/orchestration/lifecycle/test_task_name_locks.py`.

- [ ] Remove `TaskNameLock`, `TableNameCollision`, `acquire_task_name_lock`, `release_task_name_locks_for_task`, `release_task_name_locks_for_dead_tasks`.
- [ ] Delete `aaiclick/orchestration/lifecycle/test_task_name_locks.py` outright.

Commit: `cleanup: delete TaskNameLock + acquire/release helpers`.

## Task 3: Strip lock plumbing from the data layer and lifecycle interface

**Files:** `aaiclick/data/data_context/lifecycle.py`, `aaiclick/data/data_context/data_context.py`, `aaiclick/orchestration/lifecycle/task_lifecycle.py`.

- [ ] Remove `is_preserved` and `acquire_named_table_lock` from the `LifecycleHandler` base default no-ops.
- [ ] Remove the matching overrides in `TaskLifecycleHandler`. The handler's `_preserve` field, `__init__(preserve=...)`, and the per-job preserve list go away.
- [ ] Remove the `effective_scope == "job"` lock-acquire branch in `data_context.create_object`. `j_<id>_<name>` always uses `CREATE TABLE IF NOT EXISTS`. `track_table(preserved=True, owned=True)` simplifies to `track_table(owned=True)` since `preserved` is no longer per-table.
- [ ] Drop the `preserved` field from `TrackedTable` (it's no longer set anywhere).

Commit: `refactor: drop is_preserved / acquire_named_table_lock from the lifecycle interface`.

## Task 4: Strip the lock from `task_scope` and the BG worker

**Files:** `aaiclick/orchestration/orch_context.py`, `aaiclick/orchestration/background/background_worker.py`.

- [ ] Remove the `select(Job.preserve).where(Job.id == job_id)` lookup at `task_scope` start.
- [ ] Stop passing `preserve` into `TaskLifecycleHandler`.
- [ ] Remove the `release_task_name_locks_for_task` call at exit.
- [ ] Remove the `task_name_locks` DELETE in `_cleanup_at_job_completion`.
- [ ] In `_cleanup_failed_task_tables`, replace the per-job `_is_preserved` check with `name.startswith("t_")` — only `t_*` are eligible for failed-task drop; `j_<id>_*` always wait for job completion.
- [ ] Delete `_parse_preserve` and `_is_preserved` from `background_worker.py`.
- [ ] Remove the `release_task_name_locks_for_dead_tasks` call in `_cleanup_dead_workers`.

Commit: `refactor: simplify task_scope and BG cleanup — no per-task lock release`.

## Task 5: Drop `task_name_locks` from the schema

**Files:** new alembic migration.

- [ ] `alembic revision -m "drop task_name_locks"`.
- [ ] `upgrade()` drops the index + table.
- [ ] `downgrade()` recreates them empty.

Commit: `feature: alembic migration drops task_name_locks`.

## Task 6: Add the regression test + sanity check

**Files:** `aaiclick/orchestration/test_orchestration_factories.py` (or a new file).

- [ ] Add a test that two concurrent `create_object(scope="job", name="shared")` calls under the same task_scope succeed without `TableNameCollision`.
- [ ] `pytest aaiclick/ -x --no-cov` green.

Commit: `feature: regression test for concurrent named-table creation`.

---

# Done When

- `task_name_locks` table and `TaskNameLock` SQLModel are gone.
- `Preserve` is `Literal["*"] | None`.
- `TableNameCollision`, `acquire_task_name_lock`, `acquire_named_table_lock`, `is_preserved`, `_parse_preserve` no longer exist anywhere in `aaiclick/`.
- Concurrent `create_object(scope="job", name=...)` is naturally idempotent.
- `pytest aaiclick/ -x` is green; the dist matrix stays green on PG.
- Phase 8b can land independently to clean up the remaining `preserve` JSON column (cosmetic).
