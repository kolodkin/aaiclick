Phase 8 — Collapse the `preserve` API and delete `task_name_locks`
---

> Parent plan: `2026-04-25-simplify-orchestration-lifecycle.md`

**Status: implemented (single commit).**

**Goal:** Replace the three-form `preserve` parameter (`None` / `list[str]` / `"*"`) with a single boolean `preserve_all`. Delete `task_name_locks` and every coordinator that exists only to enforce single-owner semantics for the (now nonexistent) "named but not preserved" category.

**Why:** "Preserved name" vs "non-preserved name" doesn't carry weight in practice — the only reason to give a table a name is to refer to it stably, which already implies job-scoped persistence. Concurrent `create_object(scope="job", name="x")` calls become naturally idempotent via `CREATE TABLE IF NOT EXISTS`; the lock falls away. With the list form gone, the JSON column is just a clumsy boolean — collapse it to `BOOLEAN preserve_all`.

---

# Behavior summary (after Phase 8)

| Table type                  | Survives task exit | Survives job |
|-----------------------------|--------------------|--------------|
| `t_<id>` (anonymous)        | No                 | No           |
| `t_<id>` (`preserve_all=True`) | Yes             | Yes (BG worker drops at job completion) |
| `j_<id>_<name>`             | Yes                | Yes (BG worker drops at job completion) |
| `p_<name>`                  | Yes                | Yes — never dropped (user-managed)      |

`preserve_all` is a boolean: `False` (default) drops anonymous scratch on task exit; `True` keeps them for Tier 2 / full-replay debugging.

---

# What landed

## Type collapse + API
- `Preserve = list[str] | Literal["*"] | None` → deleted.
- `resolve_preserve(...) → Preserve` → renamed `resolve_preserve_all(explicit: bool | None = None, registered: bool = False) -> bool`.
- All `preserve=` kwargs renamed to `preserve_all` on `create_job`, `register_job`, `upsert_registered_job`, `run_job`, `JobFactory.__call__`.
- CLI flag: `--preserve` → `--preserve-all` (`store_true`).
- Pydantic request schemas: `RunJobRequest.preserve` and `RegisterJobRequest.preserve` → `preserve_all`.

## Lock plumbing deleted
- `TaskNameLock`, `TableNameCollision`, `acquire_task_name_lock`, `release_task_name_locks_for_task`, `release_task_name_locks_for_dead_tasks` → gone from `aaiclick/orchestration/lifecycle/db_lifecycle.py`.
- `aaiclick/orchestration/lifecycle/test_task_name_locks.py` → deleted.
- `LifecycleHandler.is_preserved`, `LifecycleHandler.acquire_named_table_lock` → removed from base.
- `TaskLifecycleHandler._preserve` field, `__init__(preserve=...)` → gone; the handler no longer needs the per-job preserve list.
- `TrackedTable.preserved` field → gone; cleanup is `(owned, pinned)` only. `track_table(preserved=...)` parameter removed.
- `data_context.create_object`'s lock-acquire branch for `scope="job"` → removed; `j_<id>_<name>` always uses `CREATE TABLE IF NOT EXISTS`.
- `task_scope.__aexit__` simplified: drop owned + unpinned + `t_*` tables, skipping the whole loop when `Job.preserve_all` is True.
- BG worker: `_parse_preserve`, `_is_preserved` → deleted; `_cleanup_failed_task_tables` drops only `t_*` (named tables wait for job completion); `_cleanup_dead_workers` no longer releases locks.

## Schema
Single migration (`b8c49269a7c6`):
- Drops `task_name_locks` and its index.
- Adds `preserve_all BOOLEAN NOT NULL DEFAULT FALSE` to `jobs` and `registered_jobs`.
- Backfills `preserve_all = TRUE` where the old `preserve = '"*"'`.
- Drops the `preserve` JSON column.

Downgrade reverses (lossy: `list[str]` preserve values cannot round-trip; the new boolean only encodes "*"/None).

---

# Done When

- [x] `task_name_locks` table and `TaskNameLock` SQLModel are gone.
- [x] `Preserve` type alias is gone.
- [x] `TableNameCollision`, `acquire_task_name_lock`, `acquire_named_table_lock`, `is_preserved`, `_parse_preserve` no longer exist.
- [x] Concurrent `create_object(scope="job", name=...)` is naturally idempotent.
- [x] `pytest aaiclick/ -x` is green on local; CI dist matrix is the gating check.
- [x] Migration round-trips on a fresh SQLite DB.
- [x] `preserve` JSON column on `jobs` / `registered_jobs` replaced with `preserve_all` BOOLEAN.
