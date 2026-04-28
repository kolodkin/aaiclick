Simplify Orchestration Lifecycle — Implementation Plan
---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the distributed-refcount task lifecycle (`OrchLifecycleHandler`, `table_run_refs`, `table_context_refs`, `PreservationMode`) with a `LocalLifecycleHandler`-based per-task scope plus an explicit `preserve` list of named tables. Inline-drop task-local tables on success; BackgroundWorker handles all cross-task and job-scoped cleanup.

**Architecture:** Tasks become DataContexts with three extras — pin/unpin for Object outputs, name-lock acquisition for non-preserved named tables, and preserved-name registration. Three SQL tables go away (`table_run_refs`, `table_context_refs`, the `preservation_mode` columns); one new SQL table comes in (`task_name_locks`). All cleanup ownership rules live in two places: `task_scope.__aexit__` (inline DROPs through the existing `AsyncTableWorker` queue, never parallel `ch_client.command` calls) and `BackgroundWorker` (every `j_<id>_*` and pinned `t_*` drop).

`task_scope` swaps `OrchLifecycleHandler` for a new `TaskLifecycleHandler` (a `LocalLifecycleHandler` subclass) that owns the SQL-side writes the orch handler did. See the spec for the full responsibilities list.

**Tech Stack:** Python 3.11+, SQLModel, Alembic, async ClickHouse client (`ChClient`), pytest with `asyncio_mode=auto`, pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-04-25-simplify-orchestration-lifecycle-design.md`

**Branch:** `claude/simplify-plab-lifecycle-YRXOm`

**Project-wide invariant:** every phase ends with `pytest aaiclick/ -x` green. The data suite (`aaiclick/data/`) is the most sensitive part — it routes through `Object.data()` → `_get_table_schema` → `table_registry.schema_doc`, so any handler swap that drops the `register_table` write path fails it wholesale.

---

# Phase Layout

Each phase is a separate file. Phases are sequential — finish one before starting the next, but each phase ends with a passing test suite so partial completion is safe.

| Phase | File                                                                                   | Theme                          |
|-------|----------------------------------------------------------------------------------------|--------------------------------|
| 1     | `2026-04-25-simplify-orchestration-lifecycle-phase-1-foundation.md`                    | Type alias, exception, Alembic migration, `preserve` columns on `Job`/`RegisteredJob` |
| 2     | `2026-04-25-simplify-orchestration-lifecycle-phase-2-api.md`                           | `resolve_preserve()`, `create_job(preserve=...)`, `@register_job(preserve=...)` |
| 3     | `2026-04-25-simplify-orchestration-lifecycle-phase-3-locks.md`                         | `TaskNameLock` SQL model + acquire/release/sweep ops |
| 4     | `2026-04-25-simplify-orchestration-lifecycle-phase-4-lifecycle.md`                     | Extend `LocalLifecycleHandler`, rewrite `task_scope()`, hook `ctx.table()` into name lock + preserved-name registration |
| 5     | `2026-04-25-simplify-orchestration-lifecycle-phase-5-bgworker.md`                      | New `BackgroundWorker` cleanup methods; integrate into `try_complete_job()` and dead-worker sweep |
| 6     | `2026-04-25-simplify-orchestration-lifecycle-phase-6-removals.md`                      | Delete `OrchLifecycleHandler`, `PreservationMode`, `TableRunRef`, `TableContextRef`, env default, `resolve_job_config()` |
| 7     | `2026-04-25-simplify-orchestration-lifecycle-phase-7-docs.md`                          | Update `docs/orchestration.md`, `docs/future.md`, examples |

---

# File Structure

## Files modified

| File                                                                            | Change                                                              |
|---------------------------------------------------------------------------------|---------------------------------------------------------------------|
| `aaiclick/orchestration/models.py`                                              | Add `Preserve` type, `preserve` JSON column on `Job` and `RegisteredJob`. Delete `PreservationMode` (Phase 6). |
| `aaiclick/orchestration/factories.py`                                           | Add `resolve_preserve()`, `create_job(preserve=...)`. Delete `resolve_job_config()` (Phase 6). |
| `aaiclick/orchestration/decorators.py` / `registered_jobs.py`                   | `@register_job(preserve=...)` parameter.                            |
| `aaiclick/orchestration/orch_context.py`                                        | Rewrite `task_scope()` over `TaskLifecycleHandler`. Delete `OrchLifecycleHandler` (Phase 6).    |
| `aaiclick/orchestration/lifecycle/db_lifecycle.py`                              | Add `TaskNameLock`, `TableNameCollision`. Delete `TableRunRef` / `TableContextRef` (Phase 6). Add `preserved` column to `TableRegistry` (alongside the existing `schema_doc` from migration `161cfe0f1117`). |
| `aaiclick/orchestration/lifecycle/task_lifecycle.py` (NEW)                      | `TaskLifecycleHandler(LocalLifecycleHandler)` — see spec § `TaskLifecycleHandler` responsibilities. |
| `aaiclick/orchestration/env.py`                                                 | Delete `get_default_preservation_mode()` and `AAICLICK_DEFAULT_PRESERVATION_MODE` (Phase 6). |
| `aaiclick/orchestration/background/background_worker.py`                        | Add `_cleanup_failed_task_tables()`, `_cleanup_orphan_scratch_tables()`, `_cleanup_at_job_completion()`. Extend `_cleanup_dead_workers()`. Phase 1 stubs `_cleanup_unreferenced_tables()` to no-op; Phase 6 deletes it. |
| `aaiclick/orchestration/background/handler.py` (+ `pg_handler.py`, `sqlite_handler.py`) | Add query helpers for new cleanup methods and lock ops. |
| `aaiclick/data/data_context/lifecycle.py`                                       | Extend `LocalLifecycleHandler` with `track_table(name, *, preserved)`, `mark_pinned(name)`, `iter_tracked_tables()` so task_scope can decide what to drop on exit. Drops still go through the existing `AsyncTableWorker` queue (no parallel `ch_client.command`). |
| `aaiclick/orchestration/migrations/versions/<new>_simplify_lifecycle.py`        | Single new migration: `preserve` columns, drop `table_run_refs` / `table_context_refs`, add `preserved` to `table_registry`, create `task_name_locks`. |

## Files created (tests)

| File                                                                            | Covers                                                              |
|---------------------------------------------------------------------------------|---------------------------------------------------------------------|
| `aaiclick/orchestration/test_preserve_resolution.py`                            | Phase 2: `resolve_preserve()` precedence.                           |
| `aaiclick/orchestration/lifecycle/test_task_name_locks.py`                      | Phase 3: lock acquire / release / collision / dead-task release.    |
| `aaiclick/orchestration/test_task_scope_lifecycle.py`                           | Phase 4: success-path inline DROP.                                  |
| `aaiclick/orchestration/test_task_scope_failure.py`                             | Phase 4: failure-path leaves `j_<id>_*`, drops only unpinned `t_*`. |
| `aaiclick/orchestration/test_task_named_table_collision.py`                     | Phase 4: `TableNameCollision` for concurrent non-preserved names.   |
| `aaiclick/orchestration/background/test_cleanup_at_job_completion.py`           | Phase 5: drop all `j_<id>_*` + pinned `t_*` at job end.             |
| `aaiclick/orchestration/background/test_cleanup_orphan_scratch.py`              | Phase 5: orphan `t_*` sweep skips pinned ones.                      |

## Files deleted (Phase 6)

- All references to `PreservationMode`, `OrchLifecycleHandler`, `TableRunRef`, `TableContextRef`, `_cleanup_unreferenced_tables`, `resolve_job_config`, `get_default_preservation_mode`.

---

# How to Work This Plan

1. **One phase at a time.** Each phase ends with `pytest aaiclick/orchestration/ -x` green.
2. **Each phase has one Alembic-touch point at most.** Only Phase 1 generates the migration; later phases work against the migrated schema.
3. **TDD.** Every code change follows: write failing test → run → minimal implementation → run → refactor → commit.
4. **Commit after every task.** Conventional commits (`feature:`, `bugfix:`, `refactor:`, `cleanup:`). Message must end with the standard "https://claude.ai/code/..." line stripped — this project uses HEREDOC commits per `CLAUDE.md`.
5. **Stop and ask** if a step's expected behavior conflicts with what the existing code does. The spec is the source of truth, but undocumented coupling may exist.

---

# Acceptance Criteria

- `pytest aaiclick/ -x` passes (all phases applied).
- `alembic upgrade head` succeeds on a fresh database.
- `alembic downgrade -1` round-trips for any DB created from the previous head.
- `grep -rn "PreservationMode\|OrchLifecycleHandler\|table_run_refs\|table_context_refs\|resolve_job_config\|AAICLICK_DEFAULT_PRESERVATION_MODE" aaiclick/` returns nothing (after Phase 6).
- `docs/orchestration.md` reflects the new lifecycle.
- An end-to-end job run with `preserve=["training_set"]` keeps `j_<id>_training_set` for the run and drops it at job completion (verified by integration test in Phase 5).
