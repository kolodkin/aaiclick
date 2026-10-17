Lazy ClickHouse Client Implementation Plan
---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `orch_context()` opens ClickHouse lazily (no `with_ch`), task info is scoped to the task run, and `data_context` is renamed `object_context`.

**Architecture:** A `LazyChClient` proxy is set once at `orch_context()` entry and creates the real client on first await. `execute_task` wraps its body in `current_task_info(...)`. The rename is a mechanical `git mv` plus a whole-word replace.

**Tech Stack:** Python 3 asyncio, ContextVars, chdb / clickhouse-connect, pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-10-17-lazy-ch-client-design.md`

# Global Constraints

- No backwards-compatibility aliases for `with_ch`, `orch_scope_with_ch`, `set_current_task_info` or `data_context`.
- `get_ch_client()` stays sync.
- Imports at the top of the file; ruff must pass.

# Review Focus

- Concurrent first queries from `asyncio.gather` children → exactly one real client (Task 2 test).
- `orch_context()` with no CH query never calls `create_ch_client` (Task 2 test).
- `create_ch_client` failing on first query → error surfaces, context still unwinds (Task 2 test).
- mp-worker parent never touches CH → existing `orch_module_ctx_no_ch` modules stay green (Task 2 verification).
- Task info does not outlive `execute_task` (Task 1 test).

# Task 1: Scoped task info

**Files:** `aaiclick/orchestration/execution/runner.py`, `execution_worker_context.py`, `execution/__init__.py`, `aaiclick/orchestration/test_image_injection.py`, `aaiclick/orchestration/execution/test_execution.py`

- [ ] Test `test_execute_task_restores_task_info`: run a module task via `execute_task(task)` under `orch_ctx`; afterwards `get_current_task_info()` raises `RuntimeError("No task is currently executing")`.
- [ ] Run it; expect FAIL.
- [ ] Replace `set_current_task_info(...)` in `execute_task` with `with current_task_info(TaskInfo(...)):` around the rest of the body. Delete `set_current_task_info`; drop its export; switch the `test_image_injection.py` callers to the context manager.
- [ ] Run `test_execution.py` + `test_image_injection.py`; expect PASS. Commit.

# Task 2: Lazy ClickHouse client, drop `with_ch`

**Files:** `aaiclick/data/data_context/ch_client.py` (add `LazyChClient`), `aaiclick/orchestration/orch_context.py`, all `orch_context(with_ch=...)` callers, `aaiclick/server/deps.py` + routers, `aaiclick/__main__.py`, `aaiclick/testing.py`, `aaiclick/orchestration/test_orch_context.py`, docs listed in the spec.

**Produces:** `class LazyChClient` (satisfies `ChClient`, plus `raw_stream`); `orch_context()` with no parameters.

- [ ] Tests in `test_orch_context.py`: no query → `create_ch_client` not called; `asyncio.gather` of 3 first queries → called once; patched `create_ch_client` raising → first query raises, vars reset, engine disposed once; nested `orch_context` reuses the outer client. Remove the `with_ch` tests.
- [ ] Run; expect FAIL.
- [ ] Implement `LazyChClient` (lock-guarded `_get()`, delegating methods, `close()` only if created). `orch_context()` reuses an existing `_ch_client_var` value or sets a new `LazyChClient` and closes it on exit.
- [ ] Remove `with_ch` from every caller; delete `orch_scope_with_ch`; update docs.
- [ ] Run the full suite with `-n auto`; expect PASS. Commit.

# Task 3: Rename `data_context` → `object_context`

- [ ] `git mv` the package, the module and the user-guide page; whole-word replace `data_context` → `object_context` and `DataContext` → `ObjectContext` across `*.py`, `*.md`, `*.yml`, notebooks.
- [ ] Ruff + full suite; expect PASS. Commit.
- [ ] Delete this plan and the spec; push; run `check-pr`.
