Technical Gaps Review
---

Review date: 2026-04-07

# Unimplemented Features

## High Priority (from `docs/future.md`)

| Feature                          | Impact                                                                 |
|----------------------------------|------------------------------------------------------------------------|
| `join()` operator                | Table-stakes data operation — no way to combine two Objects on a key   |
| Insert advisory lock             | Concurrent workers can interleave rows within the same millisecond     |
| Progressive tutorial             | No guided onboarding path for new users                                |

## Medium Priority (from `docs/future.md`)

| Feature                          | Impact                                                                   |
|----------------------------------|--------------------------------------------------------------------------|
| Graceful worker stop via CLI     | Workers can only be stopped via `SIGTERM`, no clean database-driven stop |
| Schema-aware agent context       | AI agents hallucinate column names — no schema in context                |
| Lineage `aai_id` uniqueness      | Row-level tracing breaks across `insert()`/`concat()` boundaries         |
| Oplog data lifecycle             | `table_registry` and `_sample` tables have no TTL/cleanup                |

## Oplog Activation (from `docs/oplog.md`)

`data_context()` does not yet accept an `oplog` parameter. The entire oplog activation flow
(Phase 3) is unimplemented — the collector exists but cannot be wired into the context manager.
This blocks provenance tracking in production use.

## UI Dashboard (from `docs/ui.md`)

`docs/ui.md` defines a full Preact + FastAPI dashboard specification. No implementation exists —
no frontend code, no FastAPI backend, no WebSocket layer. The spec is a design document only.

---

# Known Technical Debt (from `docs/technical_debt.md`)

| Item                                      | Location                                     | Status              |
|-------------------------------------------|----------------------------------------------|---------------------|
| chdb `url()` table function workaround    | `chdb_client.py` — regex rewrite to `file()` | Waiting on upstream |
| clickhouse-connect async `FutureWarning`  | `clickhouse_client.py`, `background_worker.py`| Waiting on v1.0     |
| `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true` | `.github/workflows/test.yaml`                 | Waiting on dorny v3 |

---

# Test Coverage Gaps

## Source modules with no dedicated test file

Many core modules lack corresponding test files. Notable gaps:

| Module                                          | Risk                                        |
|-------------------------------------------------|---------------------------------------------|
| `orchestration/decorators.py`                   | Core `@task`/`@job` decorator logic         |
| `orchestration/orch_context.py`                 | Context manager for orchestration sessions  |
| `orchestration/operators.py`                    | `map`/`reduce` orchestration operators      |
| `orchestration/result.py`                       | `TaskResult` serialization                  |
| `orchestration/sql_context.py`                  | SQL session management                      |
| `orchestration/task_registry.py`                | Task registration and lookup                |
| `orchestration/lifecycle/db_lifecycle.py`       | Database lifecycle management               |
| `orchestration/execution/claiming.py`           | Task claiming and cancellation              |
| `orchestration/execution/runner.py`             | Core task execution engine                  |
| `orchestration/execution/worker_context.py`     | Worker context setup                        |
| `orchestration/background/handler.py`           | Background handler dispatch                 |
| `orchestration/background/background_worker.py` | Background worker loop                      |
| `data/data_context/ch_client.py`                | Client factory dispatch                     |
| `data/data_context/chdb_client.py`              | Embedded chdb client (url rewrite logic)    |
| `data/data_context/clickhouse_client.py`        | Distributed ClickHouse client               |
| `data/object/data_extraction.py`                | `extract_scalar_data` and data retrieval    |
| `data/object/ingest.py`                         | `insert()`/`concat()` core logic            |
| `data/object/transforms.py`                     | Unary transform implementations             |
| `data/object/url.py`                            | URL loading logic                           |
| `data/object/cli.py`                            | Object CLI commands                         |
| `data/sql_utils.py`                             | SQL utility functions                       |
| `oplog/cleanup.py`                              | Oplog cleanup routines                      |
| `oplog/sampling.py`                             | Row sampling for lineage                    |
| `oplog/oplog_api.py`                            | Oplog public API                            |
| `oplog/lineage.py`                              | Graph traversal queries                     |
| `ai/config.py`                                  | AI configuration                            |
| `ai/agents/tools.py`                            | AI agent tool definitions                   |
| `backend.py`                                    | URL parsing and backend detection           |
| `snowflake_id.py`                               | Has `test_snowflake.py` (covered)           |

Some of these are indirectly tested through integration tests (e.g., `ingest.py` via `test_insert.py`,
`transforms.py` via `test_unary_transforms.py`). However, there are no unit-level tests for the
orchestration execution pipeline (`runner.py`, `claiming.py`, `worker_context.py`) or the background
worker, which are critical paths.

## Inline imports in test files (violates CLAUDE.md)

Test files that import inside functions instead of at the top of the file:

| File                                        | Lines                  |
|---------------------------------------------|------------------------|
| `data/object/test_nullable.py`              | 365                    |
| `data/object/test_count_if.py`              | 73                     |
| `orchestration/execution/test_execution.py` | 227, 376, 388, 549-586|
| `orchestration/background/test_scheduler.py`| 16                     |

---

# Code Quality Issues

## `Any` type usage

`CLAUDE.md` states "Never use `Any` as a shortcut to avoid proper typing." The following files
use `Any` in signatures or type hints:

| File                                   | Occurrences | Notes                                      |
|----------------------------------------|-------------|--------------------------------------------|
| `data/object/data_extraction.py`       | 1           | `extract_scalar_data() -> Any`             |
| `oplog/lineage.py`                     | 2           | `_to_dict(kwargs_raw: Any)`, `_to_aai_ids_dict(raw: Any)` |
| `orchestration/execution/runner.py`    | 7           | Multiple functions with `Any` params/returns|
| `orchestration/models.py`              | 2           | `model_post_init(__context: Any)` (SQLModel pattern) |
| `orchestration/result.py`              | 1           | `TaskResult.data: Any`                     |
| `orchestration/decorators.py`          | 3           | Serialization helpers                      |
| `orchestration/jobs/queries.py`        | 1           | `get_job_result() -> Any`                  |

The `model_post_init(__context: Any)` usages are dictated by SQLModel/Pydantic and are
acceptable. The rest could benefit from more specific types — particularly `runner.py` where
tighter types would improve safety in the task execution hot path.

## Single TODO comment

`aaiclick/ai/conftest.py:25` — TODO about rechecking when LiteLLM fixes an Ollama code path.
Minor, but should be tracked or resolved.

---

# Documentation vs Implementation Gaps

| Documentation claim                        | Actual state                                              |
|--------------------------------------------|-----------------------------------------------------------|
| `docs/data_context.md` references path     | Incorrect: says `aaiclick/data/data_context.py` but the   |
| `aaiclick/data/data_context.py`            | actual module is `aaiclick/data/data_context/data_context.py` |
| `docs/oplog.md` Phase 3 activation         | No code exists to wire oplog into `data_context()`        |
| `docs/ui.md` full dashboard spec           | Zero implementation — spec only                           |
| `docs/future.md` "Comparison Page"         | Deferred — no `docs/comparison.md`                        |
| `docs/future.md` "Changelog"              | Deferred — no `docs/changelog.md`                         |

---

# Summary

**Critical gaps** (blocking real-world usage):
1. No `join()` operator — cannot combine datasets on keys
2. No insert advisory lock — data corruption risk with concurrent workers
3. Oplog activation not wired into `data_context()` — provenance tracking is inert

**Quality gaps** (maintainability risk):
4. Orchestration execution pipeline has minimal test coverage
5. 15+ `Any` type annotations in core paths
6. 11 inline imports in test files

**Documentation gaps**:
7. UI spec has no implementation
8. One stale path reference in `docs/data_context.md`
9. Oplog Phase 3 and progressive tutorial remain unstarted
