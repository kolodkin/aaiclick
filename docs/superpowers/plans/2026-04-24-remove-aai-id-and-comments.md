# Remove `aai_id` and Table Comments — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drop the hidden `aai_id` column and ClickHouse column `COMMENT`s, move schema metadata into a `schema_json` column on the SQL `table_registry`, and tighten the operator contract so cross-table binary ops and `.data()` no longer rely on an implicit ordering key.

**Architecture:** Greenfield refactor (no backwards compatibility). Schema becomes a single Pydantic document (`SchemaView`) stored once in SQL when a table is created, replacing the current split between `system.columns` + per-column YAML comments. Operator SQL stops referencing `aai_id` and instead requires callers to supply an explicit `order_by` via `View(order_by=...)` for cross-table binary ops, and a `limit=1000` safety cap makes `.data()` safe without one.

**Tech Stack:** Python 3.12+, Pydantic v2, ClickHouse (via `clickhouse-connect` / chdb), PostgreSQL/SQLite via SQLModel + Alembic, pytest + pytest-asyncio.

**Spec:** [`docs/superpowers/specs/2026-04-23-remove-aai-id-and-comments-design.md`](../specs/2026-04-23-remove-aai-id-and-comments-design.md)

---

## Phases

This plan is split into six phase files. Execute in order — later phases depend on earlier ones.

| # | Phase                                                                                              | What it delivers                                                                                      |
|---|----------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| 1 | [Registry schema & Pydantic model](./2026-04-24-remove-aai-id-and-comments_phase_1.md)             | `fieldtype` on `ColumnView`/`SchemaView`; Alembic migration adds `schema_json` to `table_registry`.   |
| 2 | [Schema reads from registry](./2026-04-24-remove-aai-id-and-comments_phase_2.md)                   | `_get_table_schema` reads `table_registry.schema_json`; `Schema` ↔ `SchemaView` conversion helpers.   |
| 3 | [DDL without `aai_id` or COMMENTs](./2026-04-24-remove-aai-id-and-comments_phase_3.md)             | `create_object`, `CopyInfo`, `build_order_by_clause`, ingest paths emit clean DDL + write registry.   |
| 4 | [Operator contract](./2026-04-24-remove-aai-id-and-comments_phase_4.md)                            | Cross-table `a + b` requires `View(order_by=...)`; scalar broadcast & aggregation SQL drop `aai_id`.  |
| 5 | [`.data()` kwargs + `View` override](./2026-04-24-remove-aai-id-and-comments_phase_5.md)           | `.data(order_by=, offset=, limit=)` with `limit=1000` default; View kwargs override.                  |
| 6 | [Cleanup: ColumnMeta, renderer, docs](./2026-04-24-remove-aai-id-and-comments_phase_6.md)          | Delete `ColumnMeta`; drop `aai_id` skip in CLI renderer; update `docs/object.md` etc.                 |

## Execution Order & Commits

Each phase file contains its own numbered tasks with TDD steps (failing test → implementation → passing test → commit). Finish one phase fully (including its final "phase-complete" commit) before starting the next.

**Conventional commit types** (from `CLAUDE.md`): `feature:`, `bugfix:`, `refactor:`, `cleanup:`. Multi-type allowed: `[refactor, cleanup]: ...`.

## Self-Review Checklist

Before declaring the plan done, verify:

1. **Spec coverage** — every section in the spec ("Physical Table Shape", "Registry Metadata", "Operator Contract", "Schema Reconstruction & Data Flow", "Testing") maps to at least one task across the six phase files.
2. **No `aai_id` references remain** — `grep -r aai_id aaiclick/ docs/` after Phase 6 should return zero production hits (tests explicitly asserting absence are OK).
3. **Registry row invariant holds** — every `create_object` / copy / ingest path that creates a ClickHouse table also writes a `table_registry` row with `schema_json`.
4. **Type consistency** — `SchemaView.fieldtype: Literal["s","a","d"]` and `ColumnView.fieldtype: Literal["s","a"]` are used identically in every task that touches them.
