Future Plans
---

Unimplemented features and planned work across aaiclick. See individual spec docs for context.

---

# Orchestration

## Graceful Worker Stop via CLI

Add `aaiclick worker stop` — writes a stop request to the database so the worker finishes its current task then exits cleanly, instead of receiving `SIGTERM` directly.

## flatMap() and join() Operators

- `flatMap()` — like `map()` but each callback returns multiple rows, flattened into the output Object
- `join()` — distributed join of two Objects on a key column (e.g. `basics.join(ratings, on="tconst", how="left")`)

## Operation Provenance Integration (Phase 3)

Wire `OplogCollector` into `execute_task()` so all jobs automatically capture provenance with no user code changes. Add `data_context(oplog=collector)` support. Spec: `docs/ai.md`, target: `aaiclick/orchestration/execution.py`.

---

# AI Agents

## Schema-Aware Agent Context

Both `debug_agent` and `lineage_agent` build context from the oplog graph but never include table schemas, causing the LLM to hallucinate column names. Two improvements:

1. **Schema injection** — fetch `DESCRIBE TABLE` for every node before the agentic loop and include in the initial context message.
2. **`get_column_stats` tool** — replace `get_stats(table, column)` with a schema-first tool that returns stats for all columns without requiring the LLM to know column names upfront.

---

# Oplog

## Table Lifecycle & Cleanup (Phase 3)

On job completion, replace each ephemeral table with a 10-row sample so `operation_log` references remain valid for AI lineage agents. Rename back to the original table name; persistent tables (`p_` prefix) excluded. Integrate into `PgCleanupWorker`.

## Pinned Row Sampling (Phase 5)

`pin_rows("my_table", where="value < 5")` — user-defined predicates that guarantee semantically important rows survive cleanup. Phase 3 preserves an arbitrary 10 rows; Phase 5 lets specific rows be guaranteed.

---

# Data / Object API

## `literal()` Computed Helper

Add `literal(value, type)` to `aaiclick/data/transforms.py` as a convenience wrapper over `Computed` for constant columns. `str` → `'value'`, `bool` → `true`/`false`, `int`/`float` → bare numeric. Export alongside `cast` and `split_by_char`.

---

# Documentation

## Progressive Tutorial

7-page tutorial using named snippets (`pymdownx.snippets` section markers) from existing
example files — 6 of 7 pages need zero new code. Pages: Your First Object, Operations,
Aggregations, Multi-Column Data, Views & Filters, Persistence, Orchestration. See
`docs/docs_implementation_plan.md` — "Reuse Strategy: Named Snippets" for the snippet
marker plan and page-to-source mapping.

## Cross-Page Links

Add "See Also" footers to guide pages linking to related examples and API reference.
Add header links from example pages back to relevant guide sections. Implement alongside
the tutorial.

## Comparison Page

`docs/comparison.md` — feature matrix comparing aaiclick vs Pandas, Spark, and Dask across
dimensions like compute engine, data location, setup complexity, memory limits, SQL interop,
and built-in orchestration. Defer until the project has enough real-world usage to make
meaningful claims.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
