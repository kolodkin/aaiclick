Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# High Priority

## `join()` Operator

Distributed join of two Objects on a key column:

```python
basics.join(ratings, on="tconst", how="left")
```

Core data operation — table-stakes for any data framework.

## Insert Advisory Lock for Concurrent Workers

Concurrent workers inserting into the same persistent Object can produce interleaved Snowflake IDs within the same millisecond.

Serialize via PostgreSQL advisory locks (`pg_advisory_lock(table_hash)`) per-table. SQLite mode is single-process and needs no lock.

## Progressive Tutorial

7-page tutorial using named snippets (`pymdownx.snippets` section markers) from existing
example files — 6 of 7 pages need zero new code. Pages: Your First Object, Operations,
Aggregations, Multi-Column Data, Views & Filters, Persistence, Orchestration. Add
`# --8<-- [start:name]` / `# --8<-- [end:name]` markers to example `.py` files, then
include specific sections in tutorial `.md` pages via snippet syntax.

Add "See Also" footers and cross-page links alongside the tutorial.

---

# Medium Priority

## Job-Scoped Persistent Table Names

Rename the `p_<name>` convention to `j_<job_id>_<name>`. Today, `create_object_from_value(val, name="kev_catalog")` creates `p_kev_catalog` — globally shared across jobs, with append-on-existing-table semantics. That's a design smell: two parallel jobs collide on the name, and the e2e test under Orch dist had to work around it with a hand-generated snowflake suffix.

Encoding the owning `job_id` into the name makes the scope explicit:

- Two jobs passing the same `name=` get isolated namespaces automatically.
- `_cleanup_expired_jobs()` drops a job's tables with `SHOW TABLES LIKE 'j_<id>_%'` — no `table_registry` lookup for these.
- `is_input_task()` detection keeps its current semantics (prefix check flips from `p_` to `j_`).
- Phase 3b replay references the original job's persistent tables explicitly as `j_<original_job_id>_<name>`.

**Breaking change**: any code that relied on `p_foo` being shared across jobs (or on the append-on-existing-table behavior) needs to migrate. The "shared across jobs" use case — if it matters — could be re-added as a separate `shared/<name>` convention later.

**Work**: update `create_object_from_value()` name handling in `aaiclick/data/data_context/data_context.py`; flip the `persistent` property + prefix checks in `aaiclick/data/object/object.py`, `aaiclick/orchestration/lineage.py::is_input_task`, `aaiclick/oplog/cleanup.py`, and `aaiclick/orchestration/background/background_worker.py`; update every example (basic-lineage, cyber-threat-feeds, imdb, nyc-taxi) + tests; docs across `data_context.md`, `object.md`, `orchestration.md`, `lineage_3_phases.md`. Wide blast radius — ship in its own PR.

## Move `table_registry` from ClickHouse to SQL

`table_registry` (table → owning `job_id` / `task_id` / `run_id`) currently lives in ClickHouse alongside `operation_log`, but it's cleanup metadata — not append-only audit. Every consumer is a keyed lookup or owner join during background cleanup, which already reads `table_context_refs` / `table_pin_refs` / `table_run_refs` from SQL.

Moving it to SQL collapses `_cleanup_unreferenced_tables` to a single query that joins unreferenced tables → registry → jobs, enabling mode-aware filtering in-database (e.g. `WHERE j.preservation_mode != 'FULL'`). The background worker stops needing a ClickHouse client for metadata scans; it only needs CH to issue the `DROP TABLE` itself. Also unblocks cleaner Phase 3 replay queries.

**Work**: Alembic migration creating `table_registry` in SQL; one-time copy from CH on upgrade; flip `OrchLifecycleHandler._write_table_registry_row` to a SQL INSERT; rewrite `_lookup_table_owners` / `_cleanup_unreferenced_tables` / `_cleanup_expired_jobs` scans; drop the CH-side table.

## Lineage: Three-Phase Debugging

Question-driven lineage debugging in three phases: graph structure (have today), targeted sampling via WHERE clauses derived from the user's question, and row-level trace using those targeted samples. Replaces random pre-sampling with on-demand, question-driven sampling.

**Design**: `docs/lineage_3_phases.md`

## Clear Task + Downstream

Reset a specific task and all its downstream tasks to PENDING — same concept as Airflow's "clear task". Upstream tasks are untouched; their output tables remain as-is. Useful for re-running part of a pipeline without re-executing the entire job. Independent of lineage — general orchestration capability.

---

# Deferred

Items deferred until preconditions are met.

## Comparison Page

`docs/comparison.md` — feature matrix comparing aaiclick vs Pandas, Spark, and Dask. Defer until the project has enough real-world usage to make meaningful claims.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
