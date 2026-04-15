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

## Two-Tier Persistent Tables: `p_*` (user-managed) + `j_<job_id>_*` (job-scoped)

Today only one "persistent" tier exists — `p_<name>` created via `create_object_from_value(val, name=...)`. It serves two conflicting goals:

1. **User-managed durable data** ("survives everything, only the user deletes it")
2. **Pinned intermediate outputs** ("survives one job's cleanup so downstream tasks can reference it by name")

The two goals are both needed but have opposite cleanup rules, and the current code half-supports both:

- `_cleanup_unreferenced_tables()` correctly skips `p_*` (they don't get refcount-dropped), so goal 1 looks honored mid-job.
- But `_delete_job_data()` at `background_worker.py:323` drops **every** table registered to an expired job via `table_registry`, which today includes `p_*` — so `AAICLICK_JOB_TTL_DAYS` silently nukes user-managed persistent tables. Pre-existing bug.
- And two parallel jobs using `name="kev_catalog"` both write to `p_kev_catalog` (append-on-existing), which is surprising at best.

**Proposed split**:

| Tier         | Prefix             | Lifetime                                           | API                                                             |
|--------------|--------------------|----------------------------------------------------|-----------------------------------------------------------------|
| User tables  | `p_<name>`         | Forever; only the user deletes via `data delete`  | `create_object_from_value(val, name="foo")` (unchanged)         |
| Job-scoped   | `j_<job_id>_<name>` | Until the owning job TTL-expires                  | `create_object_from_value(val, name="foo", scope="job")` or new helper |

**Invariant changes**:

- `p_*` is **exempt** from `_delete_job_data()` / `_cleanup_expired_jobs()` — add a `NOT LIKE 'p\_%'` guard in the `table_registry` drop list (or stop registering `p_*` in `table_registry` in the first place and rely on a separate `persistent_tables` catalog).
- `j_<job_id>_<name>` is **always** cleaned up at job TTL via a pure prefix match (`SHOW TABLES LIKE 'j_<id>_%'`) — no `table_registry` lookup needed.
- Two parallel jobs never collide on `j_<id>_<name>` because `job_id` is globally unique.
- `is_input_task()` detects **either** tier as an input: persistent tables that survive cleanup are valid replay inputs.

**Replay (Phase 3b) implications**: a replay job references the original's job-scoped inputs as `j_<original_job_id>_<name>`. User-tier `p_<name>` is already referenced by name. Both are stable across the replay.

**Work**:
- `aaiclick/data/data_context/data_context.py` — add `scope` kwarg to `create_object_from_value()`; route `scope="job"` through a job-id-aware name builder (pulls from orch context).
- `aaiclick/data/object/object.py` — `persistent` property recognizes both `p_*` and `j_*`; add an `is_user_persistent` / `is_job_scoped` split if callers need to distinguish.
- `aaiclick/orchestration/background/background_worker.py` — `_delete_job_data()` excludes `p_*` from the drop list; `_cleanup_expired_jobs()` adds a `SHOW TABLES LIKE 'j_<id>_%'` pass.
- `aaiclick/orchestration/lineage.py::is_input_task` — recognize both prefixes.
- `aaiclick/oplog/cleanup.py` — same prefix check update.
- Examples that use `name=` for intermediate outputs (basic-lineage, cyber-threat-feeds, imdb, nyc-taxi) migrate to `scope="job"` where appropriate; genuinely user-facing catalog tables stay as `p_*`.
- Docs across `data_context.md`, `object.md`, `orchestration.md`, `lineage_3_phases.md`.

Wide blast radius — ship in its own PR. Pairs naturally with the `table_registry` → SQL move below.

## Move `table_registry` from ClickHouse to SQL

`table_registry` (table → owning `job_id` / `task_id` / `run_id`) currently lives in ClickHouse alongside `operation_log`, but it's cleanup metadata — not append-only audit. Every consumer is a keyed lookup or owner join during background cleanup, which already reads `table_context_refs` / `table_pin_refs` / `table_run_refs` from SQL.

Moving it to SQL collapses `_cleanup_unreferenced_tables` to a single query that joins unreferenced tables → registry → jobs, enabling mode-aware filtering in-database (e.g. `WHERE j.preservation_mode != 'FULL'`). The background worker stops needing a ClickHouse client for metadata scans; it only needs CH to issue the `DROP TABLE` itself. Also unblocks cleaner Phase 3 replay queries.

**Work**: Alembic migration creating `table_registry` in SQL; one-time copy from CH on upgrade; flip `OrchLifecycleHandler._write_table_registry_row` to a SQL INSERT; rewrite `_lookup_table_owners` / `_cleanup_unreferenced_tables` / `_cleanup_expired_jobs` scans; drop the CH-side table.

## Lineage: Three-Phase Debugging

Question-driven lineage debugging in three phases: graph structure (have today), targeted sampling via WHERE clauses derived from the user's question, and row-level trace using those targeted samples. Replaces random pre-sampling with on-demand, question-driven sampling.

**Design**: `docs/lineage_3_phases.md`

## Clear Task + Downstream

Reset a specific task and all its downstream tasks to PENDING — same concept as Airflow's "clear task". Upstream tasks are untouched; their output tables remain as-is. Useful for re-running part of a pipeline without re-executing the entire job. Independent of lineage — general orchestration capability.

## ClickHouse Migration Framework

aaiclick has no migration system for the ClickHouse side. Alembic manages the SQL schema (`jobs`, `tasks`, `dependencies`, `registered_jobs`, …), but ClickHouse tables created via the `ChClient` — `operation_log`, `table_registry`, all `p_*` / `t_*` / `j_*` data tables produced at runtime — are created with `CREATE TABLE IF NOT EXISTS` in `aaiclick/oplog/models.py` plus a column-existence validator. No versions, no history, no upgrade path.

The consequence: any DDL change in the Python source that would need to alter an existing table is silently a no-op on installs that already have it. Today this has bitten the `operation_log` `ORDER BY` change; it will keep biting every time anything structural changes on the CH side. Column types, new required columns, MergeTree key changes, TTL clauses, materialized projections, etc. all need a coordinated server-side update that the current setup cannot perform.

Also relevant: ClickHouse's own `ALTER TABLE` is limited — `MODIFY ORDER BY` can only append freshly added columns to the sort key, you can't reshape existing ones without rebuilding the table. So even a "real" migration framework has to handle per-change execution strategies (pure ALTER, shadow-table-rebuild, or drop-and-recreate with manual data move), not just a linear script runner.

**What a minimal framework would look like**:

- A `schema_version` table in ClickHouse tracked per-database.
- Versioned DDL scripts under `aaiclick/oplog/migrations/` (or a broader `aaiclick/ch_migrations/`) applied in order by `init_oplog_tables()` on startup.
- Each script declares its own execution strategy — inline `ALTER`, shadow-table rewrite, or a Python callable for data-move logic.
- A `--dry-run` mode for operators.
- Column validator (`_validate_schema`) grows a version check and surfaces a clear error ("your table is at v3, code expects v5, run `aaiclick migrate`").

**Alternatives to building a framework**:

- **Release-notes recipe** — document a maintenance step per release. Zero code, high operator burden, easy to miss.
- **Per-change maintenance CLIs** — `aaiclick maintenance rebuild-oplog`, etc. Works but doesn't scale past a handful of changes.

No action today — fresh installs keep working, existing installs degrade gracefully at worst. Revisit once there is a third structural CH-side change (which makes the per-change CLI approach untenable) or once a change actually breaks (not just slows down) an existing install.

## Structural-First Lineage Forest Walker

`aaiclick/oplog/lineage_forest.py::build_forest` walks backward from every strategy-matched row in the target, up to `MAX_FOREST_ROOTS=200`. Each root issues one oplog lookup + one value-column fetch per hop, memoized by `(table, aai_id)` so shared ancestors are deduped. For a 200-root × 5-hop pipeline with no DAG collapse, that's ~2000 queries — parallelized via `asyncio.gather` but still a lot of round-trips.

`collapse_to_routes` then throws ~95% of that work away: 200 rows sharing a pipeline shape become one `Route` record with `match_count=200`, aggregated leaf/root value lists, and `MAX_EXEMPLARS_PER_ROUTE=5` concrete paths. The rendered prompt context stays small (routes × 5 exemplars × hops ≈ tens of lines) regardless of match count, so there is **no LLM context bloat** — the waste is purely in fetch work.

**Smarter algorithm for count-preserving pipelines** (multiply, add, concat, with_columns, rename, view — the common case):

1. **Walk DAG structure once**, not per-row. Start from the target's oplog row, follow `kwargs` → upstream ops → recurse. One query per *producing op*, not per row. Enumerates the routes directly.
2. **Match count comes free**: `length(result_aai_ids)` on the target's oplog row already tells you how many rows fit each route.
3. **Exemplars in one batched fetch per leaf**: pick the first N aai_ids at the target, positionally map backward through each hop's `kwargs_aai_ids` in memory (no queries), then issue one `SELECT aai_id, value FROM leaf WHERE aai_id IN {ids:Array(UInt64)}` per leaf table to grab the real column values.

Total drops from **O(roots × depth)** oplog + value fetches to **O(route_shapes × depth)** structural fetches + **O(route_shapes)** batched exemplar fetches. For basic_lineage that's ~6 queries instead of ~12; for a 10k-matched-row pipeline it's still ~10 queries instead of ~20k.

**Why the current per-row walker exists anyway**: uniformity breaks when the pipeline contains `insert`, `group_by`, or (eventually) `join` — different matched roots can take different DAG paths or collapse together in ways the structural walk can't predict from the target alone. The current implementation is correct for every topology; the smarter one is only correct when every hop is count-preserving.

**Work**:

- Add a uniformity probe to `_walk` (or a new structural pass): walk once, detect whether every hop's `result_aai_ids` length matches its source's, and whether each source row maps 1:1 to a result row.
- When uniform, route enumeration runs structurally and exemplar fetches batch per leaf table.
- When divergent, fall back to the current per-row walker for the affected subtree.
- Keep the memoization cache — it still wins when shared ancestors appear within the per-row fallback.

No action today. Revisit when a strategy match set pushes the per-row walker into noticeable latency (> ~1s of fetch time on chdb, or > ~5s on remote ClickHouse) on a real pipeline. The `MAX_FOREST_ROOTS=200` cap buys breathing room until then.

---

# Deferred

Items deferred until preconditions are met.

## Comparison Page

`docs/comparison.md` — feature matrix comparing aaiclick vs Pandas, Spark, and Dask. Defer until the project has enough real-world usage to make meaningful claims.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
