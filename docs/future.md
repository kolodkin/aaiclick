Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# High Priority

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

**Work**:
- `aaiclick/data/data_context/data_context.py` — add `scope` kwarg to `create_object_from_value()`; route `scope="job"` through a job-id-aware name builder (pulls from orch context).
- `aaiclick/data/object/object.py` — `persistent` property recognizes both `p_*` and `j_*`; add an `is_user_persistent` / `is_job_scoped` split if callers need to distinguish.
- `aaiclick/orchestration/background/background_worker.py` — `_delete_job_data()` excludes `p_*` from the drop list; `_cleanup_expired_jobs()` adds a `SHOW TABLES LIKE 'j_<id>_%'` pass.
- `aaiclick/oplog/cleanup.py` — same prefix check update.
- Examples that use `name=` for intermediate outputs (basic-lineage, cyber-threat-feeds, imdb, nyc-taxi) migrate to `scope="job"` where appropriate; genuinely user-facing catalog tables stay as `p_*`.
- Docs across `data_context.md`, `object.md`, `orchestration.md`.

Wide blast radius — ship in its own PR. Pairs naturally with the `table_registry` → SQL move below.

## Move `table_registry` from ClickHouse to SQL

`table_registry` (table → owning `job_id` / `task_id` / `run_id`) currently lives in ClickHouse alongside `operation_log`, but it's cleanup metadata — not append-only audit. Every consumer is a keyed lookup or owner join during background cleanup, which already reads `table_context_refs` / `table_pin_refs` / `table_run_refs` from SQL.

Moving it to SQL collapses `_cleanup_unreferenced_tables` to a single query that joins unreferenced tables → registry → jobs, enabling mode-aware filtering in-database (e.g. `WHERE j.preservation_mode != 'FULL'`). The background worker stops needing a ClickHouse client for metadata scans; it only needs CH to issue the `DROP TABLE` itself.

**Work**: Alembic migration creating `table_registry` in SQL; one-time copy from CH on upgrade; flip `OrchLifecycleHandler._write_table_registry_row` to a SQL INSERT; rewrite `_lookup_table_owners` / `_cleanup_unreferenced_tables` / `_cleanup_expired_jobs` scans; drop the CH-side table.

## Retry `create_object_from_url` on Transient Upstream Failures

`create_object_from_url` currently surfaces any HTTP failure from the
remote host as a hard task failure. Transient upstream blips — `502 Bad
Gateway`, `503 Service Unavailable`, `504 Gateway Timeout`, socket
resets, DNS hiccups — are common on public datasets (Wikidata SPARQL,
HuggingFace CDN, IMDb mirrors) and routinely kill an otherwise healthy
pipeline run.

Add retries with exponential backoff (e.g. 2 s / 4 s / 8 s / 16 s, up
to 4 attempts) for a bounded set of retryable errors: `5xx` status
codes, `ConnectionError`, `TimeoutError`. Non-retryable errors (`4xx`
other than `429`, TLS failures, DNS NXDOMAIN) should still fail fast.
Settings override via `ch_settings` keyword or dedicated
`retry`/`retry_backoff` kwargs.

Example motivation: the IMDb dataset builder pipeline's Wikidata
SPARQL resolver hit a single `502 Bad Gateway` in one CI run and
failed the whole job, even though a retry 2 s later would have
succeeded. Same concern applies to HF Parquet downloads.

**Work**: wrap the internal `ch.command()` / `ch.query()` call path in
`aaiclick/data/object/url.py` with a retry decorator that inspects the
exception type and status code; ensure the retry is idempotent (same
INSERT … SELECT … LIMIT … is safe to replay since each invocation
targets a fresh target table).

## Lazy Operator Results (Operators Return Views, Not Tables)

Every operator today materializes its result into a fresh ClickHouse table via `create_object(schema)` + `INSERT INTO ... SELECT ...`. For scalar and small-result aggregations (`sum`, `nunique`, `count`, `min`, `max`, `mean`, single-key `group_by.sum`), the extra `CREATE TABLE ... ENGINE = Memory` round-trip dominates wall clock on cheap queries.

**Evidence** (1M rows, chdb 26, `aaiclick/example_projects/chdb_benchmark`):

| Operation | Native `SELECT` | aaiclick `CREATE + INSERT SELECT` | Empty `CREATE TABLE` alone |
|---------------|----------------:|----------------------------------:|---------------------------:|
| Count distinct | 3.89 ms | 9.01 ms | 4.18 ms |
| Group-by sum | 6.62 ms | 8.44 ms | — |

~60–70% of the aaiclick overhead on scalar aggregations is the DDL round-trip — a fixed ~4 ms cost paid to register a throwaway sink table in the catalog. The remaining ~30–40% is Python orchestration (Schema build, Object register, async plumbing).

**Root cause**: `operators.nunique_agg` / `operators.group_by_agg` / `_apply_aggregation` build a `Schema` in Python, then call `create_object(schema)` which emits `CREATE TABLE <result> (...) ENGINE = Memory` with column comments — just to hold a 1-row or 10-row result that the caller almost always unwraps via `.data()`. The schema is fully known in Python before the DDL is sent; the CREATE just *serializes* metadata the runtime already has.

**Proposal**: Scalar and small-result operators return a `LazyScalar` / `LazyView` wrapper carrying the same `Schema` (types, fieldtype, nullability, LowCardinality, descriptions) plus the query SQL. Materialization into a real table happens only when genuinely needed — e.g. `.materialize()`, cross-process handoff, or downstream ops that require a table source.

```python
# Today
async def nunique_agg(info, ch_client):
    schema = Schema(...)
    result = await create_object(schema)          # CREATE TABLE + comments
    await ch_client.command(f"INSERT INTO {result.table} ... SELECT count() ...")
    return result

# Lazy
async def nunique_agg(info, ch_client):
    schema = Schema(...)                          # same Schema
    sql = f"SELECT count() FROM (SELECT value FROM {info.source} GROUP BY value)"
    return LazyScalar(schema=schema, sql=sql, ch_client=ch_client)
    # .data() → one SELECT (saves the ~4 ms CREATE round-trip)
    # .materialize() → falls back to today's behavior when a table is needed
```

**What doesn't change**: `Schema`, `ColumnInfo` (including `low_cardinality`, `nullable`, `array`, `description`), column comments on **persistent** / **job-scoped** tables, cross-process handoff via table name, `open_object()` reconstruction. Metadata remains Python-side first; the CREATE TABLE stays as the serialization path for tables that need to cross a process or session boundary.

**Where a table is still required**:

- Persistent (`p_<name>`) / job-scoped (`j_<job_id>_<name>`) objects.
- Orch task outputs handed off to downstream workers.
- Repeated reads where the result should be cached.
- Joining a result as a table source (rare for scalars; broadcasting as a literal is usually better).

Add `.materialize()` as the explicit escape hatch so callers can opt in.

**Work**:
- `aaiclick/data/object/operators.py` — new `LazyScalar` / `LazyView` classes or extend existing `View`; route `nunique_agg`, `_apply_aggregation` (sum/mean/min/max/count/std/var), `group_by_agg` for small results through them.
- `aaiclick/data/object/object.py` — `.data()` on a lazy result executes the SQL directly; chain operators inline the lazy SQL as a subquery instead of reading from a table name.
- Decide group-by threshold: always lazy vs. materialize above N result rows — likely always lazy, let downstream `.copy()` or `.materialize()` decide.
- Benchmark: `chdb_benchmark` should show `Count distinct` / `Group-by sum` dropping from ~10 ms → ~5 ms at 1M rows.
- Tests: every operator test that currently asserts against a materialized table still passes (via implicit materialize-on-data or an explicit `.materialize()` in tests that introspect `.table`).

Pairs with the "scalar Object unwrapping" idea — once `.data()` is cheap, the ergonomic case of "just give me the number" becomes the fast default.

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

---

# Deferred

Items deferred until preconditions are met.

## `Object.export()` HTML Format

`.html` extension → ClickHouse `HTML` output format. The format is supported
by upstream ClickHouse but the chdb build that aaiclick ships against rejects
it with `UNKNOWN_FORMAT` (chdb appears to omit the HTML output handler). Add
the `.html` → `HTML` mapping to `_EXPORT_FORMATS` and the corresponding test
once chdb's build includes it, or once aaiclick gains a way to fall back to
clickhouse-connect for formats chdb doesn't ship.

## Comparison Page

`docs/comparison.md` — feature matrix comparing aaiclick vs Pandas, Spark, and Dask. Defer until the project has enough real-world usage to make meaningful claims.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
