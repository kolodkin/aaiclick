Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# High Priority

## Progressive Tutorial

7-page tutorial using named snippets (`pymdownx.snippets` section markers) from existing
example files — 6 of 7 pages need zero new code. Pages: Your First Object, Operations,
Aggregations, Multi-Column Data, Views & Filters, Persistence, Orchestration. Add
`# --8<-- [start:name]` / `# --8<-- [end:name]` markers to example `.py` files, then
include specific sections in tutorial `.md` pages via snippet syntax.

Add "See Also" footers and cross-page links alongside the tutorial.

---

# Medium Priority

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

Pairs with the "scalar Object unwrapping" idea — once `.data()` is cheap, the ergonomic case of "just give me the number" becomes the fast default. Also a hard precondition for the `.as_()` postfix naming entry below: naming only lands cleanly when it can ride on the first CREATE.

## Clear Task + Downstream

Reset a specific task and all its downstream tasks to PENDING — same concept as Airflow's "clear task". Upstream tasks are untouched; their output tables remain as-is. Useful for re-running part of a pipeline without re-executing the entire job. Independent of lineage — general orchestration capability.

## ClickHouse Migration Framework

aaiclick has no migration system for the ClickHouse side. Alembic manages the SQL schema (`jobs`, `tasks`, `dependencies`, `registered_jobs`, `table_registry`, …), but ClickHouse tables created via the `ChClient` — `operation_log`, all `p_*` / `t_*` / `j_*` data tables produced at runtime — are created with `CREATE TABLE IF NOT EXISTS` in `aaiclick/oplog/models.py` plus a column-existence validator. No versions, no history, no upgrade path.

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

## Postfix Naming for Operator Results (`.as_()`)

**Depends on Lazy Operator Results above.** Without lazy, `.as_()` degrades to a post-hoc `RENAME TABLE` or a Python-only alias — both unsatisfactory (see below).

Today every arithmetic / comparison / boolean operator on `Object` materializes its result into an auto-generated `t_<snowflake>` table. There is no way to attach a stable name to the output of `prices * quantities` or `revenue + bonus` — only `create_object*` accepts a `name`. Pipelines that mix named source objects with anonymous intermediate results read inconsistently in lineage graphs and are harder to debug since the agent has to deduce intermediate identity from operations rather than names.

**Proposal**: a single postfix method, `.as_(name, *, scope="task")`, that names the result of any expression — arithmetic, aggregation, group-by, comparison. One method on the lazy result type covers the whole operator surface; no kwarg sweep through `__add__` / `__mul__` / `_apply_aggregation` / `group_by_agg`.

```python
# With lazy operators in place
lazy = prices * quantities                  # no DDL — Schema + SQL only
revenue = await lazy.as_("revenue")         # CREATE TABLE t_revenue ... INSERT SELECT ...

by_region = await (
    sales.group_by("region").sum("amount")
).as_("revenue_by_region", scope="persistent")

# Chains stay anonymous when no name is needed
margin = (revenue - costs) / revenue
```

**Why lazy is required**: if `prices * quantities` has already materialized, `.as_()` can only:

- Issue `RENAME TABLE t_<snowflake> TO t_revenue` — another DDL round-trip on top of the one being avoided, and it breaks any concurrent reader still holding the old name.
- Stay a Python-side alias — lineage graphs use the friendly name, but `system.tables` and any pasted SQL still show `t_<snowflake>`. The Python view and the CH reality diverge.

With lazy operators, `.as_()` *is* the first materialization, so the name lands on the initial CREATE. No rename, no alias-vs-real-name split.

**Why not per-operator `name=` kwargs**: `*` and `+` overloads don't accept kwargs in Python. Fluent variants (`mul(name=)`, `add(name=)`, `sum(name=)`, …) duplicate the operator surface and still miss chained expressions like `(a * b - c).as_("net")`. One postfix method composes over any expression.

**Work** (after Lazy Operator Results):
- `aaiclick/data/object/object.py` — `Object.as_(name, *, scope)` on the lazy result type, materializing under the requested name and scope.
- Decide spelling: `.as_()` (avoids the `as` keyword, terse) vs. `.named()` (longer, no trailing-underscore wart). Pick one and commit.
- Tests: every operator's result accepts `.as_()`; chained expressions name correctly; `scope="persistent"` produces a `p_<name>` table; double-naming (`.as_("a").as_("b")`) is either rejected or re-materializes under `b`.

---

# Deferred

Items deferred until preconditions are met.

## `Object.export()` HTML Format

`.html` extension → ClickHouse `HTML` output format. The format is supported
by upstream ClickHouse but the chdb build that aaiclick ships against rejects
it with `UNKNOWN_FORMAT` (chdb appears to omit the HTML output handler). Add
an `.html` / `HTML` entry to `FORMATS` in `aaiclick/data/formats.py` and the
corresponding test once chdb's build includes it, or once aaiclick gains a
way to fall back to clickhouse-connect for formats chdb doesn't ship.

## Nightly AI Live Tests

Bring back a nightly workflow that runs the live-LLM tests (`aaiclick/ai/test_provider_live.py`, `aaiclick/ai/agents/test_lineage_agent_live.py`) against a real model. The previous `project-ai-tests.yaml` spun up an `ollama/ollama` service and pulled `llama3.2:1b` on every run, which was slow and flaky. The non-live AI tests now run on every PR inside `test.yaml` (`AI local` group); the live tests auto-skip without `AAICLICK_AI_LIVE_TESTS=1`, so they cost nothing there.

**When to revisit**: once we either (a) have a stable, cached Ollama model image to avoid the per-run pull, or (b) move to a hosted provider with a CI-friendly budget. Gate the workflow to `schedule:` only — never on PRs.

**Work**:
- Recreate `.github/workflows/project-ai-tests.yaml` (or fold into a broader nightly workflow) running `pytest -m live_llm` against `aaiclick/ai/`.
- Re-add the `ai-tests` job to `run-all-projects.yaml` (or its successor).

## SSE Fanout Upgrades

The v0 SSE pipeline (`docs/frontend.md`) polls the database every 1–2 s on
the server side, diffs against the last snapshot, and emits the deltas as
SSE events. It works identically against the local (chdb + SQLite) and
distributed (ClickHouse + Postgres) backends with no extra infrastructure,
but it caps event latency at the polling interval and burns one query per
tick per active connection.

Two upgrade paths, both deferred until polling actually hurts:

- **Postgres LISTEN/NOTIFY** — workers `NOTIFY` on job/task state changes;
  the FastAPI server `LISTEN`s and forwards to SSE clients. Sub-second
  latency, no new service, but only available in distributed mode (SQLite
  has no equivalent, so the polling fallback has to stay for local mode).
- **Redis Pub/Sub** — needed only once we run multiple FastAPI workers
  across machines and need cross-process fanout that LISTEN/NOTIFY can't
  cheaply provide. New service to operate.

**When to revisit**: when polling latency is a visible UX problem, or
when CPU/query load from the polling loop becomes measurable, or when we
horizontally scale the API server beyond a single host.

## Comparison Page

`docs/comparison.md` — feature matrix comparing aaiclick vs Pandas, Spark, and Dask. Defer until the project has enough real-world usage to make meaningful claims.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
