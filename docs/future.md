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

## Replace `datetime.utcnow()` and Add Python 3.13 to CI Matrix

`datetime.utcnow()` is deprecated in Python 3.12+. The codebase has ~59 call sites (mostly `aaiclick/orchestration/`: `models.py`, `factories.py`, `registered_jobs.py`, `background/`, plus a few tests). Local development on Python 3.13 with `filterwarnings = ["error"]` turns the deprecation into test failures; CI doesn't see this because every `uv sync` invocation in `.github/workflows/test.yaml` pins `--python 3.10`.

A surgical fix landed for `aaiclick/orchestration/orch_context.py` (the only call site touched by `aaiclick/oplog/test_graph.py`). The rest of the sweep is deferred.

**Work**:
- Replace every `datetime.utcnow()` with `datetime.now(UTC)` (add `UTC` to existing `from datetime import …` imports).
- For SQLModel/Pydantic `default_factory=datetime.utcnow` fields, switch to `default_factory=lambda: datetime.now(UTC)` and verify the column types still round-trip correctly — `datetime.now(UTC)` returns a timezone-aware datetime, whereas `utcnow()` returned naive. The DB columns + serializers may need to be made tz-aware (or strip tzinfo at the boundary if we want to preserve naive storage).
- Add a Python 3.13 leg to the test matrix in `.github/workflows/test.yaml`: change the `--python 3.10` pins to a matrix `python-version: ["3.10", "3.13"]` so future deprecations are caught at the CI boundary instead of by individual developers.

## Make `close_session()` Opt-In Instead of Unconditional on `orch_context` Exit

`orch_context.py` unconditionally calls `close_session(get_chdb_data_path())` in its `finally` block when running on chdb. The reason is real — a subprocess worker about to be spawned needs the chdb file lock — but in-process-only callers pay the cost for nothing, and chdb's `Session.cleanup()` + re-init is not safe to repeat within one process (see `docs/technical_debt.md`, [chdb-io/chdb#229](https://github.com/chdb-io/chdb/issues/229)). Tests work around it today via the `_pin_chdb_session` fixture; production code shouldn't need a test fixture to stay stable.

**Proposal**: gate the close behind an explicit signal that the caller is about to hand the chdb file off to another process. Shapes to consider:
- A `release_chdb_session: bool = False` kwarg on `orch_context()` that the worker-spawning path sets to `True`.
- A context manager `with releasing_chdb_session():` around the code that spawns workers.
- An env var (`AAICLICK_CHDB_RELEASE_ON_EXIT=1`) for test-runner-style callers.

**Work**: `aaiclick/orchestration/orch_context.py` — remove the unconditional `close_session()` from the `finally` branch; add the opt-in signal above; update any worker-spawning path (`aaiclick/orchestration/execution/mp_worker.py`, etc.) to invoke it. Remove the `_pin_chdb_session` fixture once this lands.

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

## Switch `StrEnum` Usages to `Literal`

**Codebase-wide rule** (also in `CLAUDE.md` → Coding Guidelines): `typing.Literal` is preferred over `StrEnum` / `(str, Enum)` for closed sets of string values. Reach for a real `Enum` class only when something forces it.

Every status/mode in `aaiclick/orchestration/models.py` — `JobStatus`, `TaskStatus`, `WorkerStatus`, `RunType`, `PreservationMode` — is a `StrEnum` purely because SQLModel needs a real `Enum` class to map a type hint to a column. Pure-view models already use `Literal` (`ObjectScope`, `NamedScope`, `SetupStepStatus`). `aaiclick/view_models.py` also still carries `OllamaBootstrapStatus` and `MigrationAction` as `(str, Enum)` subclasses; these do not need DB mapping and can flip to `Literal` directly.

**Proposal**: make Literal the single source of truth; declare the DB mapping explicitly via `sa_column` with `SaEnum(*get_args(MyLiteral))`.

```python
from typing import Literal, get_args
from sqlalchemy import Column
from sqlalchemy import Enum as SaEnum

JobStatus = Literal["PENDING", "RUNNING", "COMPLETED", "FAILED", "CANCELLED"]

class Job(SQLModel, table=True):
    status: JobStatus = Field(
        sa_column=Column(SaEnum(*get_args(JobStatus), name="job_status"), nullable=False),
    )
```

**Tradeoffs to resolve first**:

- Alembic autogenerate is less reliable with Literal + explicit `sa_column` than with a real `Enum` class — value-set changes may need hand-written migrations.
- Postgres native `ENUM` needs `ALTER TYPE ADD VALUE` (non-transactional) to add values. Consider switching those columns to `Column(String, CheckConstraint(...))` for easier migrations — at the cost of losing the native ENUM type on the DB side.
- Every `JobStatus.PENDING` reference across the codebase, tests, and examples flips to `"PENDING"`. Bulk rename with care; pydantic / type-check will catch most mistakes.

**Work**:

- `aaiclick/orchestration/models.py` — replace the five StrEnums with Literal aliases and add `sa_column=Column(SaEnum(...))` to each Field.
- `aaiclick/view_models.py` — flip `OllamaBootstrapStatus` and `MigrationAction` to `Literal` aliases (no `sa_column` needed; these are pure-view models).
- Update every `Status.VALUE` reference in `aaiclick/`, tests, and examples to string literals.
- Audit alembic migrations for new diffs; hand-write migrations for any that autogenerate misses.
- `CLAUDE.md` Literal-first rule is already in place; revisit once migration lands to remove the "scheduled for migration" callout.

## Collapse Dataclass ↔ Pydantic View-Model Duplication

Several pure data containers are defined twice — once as a `@dataclass` for in-process use and once as a Pydantic `BaseModel` for the API/MCP/REST surface — with hand-written adapters to convert between the two. Pydantic v2 handles methods, properties, classmethods, and `Field(default_factory=...)` natively, so the dataclass form earns its keep only when something forces it (frozen + slotted hot path, `dataclasses.asdict` consumers, etc.). For these cases, nothing forces it.

**Confirmed duplications** (all keyword-constructed, no `dataclasses.asdict` / `replace` / `fields()` consumers in production):

- `ColumnInfo` (`aaiclick/data/models.py:55`) ↔ `ColumnView` (`aaiclick/data/view_models.py:28`), bridged by `column_info_to_view`. Note `ColumnInfo` is `frozen=True` and has a `with_fieldtype()` helper plus a `ch_type()` formatter — both translate to Pydantic with `model_config = ConfigDict(frozen=True)` + `model_copy(update=...)`.
- `Schema` / `ViewSchema` (`aaiclick/data/models.py:277`, `:311`) ↔ `SchemaView` (`aaiclick/data/view_models.py:40`), bridged by `schema_to_view` / `view_to_schema`.

**Intentionally NOT in scope**:

- `aaiclick/orchestration/models.py` SQLModel tables (`Job`, `Task`, `Worker`, `RegisteredJob`) ↔ `orchestration/view_models.py` views — that split is the deliberate persistence-vs-API boundary, not duplication.
- `aaiclick/data/models.py` `QueryInfo` / `IngestQueryInfo` / `CopyInfo` / `GroupByInfo` — internal SQL-builder DTOs that never cross the API boundary; no Pydantic mirror exists.

**Work**:

- Replace `@dataclass` with `BaseModel` on `ColumnInfo`, `Schema`, `ViewSchema`. Convert `field(default_factory=...)` → `Field(default_factory=...)`; methods stay as-is; for `ColumnInfo` keep the frozen semantics via `model_config = ConfigDict(frozen=True)` and replace `dataclasses.replace` call sites with `model_copy(update=...)`.
- Delete `ColumnView`, `SchemaView`, `column_info_to_view`, `schema_to_view`, `view_to_schema` from `aaiclick/data/view_models.py`; expose `ColumnInfo` / `Schema` directly to the API surface and update `ObjectDetail.table_schema: Schema`.
- Sweep all `Schema(...)`, `ColumnInfo(...)`, and `replace(info, ...)` call sites; verify keyword-only construction holds.
- Already done in scope `claude/test-lineage-mcp-dmOVz` for `OplogNode` / `OplogEdge` / `OplogGraph` (`aaiclick/oplog/lineage.py`) — no mirrors needed; the dataclasses became Pydantic models in place.

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

## Nightly AI Live Tests

Bring back a nightly workflow that runs the live-LLM tests (`aaiclick/ai/test_provider_live.py`, `aaiclick/ai/agents/test_lineage_agent_live.py`) against a real model. The previous `project-ai-tests.yaml` spun up an `ollama/ollama` service and pulled `llama3.2:1b` on every run, which was slow and flaky. The non-live AI tests now run on every PR inside `test.yaml` (`AI local` group); the live tests auto-skip without `AAICLICK_AI_LIVE_TESTS=1`, so they cost nothing there.

**When to revisit**: once we either (a) have a stable, cached Ollama model image to avoid the per-run pull, or (b) move to a hosted provider with a CI-friendly budget. Gate the workflow to `schedule:` only — never on PRs.

**Work**:
- Recreate `.github/workflows/project-ai-tests.yaml` (or fold into a broader nightly workflow) running `pytest -m live_llm` against `aaiclick/ai/`.
- Re-add the `ai-tests` job to `run-all-projects.yaml` (or its successor).

## Comparison Page

`docs/comparison.md` — feature matrix comparing aaiclick vs Pandas, Spark, and Dask. Defer until the project has enough real-world usage to make meaningful claims.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
