# Future Work — LazyOperator

Planned extensions to the `LazyOperator` design shipped in `docs/object.md`
("Lazy Operator Results").

---

## Phase 2a: Aggregations + Unary Transforms — ✅ SHIPPED

`.as_(name, scope=...)` now works on:

- **Aggregations**: `.min()`, `.max()`, `.sum()`, `.mean()`, `.std()`,
  `.var()`, `.count()`, `.count_if()`, `.quantile()`, `.unique()`,
  `.nunique()`.
- **Unary transforms**: `.year()`, `.month()`, `.day_of_week()`, `.lower()`,
  `.upper()`, `.length()`, `.trim()`, `.abs()`, `.log2()`, `.sqrt()`.

Each method is now a sync planner returning `LazyOperator(lhs=self,
rhs=None, operator=<name>)`. Parametrized operators (`count_if`,
`quantile`) carry their extra args on `LazyOperator.params`. The
materialize path lives in `LazyOperator._materialize_unary`, which
dispatches by operator name to the matching function in
`aaiclick/data/object/operators.py` and forwards `name`/`scope` to
`create_object`. Preview helpers (`_preview_agg_schema`,
`_preview_unary_schema`, `_preview_count_if_schema`,
`_preview_quantile_schema`, `_preview_unique_schema`,
`_preview_nunique_schema`) live in `aaiclick/data/object/schema_compute.py`
so the preview and materialize paths share one source of truth.

The fluent `(a + b).sum()` pattern now stacks two `LazyOperator` nodes
instead of materializing eagerly between them. `await (a + b).sum().as_("total", scope="job")`
writes one unnamed temp for the inner `+` and `j_<job_id>_total` for the
outer `sum` — both nodes pick up their `name`/`scope` from the call chain.

---

## Phase 2b: `name`/`scope` for Joins, Concat, Copy, Group-By — ✅ SHIPPED

`.copy()`, `.concat()`, `.join()`, and every `GroupByQuery` method
(`agg`, `sum`, `mean`, `min`, `max`, `count`, `std`, `var`, `any`,
`group_array_distinct`) now accept `name`/`scope` kwargs that forward to
`create_object()` — same rules as Phase 2a's `.as_()`.

These ops always materialize a new table, so there's no fluency or perf
win from routing them through `LazyOperator` (that path is reserved for
chains where materialization can be elided). The kwargs land directly on
the existing async methods, and LazyOperator's overrides forward them to
the materialized delegate so `(a + b).copy(name=..., scope=...)` and
`(a + b).join(b, name=..., scope=...)` work.

**Implementation**: `Object.copy/concat/join` and `GroupByQuery.*` in
`aaiclick/data/object/object.py`; underlying materializers in
`ingest.py` (`copy_db`, `copy_db_selected_fields`, `concat_objects_db`),
`join.py` (`join_objects_db`), `operators.py` (`group_by_agg`).

---

## Phase 3: Elide Materialization Entirely for Small / Scalar Results

**Status:** Deeper change. Real wall-clock wins on cheap queries.

Every operator today — including phase 1's `LazyOperator` — materializes its
result into a fresh ClickHouse table via `create_object(schema)` +
`INSERT INTO ... SELECT`. For scalar and small-result aggregations
(`sum`, `nunique`, `count`, `min`, `max`, `mean`, single-key
`group_by.sum`), the extra `CREATE TABLE ... ENGINE = Memory` round-trip
dominates wall clock on cheap queries.

**Evidence** (1M rows, chdb 26, `aaiclick/example_projects/chdb_benchmark`):

| Operation | Native `SELECT` | aaiclick `CREATE + INSERT SELECT` | Empty `CREATE TABLE` alone |
|---------------|----------------:|----------------------------------:|---------------------------:|
| Count distinct | 3.89 ms | 9.01 ms | 4.18 ms |
| Group-by sum | 6.62 ms | 8.44 ms | — |

~60–70% of the aaiclick overhead on scalar aggregations is the DDL
round-trip — a fixed ~4 ms cost paid to register a throwaway sink table in
the catalog. The remaining ~30–40% is Python orchestration (Schema build,
Object register, async plumbing).

**Root cause:** `operators.nunique_agg` / `operators.group_by_agg` /
`_apply_aggregation` build a `Schema` in Python, then call
`create_object(schema)` which emits
`CREATE TABLE <result> (...) ENGINE = Memory` with column comments — just
to hold a 1-row or 10-row result that the caller almost always unwraps via
`.data()`. The schema is fully known in Python before the DDL is sent; the
CREATE just *serializes* metadata the runtime already has.

**Proposal:** Scalar and small-result operators return a `LazyScalar` /
`LazyView` wrapper carrying the same `Schema` (types, fieldtype,
nullability, LowCardinality, descriptions) plus the query SQL.
Materialization into a real table happens only when genuinely needed —
e.g. `.materialize()`, cross-process handoff, or downstream ops that
require a table source.

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

**What doesn't change:** `Schema`, `ColumnInfo` (including
`low_cardinality`, `nullable`, `array`, `description`), column comments on
**persistent** / **job-scoped** tables, cross-process handoff via table
name, `open_object()` reconstruction. Metadata remains Python-side first;
the CREATE TABLE stays as the serialization path for tables that need to
cross a process or session boundary.

**Where a table is still required:**

- Persistent (`p_<name>`) / job-scoped (`j_<job_id>_<name>`) objects.
- Orch task outputs handed off to downstream workers.
- Repeated reads where the result should be cached.
- Joining a result as a table source (rare for scalars; broadcasting as a
  literal is usually better).

Add `.materialize()` as the explicit escape hatch so callers can opt in.

**Work:**

- `aaiclick/data/object/operators.py` — new `LazyScalar` / `LazyView`
  classes or extend existing `View`; route `nunique_agg`,
  `_apply_aggregation` (sum/mean/min/max/count/std/var), `group_by_agg`
  for small results through them.
- `aaiclick/data/object/object.py` — `.data()` on a lazy result executes
  the SQL directly; chain operators inline the lazy SQL as a subquery
  instead of reading from a table name.
- Decide group-by threshold: always lazy vs. materialize above N result
  rows — likely always lazy, let downstream `.copy()` or `.materialize()`
  decide.
- Benchmark: `chdb_benchmark` should show `Count distinct` /
  `Group-by sum` dropping from ~10 ms → ~5 ms at 1M rows.
- Tests: every operator test that currently asserts against a
  materialized table still passes (via implicit materialize-on-data or
  an explicit `.materialize()` in tests that introspect `.table`).

Pairs with the "scalar Object unwrapping" idea — once `.data()` is cheap,
the ergonomic case of "just give me the number" becomes the fast default.
The phase 2 work above is the precondition: it gives every operator a
single sync planner whose materializer can be swapped for a SQL-string
recorder without touching call sites.
