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

## Phase 3: Defer the CREATE TABLE — Materialize Only on Demand

**Status:** Deeper change. Real wall-clock wins on cheap queries.

Every scalar aggregation today goes through `operators._apply_aggregation`
/ `nunique_agg` / `count_if_agg` / `quantile_agg`, which always do:

```sql
CREATE TABLE t_xxx (value Float64) ENGINE = Memory;  -- ~4 ms DDL
INSERT INTO t_xxx SELECT sum(value) AS value FROM source;
```

…then `.data()` reads `SELECT value FROM t_xxx`. Two round-trips, where
the first is a throwaway DDL whose result is rarely read as a table.

**Evidence** (1M rows, chdb 26, `aaiclick/example_projects/chdb_benchmark`):

| Operation | Native `SELECT` | aaiclick `CREATE + INSERT SELECT` | Empty `CREATE TABLE` alone |
|---------------|----------------:|----------------------------------:|---------------------------:|
| Count distinct | 3.89 ms | 9.01 ms | 4.18 ms |
| Group-by sum | 6.62 ms | 8.44 ms | — |

~60–70% of the aaiclick overhead on scalar aggregations is the DDL
round-trip — a fixed ~4 ms cost paid to register a throwaway sink table in
the catalog. The remaining ~30–40% is Python orchestration (Schema build,
Object register, async plumbing).

**Proposal — no new class, no new public API.** `LazyOperator` already
exists (`aaiclick/data/object/object.py:2831`) and already represents
every scalar aggregation with `rhs=None`. `.sum()`, `.mean()`, `.count()`,
`.nunique()`, `.count_if()`, `.quantile()` all return one today. The
change is **when** the CREATE fires, not the shape of the type system:

- **Today.** `.data()` triggers `_materialize()` → CREATE TABLE + INSERT
  → SELECT.
- **Tomorrow.** `.data()` runs the inner SELECT directly. `_materialize()`
  only runs when something genuinely needs a table — `.as_(name, scope=...)`,
  `.table` property access, downstream ops that use the scalar as a table
  source, orch cross-process handoff.

The SELECT that `.data()` runs is the *same* SQL today's code INSERTs
from — same `Schema`, same `operators` module SQL builders — just
executed bare instead of wrapped in `INSERT INTO … SELECT`. Downstream
`LazyOperator`s consuming an unmaterialized scalar paste the SELECT into
the subquery slot of `_get_query_info()`, which consumer sites already
accept.

**What doesn't change:** `Schema`, `ColumnInfo` (including
`low_cardinality`, `nullable`, `array`, `description`), column comments on
**persistent** / **job-scoped** tables, cross-process handoff via table
name, `open_object()` reconstruction. The CREATE TABLE stays as the
serialization path for tables that need to cross a process or session
boundary — it just stops being eager.

**Where a table is still required:**

- Persistent (`p_<name>`) / job-scoped (`j_<job_id>_<name>`) objects.
- Orch task outputs handed off to downstream workers.
- Repeated reads where the result should be cached.
- Joining a result as a table source (rare for scalars; broadcasting as a
  literal is usually better).

What actually triggers `_materialize()`:

- `await lazy_op` — bare-await contract returns a materialized `Object`,
  so the caller can read `.table`.
- `.table` property access — sync; raises today if unmaterialized
  (`object.py:2904-2916`), and continues to.
- A downstream op that demands a real table source (join side, orch
  cross-process handoff).

`.as_(name, scope=...)` is **pure metadata** — it clones the LazyOperator
and stamps `_name`/`_scope`, no DB call (`object.py:2881-2901`). The
stamped name/scope is consumed *if and when* one of the triggers above
fires. So `await sum_lazy.as_("foo", scope="job")` materializes to
`j_<job>_foo`; `sum_lazy.as_("foo").data()` runs the SQL-direct path and
never creates `foo` — the user asked for data, not a table.

**Interaction with `View` — no `LazyView` needed.** `View`
(`aaiclick/data/object/object.py:2248`) is already the projection-side
lazy operation: WHERE / LIMIT / OFFSET / ORDER BY / `selected_fields` /
`computed_columns` / `renamed_columns` / `exploded_columns` compose into
SQL without materializing, by overriding `_get_query_info()` to render a
constrained subquery against the source `.table`. `LazyOperator` covers
operators; `View` covers projections. Both feed each other:

- **`view.sum()` works without materializing the view.** `View` is an
  `Object` subclass, so `LazyOperator.lhs` / `.rhs` (typed as
  `Object | ValueScalarType[ | None]`) already accept it. The materialize
  path calls `lhs._get_query_info()`, which `View` overrides to inject
  the WHERE/LIMIT/etc. as a subquery. End result: `view.sum().data()` →
  one SELECT against the constrained view, no intermediate table.
- **`lazy_op.view(...)` materializes first.** `View.__init__` reads
  `source.table` and `source._schema`, so calling `.view(...)` on an
  unmaterialized `LazyOperator` must trigger `_materialize()`.
  **Decision: make `Object.view()` async.** The materialize-and-delegate
  pattern needs `await`, and rather than diverge with a sync `Object.view()`
  + async `LazyOperator.view()`, we promote `view()` on the base class.
  Small API break (call sites change from `obj.view(...)` to
  `await obj.view(...)`); aligns `view` with `.copy()`, `.concat()`,
  `.join()`, which are already async. `LazyOperator.view()` then auto-
  materializes via the same `materialize-and-delegate` pattern as
  `.copy()` (`object.py:3009-3036`).

The result: no `LazyView` class, no duplication. Filter/project chains
stay in `View`, operator chains stay in `LazyOperator`, and they compose
through `_get_query_info()` without either side needing to know about
the other's internals.

**Work:**

- `aaiclick/data/object/operators.py` — split `_apply_aggregation`,
  `nunique_agg`, `count_if_agg`, `quantile_agg`, `unique_group` into a
  SQL-builder half and an optional materializer half. The SQL-builder is
  what `.data()` calls; the materializer is what `_materialize()` calls.
  Mechanical extraction — same SQL on both sides.
- `aaiclick/data/object/object.py` — `LazyOperator.data()` /
  `.markdown()` / `.export()` take the SQL-builder path when no
  `name`/`scope` was set. `_get_query_info()` on an unmaterialized
  `LazyOperator` returns the SQL wrapped as a subquery, so downstream
  binary ops paste it inline.
- `aaiclick/data/object/object.py` — promote `Object.view()` to `async`
  and update all call sites. Add a brief comment on `Object.view()`
  explaining why it is async (`LazyOperator.view()` needs to
  materialize; keeping `view` sync on `Object` but async on
  `LazyOperator` would be a confusing split).
- Benchmark: `chdb_benchmark` should show `Count distinct` /
  `Group-by sum` dropping from ~10 ms → ~5 ms at 1M rows.
- Tests: every operator test that currently asserts against a
  materialized table still passes — `.as_()` and `.table` access still
  produce a table; only the implicit `.data()`-only case becomes faster.

Pairs with the "scalar Object unwrapping" idea — once `.data()` is cheap,
the ergonomic case of "just give me the number" becomes the fast default.
The Phase 2a work above is the precondition: every operator already has a
single sync planner whose materializer can be swapped without touching
call sites.
