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

These trigger `_materialize()` via `.as_(name, scope=...)`, `.table`, or
the existing table-source code path. No new escape hatch needed —
`.as_(name, scope=...)` already exists from Phase 2a.

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
