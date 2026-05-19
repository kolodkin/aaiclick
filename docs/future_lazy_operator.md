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
