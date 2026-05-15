# Lazy Operator — Named Operator Results

⚠️ NOT YET IMPLEMENTED — phase 1 covers the 16 binary dunders only.

## Problem

Operator results (`a + b`, `a * b`, etc.) materialize into ClickHouse tables
with auto-generated names (`t_<snowflake>`). There is no way to give the
result a meaningful name or persist it beyond the current context — even
though `create_object()` already supports `name=` and `scope=` for direct
construction.

## Design

Operator methods return a `LazyOperator` — a subclass of `Object` that
captures the operation plan and materializes on `await`. A new `.as_(name,
scope="temp_named")` method lets the caller name the result and choose its
lifetime before materialization.

```python
# Before — unnamed temp, no control
result = await (a + b)                          # table = t_<id>

# After — same call still works
result = await (a + b)                          # table = t_<id>

# New — control the table name
result = await (a + b).as_("daily_total")       # table = t_daily_total_<id>

# New — persist beyond the context
result = await (a + b).as_("daily_total", scope="job")    # table = j_<job_id>_daily_total
result = await (a + b).as_("yearly_avg", scope="global")  # table = p_yearly_avg
```

### Scope

In scope (phase 1): the 16 binary dunders on `Object` — arithmetic
(`+ - * / // % **`), comparison (`== != < <= > >=`), and bitwise
(`& | ^`), plus the matching reverse dunders (`__radd__` etc.).

Explicitly out of scope:

- **Views and constraints** (`.view()`, `.where()`, field selection,
  rename, exploded columns) — these don't produce new tables. They wrap
  an existing table with constraints applied at query time, so naming
  doesn't apply.
- **Aggregations** (`.sum()`, `.mean()`, etc.) — follow-up phase.
- **Unary transforms** (`.abs()`, `.lower()`, etc.) — follow-up phase.
- **Joins, concat, copy, ingest, group_by, insert_from_url** —
  follow-up phases.

### `LazyOperator` class

```python
class LazyOperator(Object):
    """A planned operation that materializes into an Object on ``await``."""

    # The planned operation, structured for both binary and future unary ops.
    lhs: Object | LazyOperator                  # always present
    rhs: Object | ValueScalarType | None        # None for unary / self-ops (e.g. .mean())
    operator: str                                # e.g. "+", "-", "mean"

    def __init__(
        self,
        lhs: Object | LazyOperator,
        rhs: Object | ValueScalarType | None,
        operator: str,
        schema_preview: Schema,
        build_select: Callable[[QueryInfo, QueryInfo | None], str],
    ): ...

    def as_(self, name: str, scope: NamedScope = "temp_named") -> LazyOperator:
        """Return a new LazyOperator that materializes with the given name and scope."""

    def _get_query_info(self) -> QueryInfo:
        """Returns a QueryInfo whose ``source`` is the fused SELECT subquery
        for this op, so chained operators fuse into one SQL at the root."""

    def __await__(self): ...  # triggers materialization at the root, caches result

    @property
    def table(self) -> str:
        """Raises RuntimeError if not yet materialized."""
```

**Field shape:** the `lhs / rhs / operator` triple is the structural
invariant. `rhs=None` is reserved for unary and aggregation operators in
later phases (e.g. `.mean()` → `LazyOperator(lhs=src, rhs=None,
operator="mean")`), so the same data shape covers everything without a
phase 2 schema migration. Phase 1 always has `rhs` populated.

Key invariants:

- **No DB writes until awaited.** Creating a `LazyOperator` is pure-Python;
  no ClickHouse round trip. If the LazyOperator is never awaited, no table
  is ever created.
- **Sync `.table` raises pre-materialize.** A clear `RuntimeError`
  instructs the caller to `await` first. Sync properties can't trigger
  async materialization safely, so they raise rather than block.
- **Async methods auto-materialize.** `.data()`, `.markdown()`,
  `.export()`, `.result()` first `await self` (which materializes once,
  cached) and then delegate to the resulting `Object`.
- **`.as_()` returns a new LazyOperator.** Immutable — calling `.as_()`
  does not mutate the receiver.
- **Re-await is idempotent.** Internal `_materialized` cache ensures one
  awaitable produces exactly one table.
- **Schema is precomputed sync.** `.schema` works before materialization
  (the result columns, fieldtype, and value type are derivable from
  operand schemas + operator without a DB call). Other table-derived
  properties — `.scope`, `.persistent`, `.order_by` — raise the same
  `RuntimeError` as `.table`, since they read `self.table` under the hood.

### Call-site refactor

Each binary dunder changes from `async def → ... Object` to
`def → ... LazyOperator`. The work currently in the method body moves into
a materializer closure captured by the LazyOperator.

Today:

```python
async def __add__(self, other) -> Object:
    return await self._apply_operator(other, "+")

async def _apply_operator(self, other, operator) -> Object:
    self.checkstale()
    other = await self._ensure_object(other)
    other.checkstale()
    _require_explicit_order_for_cross_table(self, other)
    info_a = self._get_query_info()
    info_b = other._get_query_info()
    return await operators._apply_operator_db(info_a, info_b, operator, self.ch_client)
```

After:

```python
def __add__(self, other) -> LazyOperator:
    return self._plan_operator(other, "+")

def _plan_operator(self, other, operator) -> LazyOperator:
    self.checkstale()
    schema_preview = _preview_operator_schema(
        self._schema, _peek_schema(other), operator
    )
    return LazyOperator(
        lhs=self, rhs=other, operator=operator,
        schema_preview=schema_preview,
        build_select=operators._build_operator_select,
    )
```

`_apply_operator_db` is split into a sync builder + an async wrapper:

```python
def _build_operator_select(
    info_a: QueryInfo, info_b: QueryInfo, operator: str
) -> tuple[str, Schema]:
    """Pure-SQL string for the operator's SELECT, plus result Schema.
    No DB calls. Mirrors today's same-table / cross-table / scalar
    branching, minus the INSERT wrap and result-table creation."""

async def _apply_operator_db(
    info_a, info_b, operator, ch_client,
    *, name: str | None = None, scope: NamedScope | None = None,
) -> Object:
    select_sql, schema = _build_operator_select(info_a, info_b, operator)
    await _validate_if_needed(info_a, info_b, ch_client)
    result = await create_object(schema, name=name, scope=scope)
    await ch_client.command(f"INSERT INTO {result.table} {select_sql}")
    return result
```

At await-time, the root LazyOperator runs this wrapper with its captured
`name` / `scope`. Chained inner LazyOperators are visited via
`_get_query_info()`, which returns a `QueryInfo` whose `source` is
`(SELECT …)` from `_build_operator_select` — so fusion happens
naturally inside the existing `info_a.source` plumbing.

The reverse dunders (`__radd__` etc.) follow the same pattern via
`_plan_operator_reverse`, which swaps `lhs` / `rhs` before building.

### Schema preview helper

`_preview_operator_schema(schema_a, schema_b, operator) -> Schema` is
pure-Python and reproduces the type-promotion, fieldtype-promotion, and
nullable-propagation logic at the top of `_apply_operator_db`
(operators.py:278-306). It does **not** touch ClickHouse. A test asserts
preview output equals the post-materialize schema for every operator ×
fieldtype combination — so the two paths cannot drift.

`_peek_schema(value)` returns a `Schema` for either an Object /
LazyOperator (just `.schema`) or a Python scalar (a one-row scalar schema
with the scalar's inferred type), reused by both `_plan_operator` and
`_peek` callers.

### Chaining and fused materialization

`(a + b) + c` builds a tree:

```
LazyOperator(op="+", lhs=LazyOperator(op="+", lhs=a, rhs=b), rhs=c)
```

`await` on the outer node fuses the chain into **one** SQL — no
intermediate temp tables. Mechanism:

1. `_apply_operator_db` is factored into a sync `_build_operator_select`
   (returns the inner SELECT as a string + a result `Schema`) and an
   async `_apply_operator_db` that wraps it as `INSERT INTO {named_table}
   SELECT ...` against the named/scoped result.
2. `LazyOperator._get_query_info()` calls `_build_operator_select` to
   produce a `QueryInfo` whose `source` is `(SELECT … FROM lhs JOIN rhs
   …)` — exactly the shape `_apply_operator_db` already handles for view
   sources today (via the `either_is_view` branch).
3. When `(a + b)` is the lhs of `+ c`, the outer's `info_a` is the
   subquery from the inner. `_apply_operator_db` runs once at the root,
   producing one table with the outer's name/scope.

Each chain node is visited once; the inner `+` never writes its own
table. Validation calls (`_validate_array_lengths`) move to materialize
time and run for each node before the final INSERT.

**Known fusion limitations:**

- **`_materialize_array_join` still uses an internal temp.** When both
  operands are array×array cross-table *and* at least one operand is a
  view-source subquery, the existing path materializes an ARRAY JOIN
  staging table because unwrapping nested arrays twice is too expensive
  to inline. Chains that hit this path will still create one staging
  temp per node that hits it; everything else fuses cleanly.
- **Same-table optimization is lost when one operand is a fused chain.**
  The `info_a.base_table == info_b.base_table` short-circuit in
  `_apply_operator_db` (operators.py:314-319) only fires when both sides
  resolve to a real table. A fused subquery has no `base_table`, so the
  cross-table JOIN path runs instead. Correct, just less optimal — only
  matters when the user chains ops on columns from the same source.

The user retains full control: an explicit `await` of an intermediate
forces materialization, breaking fusion at that point.

```python
inner = await (a + b)                   # forces materialization here
result = await (inner + c).as_("foo")   # no fusion across `inner`
```

## Backward compatibility

| Call | Today | After | Status |
|------|-------|-------|--------|
| `await (a + b)` | runs coroutine, returns Object | runs `__await__`, returns Object | ✅ unchanged |
| `r = a + b; await r` | awaits coroutine | awaits LazyOperator | ✅ unchanged |
| `(a + b) + c` | second `+` on coroutine — error today | inner is LazyOperator (Object subclass); `+` chains | ✅ improvement |
| `await (a + b).data()` | n/a — couldn't chain methods | auto-materializes, returns rows | ✅ new |
| `(a + b).table` (without await) | n/a | raises `RuntimeError` | ✅ explicit, safe |
| `del (a + b)` without await | coroutine warning | no DB call, no warning | ✅ improvement |

The behavior change is that `a + b` returns a `LazyOperator` synchronously
rather than a coroutine. Code that stored the result without awaiting it
will see a `LazyOperator` instead of a `coroutine` — both are awaitable,
so `await r` still works. Callers introspecting `inspect.iscoroutine(r)`
would break, but there are no such callers in the codebase.

## Testing

New file: `aaiclick/data/object/test_lazy_operator.py`.

1. **Default unnamed temp** — `await (a + b)` produces `t_<id>` (regression).
2. **`.as_(name)` (default scope)** — `await (a + b).as_("foo")` produces `t_foo_<id>`.
3. **`.as_(name, scope="job")`** — produces `j_<job_id>_foo`, survives context exit while job active.
4. **`.as_(name, scope="global")`** — produces `p_foo`, persists; cleanup via `delete_persistent_object`.
5. **Schema preview correctness** — parametrized over every binary op × array/scalar fieldtype; asserts pre-materialize `.schema` == post-materialize `.schema`.
6. **`.table` raises before await** — `pytest.raises(RuntimeError, lambda: (a + b).table)`.
7. **`.data()` auto-materializes** — `await (a + b).data()` returns rows; only one table created.
8. **No DB writes when never awaited** — assert `table_registry` count unchanged after constructing and dropping a lazy.
9. **Chain `(a + b) + c` fuses** — `await ((a + b) + c).as_("outer")` creates **one** table (the outer); inspect `table_registry` to confirm no intermediate temp was written. Output rows match the eager equivalent. Plus a counterpart test that breaking fusion via an explicit inner `await` does create two tables.
10. **Re-await idempotent** — `await lazy; await lazy` returns same `Object`, one table.
11. **Reverse op with naming** — `await (2 + a).as_("rfoo")`.
12. **`.as_()` is non-mutating** — `lazy.as_("x")` does not change `lazy`; `await lazy` still produces an unnamed temp.

## Future phases

Once phase 1 lands, the same pattern extends to:

- Aggregations (`.sum()`, `.mean()`, `.min()`, `.max()`, `.std()`, `.var()`,
  `.count()`, `.count_if()`, `.quantile()`, `.unique()`, `.nunique()`).
- Unary transforms (`.year()`, `.month()`, `.day_of_week()`, `.lower()`,
  `.upper()`, `.length()`, `.trim()`, `.abs()`, `.log2()`, …).
- `.copy()`, `.concat()`, `.join()`, `.group_by()`.
- Ingest-side creators (`insert_from_url`, etc.).

Each follow-up phase is mechanical: convert the entry method from
`async def` to a sync planner returning `LazyOperator`, and add
`name`/`scope` kwargs to the underlying materializer's single
`create_object()` call. Tracked in `docs/future.md`.
