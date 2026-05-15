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
# 1. Build a plan — pure Python, no DB call
plan = a + b                                    # LazyOperator (no table yet)

# 2. Read rows — async methods auto-materialize
rows = await (a + b).data()                     # creates t_<id>, reads rows

# 3. Get the materialized Object (for .table, legacy APIs, etc.)
obj = await (a + b)                             # table = t_<id>

# 4. Control the table name
obj = await (a + b).as_("daily_total")          # table = t_daily_total_<id>

# 5. Persist beyond the context
obj = await (a + b).as_("daily_total", scope="job")    # table = j_<job_id>_daily_total
obj = await (a + b).as_("yearly_avg", scope="global")  # table = p_yearly_avg
```

`await` on a LazyOperator triggers `__await__`, which materializes the
plan into a ClickHouse table and returns an `Object`. It's only needed
when the caller wants the materialized `Object` itself — e.g. to read
`.table`, store it for later use, or hand it to an eager API. Reading
rows (`.data()`) or chaining further operators (`(a + b) + c`) does not
require an explicit `await` of the intermediate; the lazy passes through
to the next step.

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
    ): ...

    def as_(self, name: str, scope: NamedScope = "temp_named") -> LazyOperator:
        """Return a new LazyOperator that materializes with the given name and scope."""

    def __await__(self): ...  # walks tree, materializes each node, caches result

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
    )
```

`_apply_operator_db` gains two kwargs forwarded to its single
`create_object()` call:

```python
async def _apply_operator_db(
    info_a, info_b, operator, ch_client,
    *, name: str | None = None, scope: NamedScope | None = None,
) -> Object:
    ...
    result = await create_object(schema, name=name, scope=scope)
    ...
```

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

### Chaining and materialization order

`(a + b) + c` builds a tree:

```
LazyOperator(op="+", lhs=LazyOperator(op="+", lhs=a, rhs=b), rhs=c)
```

`await` on the outer node walks the tree once and materializes each
`LazyOperator` node into its own ClickHouse table — **no fusion**.
Sequence for `await ((a + b) + c).data()`:

1. Outer's `_materialize()` recursively materializes its `lhs` first.
   Inner is a `LazyOperator` → runs the inner's materializer, producing
   an unnamed temp (`t_<id>`) holding `a + b`. The inner caches this
   `Object` on its `_materialized` slot.
2. Outer then runs its own materializer with the inner's materialized
   table as the left operand and `c` as the right. Result is a second
   table — named via `.as_(...)` if set, otherwise another unnamed
   temp.
3. `.data()` reads rows from the outer's table.

Two tables created total. The inner stays unnamed (its `.as_()` was
never called); the outer is named only if `.as_()` was applied to the
outermost LazyOperator. Eager `Object` operands in `lhs` / `rhs` are
no-ops in the walk.

Materialization is also idempotent: a `LazyOperator` already in
`_materialized` is reused — so if the same inner appears in multiple
expressions it materializes once.

```python
inner = (a + b)                          # LazyOperator, no DB call
result = await (inner + c).as_("foo")    # 2 tables: t_<id> (inner), t_foo_<id> (outer)
also   = await (inner * 3)               # 1 more table for the *3 result;
                                          # inner already materialized, reused
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
9. **Chain `(a + b) + c`** — `await ((a + b) + c).as_("outer")` creates **two** tables: an unnamed temp for the inner `a + b`, and a `t_outer_<id>` for the outer. Output rows match the eager equivalent.
9b. **Shared inner materializes once** — `inner = a + b; await (inner + c); await (inner * 3)` — assert exactly three tables created total (one for inner, one each for the two outers), not four.
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
