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

    def __init__(
        self,
        schema_preview: Schema,
        materializer: Callable[[str | None, NamedScope | None], Awaitable[Object]],
        upstream: tuple[LazyOperator | Object, ...] = (),
    ): ...

    def as_(self, name: str, scope: NamedScope = "temp_named") -> LazyOperator:
        """Return a new LazyOperator that materializes with the given name and scope."""

    def __await__(self): ...  # triggers materialization, caches result

    @property
    def table(self) -> str:
        """Raises RuntimeError if not yet materialized."""
```

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
    schema_preview = _preview_operator_schema(self._schema, _peek_schema(other), operator)
    upstream = tuple(x for x in (self, other) if isinstance(x, LazyOperator))

    async def _mat(name: str | None, scope: NamedScope | None) -> Object:
        other_resolved = await Object._ensure_object(other)
        other_resolved.checkstale()
        _require_explicit_order_for_cross_table(self, other_resolved)
        info_a = self._get_query_info()
        info_b = other_resolved._get_query_info()
        return await operators._apply_operator_db(
            info_a, info_b, operator, self.ch_client, name=name, scope=scope,
        )

    return LazyOperator(schema_preview, _mat, upstream=upstream)
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
`_plan_operator_reverse`.

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
LazyOperator(op="+", upstream=(LazyOperator(op="+", upstream=(a, b)), c))
```

`await` on the outer node calls `_materialize()`, which:

1. Walks `upstream`, materializing each `LazyOperator` first as an
   unnamed temp (no fusion — each step is its own table). Eager Objects
   in `upstream` are no-ops.
2. Runs its own materializer with the captured `name` / `scope`.

So the outer node gets the user-supplied name; intermediates remain
unnamed temps. This matches the "materialize each step, no fusion"
decision.

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
9. **Chain `(a + b) + c`** — inner unnamed, outer named via `.as_("outer")`.
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
