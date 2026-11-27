Lazy Results
---

# Lazy Operator Results (`a + b` and `a.sum()` are Plans, not Tables)

<!-- req: lazy-operator-results -->
Every value-column operator returns a `LazyOperator` — an `Object` subclass that captures the plan (`lhs`, `rhs`, `operator`, precomputed result schema) without touching ClickHouse. `CREATE TABLE` + `INSERT INTO ... SELECT` run when it is awaited.

| Family            | Methods                                                                                                                     |
|-------------------|-----------------------------------------------------------------------------------------------------------------------------|
| Binary            | `+`, `-`, `*`, `/`, `//`, `%`, `**`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `&`, `\|`, `^`                                        |
| Aggregation       | `.min()`, `.max()`, `.sum()`, `.mean()`, `.std()`, `.var()`, `.count()`, `.count_if()`, `.quantile()`, `.unique()`, `.nunique()` |
| Unary transform   | `.year()`, `.month()`, `.day_of_week()`, `.lower()`, `.upper()`, `.length()`, `.trim()`, `.abs()`, `.log2()`, `.sqrt()`      |
| String / regex    | `.match()`, `.like()`, `.ilike()`, `.extract()`, `.replace()`                                                               |
| Membership / null | `.isin()`, `.is_null()`, `.is_not_null()`, `.coalesce()`                                                                    |
| Element-wise      | `.array_map()`                                                                                                              |

```python
# 1. Build a plan — pure Python, no DB call
plan = a + b                                       # LazyOperator (no table yet)
total = a.sum()                                    # also a LazyOperator

# 2. Read rows / scalars — async methods auto-materialize
rows = await (a + b).data()                        # creates t_<id>, reads rows
value = await a.sum().data()                       # one await — materialize + read

# 3. Get the materialized Object (for .table, legacy APIs, etc.)
obj = await (a + b)                                # table = t_<id>
scalar_obj = await a.sum()                         # table = t_<id> with one row

# 4. Control the table name — use .as_()
revenue = await (prices * quantities).as_("revenue")            # t_revenue_<id>
daily_total = await orders.sum().as_("daily_total")             # t_daily_total_<id>

# 5. Persist beyond the current context
daily = await (sales + bonuses).as_("daily", scope="job")       # j_<job_id>_daily
yearly = await orders.sum().as_("yearly", scope="global")       # p_yearly
```

<!-- req: await-only-for-object -->
`await` is only needed to get the materialized `Object` back — to read `.table`, hand it to an eager API, etc. Reading rows (`.data()`, `.markdown()`, `.export()`, `.result()`) or chaining operators (`(a + b) + c`, `obj.abs().sum()`) needs no `await` of the intermediate.

## `.as_(name, scope=...)`

Names the result table and chooses its lifetime. Returns a new `LazyOperator` (the receiver is unchanged):

<!-- req: as-name-scope -->
| `scope`                  | Table name             | Lifetime                                                               |
|--------------------------|------------------------|------------------------------------------------------------------------|
| `"temp_named"` (default) | `t_<name>_<snowflake>` | Drops with the context                                                 |
| `"job"`                  | `j_<job_id>_<name>`    | Lives until the active orch job expires                                |
| `"global"`               | `p_<name>`             | Persists; remove with `delete_persistent_object(name, scope="global")` |

## `name` / `scope` on always-materializing ops

<!-- req: materializing-ops-name-scope -->
`.copy()`, `.concat()`, `.join()`, and every `GroupByQuery` aggregation
(`.sum()`, `.mean()`, `.min()`, `.max()`, `.count()`, `.std()`, `.var()`,
`.any()`, `.group_array_distinct()`, `.agg()`) always create a new table, so
they take the same `name` / `scope` kwargs as `.as_()` directly:

```python
enriched = await a.join(b, on="user_id", name="enriched", scope="job")
# → j_<job_id>_enriched

totals = await orders.group_by("category").sum("amount", name="totals", scope="global")
# → p_totals
```

<!-- req: lazy-forwards-name-scope -->
On a `LazyOperator` they forward to the materialized delegate, so
`(a + b).copy(name=..., scope=...)` and `(a + b).join(b, name=..., scope=...)` work.

## Invariants

<!-- req: lazy-invariants -->
- **No DB writes until awaited.** Creating a `LazyOperator` is pure-Python. A lazy that's never awaited never creates a table.
- **Sync `.table` raises pre-materialize.** Reading `.table` (or any property derived from it — `.scope`, `.persistent`, `.order_by`) on an unawaited LazyOperator raises `RuntimeError`. `.schema` works (it's precomputed).
- **Async methods auto-materialize.** `.data()`, `.sum()`, etc. first `await self` (materializes once, cached) then delegate.
- **`.as_()` is immutable.** Calling it returns a new LazyOperator; the receiver keeps its (None, None) name/scope.
- **Re-await is idempotent (single-task).** `_materialized` cache ensures sequential awaits of the same lazy produce one table.

## Chained operators

<!-- req: chained-lazy-operators -->
`(a + b) + c` builds a tree:

```
LazyOperator(op="+", lhs=LazyOperator(op="+", lhs=a, rhs=b), rhs=c)
```

Each awaited node materializes into its own table — no fusion. `await ((a + b) + c).as_("outer")` writes **two** tables: an unnamed temp for the inner `a + b` and a `t_outer_<id>` for the outer. A LazyOperator that's been materialized once is reused if it appears in multiple expressions.

Aggregations and unary transforms stack the same way. `obj.abs().sum()` builds:

```
LazyOperator(op="sum", lhs=LazyOperator(op="abs", lhs=obj, rhs=None), rhs=None)
```

`await (a + b).sum().as_("total", scope="job")` writes one unnamed temp for the inner `+` and `j_<job_id>_total` for the outer `sum` — aggregations always materialize their input first.

## Avoid: concurrent await of the same instance

<!-- req: concurrent-await-race -->
`asyncio.gather(lazy.data(), lazy.data())` races: both tasks see `_materialized is None` and both materialize. The result is two tables (the second wins the cache slot; the first is orphaned and dropped via refcount cleanup). To share a result across tasks, `await` once and share the returned `Object`.
