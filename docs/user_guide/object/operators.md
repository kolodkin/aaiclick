Operators
---

# Operator Support

<!-- req: scalar-operands -->
All operators work element-wise on scalar and array data, creating new Object
tables. Python scalars work on either side (`obj * 2`, `2 * obj`) and are
inlined into the SQL as typed literals, never given a table of their own. Full
script: [Examples: Basic Operators](../../examples/basic_operators.md).

## Arithmetic Type Promotion

<!-- req: arithmetic-type-promotion -->
Result types match ClickHouse's native promotion rules, verified against `SELECT toTypeName()`.

<!-- req: arithmetic-operators -->
??? note "Arithmetic Operators"

    | Python Operator | Description    | ClickHouse Equivalent | Forward Method  | Reverse Method    |
    |-----------------|----------------|-----------------------|-----------------|-------------------|
    | `+`             | Addition       | `+`                   | `__add__`       | `__radd__`        |
    | `-`             | Subtraction    | `-`                   | `__sub__`       | `__rsub__`        |
    | `*`             | Multiplication | `*`                   | `__mul__`       | `__rmul__`        |
    | `/`             | Division       | `/`                   | `__truediv__`   | `__rtruediv__`    |
    | `//`            | Floor Division | `intDiv()`            | `__floordiv__`  | `__rfloordiv__`   |
    | `%`             | Modulo         | `%`                   | `__mod__`       | `__rmod__`        |
    | `**`            | Power          | `power()`             | `__pow__`       | `__rpow__`        |

<!-- req: comparison-operators -->
??? note "Comparison Operators"

    | Python Operator | Description           | ClickHouse Equivalent | Python Method |
    |-----------------|-----------------------|-----------------------|---------------|
    | `==`            | Equal                 | `=`                   | `__eq__`      |
    | `!=`            | Not Equal             | `!=`                  | `__ne__`      |
    | `<`             | Less Than             | `<`                   | `__lt__`      |
    | `<=`            | Less Than or Equal    | `<=`                  | `__le__`      |
    | `>`             | Greater Than          | `>`                   | `__gt__`      |
    | `>=`            | Greater Than or Equal | `>=`                  | `__ge__`      |

    <!-- req: comparison-result-uint8 -->
    Comparison operators don't need explicit reverse methods — Python swaps `<`/`>` and `<=`/`>=` automatically (e.g., `5 < obj` becomes `obj > 5`). The result column is always `UInt8` (0/1), as in ClickHouse, whatever the operand type.

<!-- req: bitwise-operators -->
??? note "Bitwise Operators"

    | Python Operator | Description | ClickHouse Equivalent | Forward Method | Reverse Method |
    |-----------------|-------------|-----------------------|----------------|----------------|
    | `&`             | Bitwise AND | `bitAnd()`            | `__and__`      | `__rand__`     |
    | `\|`            | Bitwise OR  | `bitOr()`             | `__or__`       | `__ror__`      |
    | `^`             | Bitwise XOR | `bitXor()`            | `__xor__`      | `__rxor__`     |

## Aggregation Operators

Reduce an array to a scalar Object.

<!-- req: aggregation-operators -->
| Method            | ClickHouse Function | Notes                                            |
|-------------------|---------------------|--------------------------------------------------|
| `.min()`          | `min()`             |                                                  |
| `.max()`          | `max()`             |                                                  |
| `.sum()`          | `sum()`             |                                                  |
| `.mean()`         | `avg()`             |                                                  |
| `.std()`          | `stddevPop()`       |                                                  |
| `.var()`          | `varPop()`          |                                                  |
| `.count()`        | `count()`           |                                                  |
| `.quantile(q)`    | `quantile(q)()`     | Approximate                                      |
| `.count_if(cond)` | `countIf()`         | Scalar (str) or dict Object (dict of conditions) |

## Unique

<!-- req: unique -->
| Method       | ClickHouse Implementation                  | Notes                |
|--------------|--------------------------------------------|----------------------|
| `.unique()`  | `GROUP BY`                                 | Order not guaranteed |
| `.nunique()` | `SELECT count() FROM (... GROUP BY value)` | Fused count distinct |

## String/Regex Operators

<!-- req: string-regex-lazy -->
Pattern matching on String columns. Each method takes a `str` pattern and returns a [`LazyOperator`](lazy_results.md#lazy-operator-results-a-b-and-asum-are-plans-not-tables) — chainable, and nameable with `.as_(name, scope=...)`.

<!-- req: string-regex-operators -->
| Method           | ClickHouse Function           | Result Type | Description                   |
|------------------|-------------------------------|-------------|-------------------------------|
| `.match(p)`      | `match(val, p)`               | UInt8       | RE2 regex match (0 or 1)      |
| `.like(p)`       | `val LIKE p`                  | UInt8       | SQL LIKE (`%`, `_` wildcards) |
| `.ilike(p)`      | `val ILIKE p`                 | UInt8       | Case-insensitive LIKE         |
| `.extract(p)`    | `extract(val, p)`             | String      | Extract first capture group   |
| `.replace(p, r)` | `replaceRegexpAll(val, p, r)` | String      | Replace all regex matches     |

```python
obj = await create_object_from_value(["apple", "banana", "avocado"])

await obj.match("^a").data()                                   # [1, 0, 1]
hits = await obj.like("a%").as_("hits")                        # t_hits_<id>
count = await obj.extract("(an)").match("^an$").sum().data()   # 1 — chain stays lazy
```

ClickHouse uses RE2 regex syntax (no lookaheads/lookbehinds).

## Membership Operator: `isin()`

<!-- req: isin -->
`.isin(other)` returns a `LazyOperator` UInt8 mask computed as
`value IN (SELECT value FROM other_table)` — all data stays in the database.
`other` is an `Object` or a Python `list`.

```python
obj = await create_object_from_value(["a", "b", "c", "d"])
allowed = await create_object_from_value(["a", "c"])
await obj.isin(allowed).data()          # [1, 0, 1, 0]

# Also works with a plain Python list, and chains into an aggregation
await obj.isin(["a", "c"]).sum().data()  # 2

# Works on dict column selection
obj = await create_object_from_value({"category": ["a", "b", "c"], "val": [1, 2, 3]})
mask = await obj["category"].isin(allowed)
```

<!-- req: isin-list-literals -->
A Python list is inlined as literals (`arrayJoin([...])`) rather than loaded into a table; lists longer than `LITERAL_LIST_MAX` (1000) fall back to a table. Runnable examples: `examples/isin.py`.

## Unary Transform Operators

Apply a ClickHouse function element-wise to the value column, returning a new Object — the Object-level counterparts of the View [Domain Helpers](views.md#domain-helpers).

<!-- req: unary-transforms -->
| Method          | ClickHouse Function | Result Type | Category  |
|-----------------|---------------------|-------------|-----------|
| `.year()`       | `toYear()`          | UInt16      | Date/time |
| `.month()`      | `toMonth()`         | UInt8       | Date/time |
| `.day_of_week()`| `toDayOfWeek()`     | UInt8       | Date/time |
| `.lower()`      | `lower()`           | String      | String    |
| `.upper()`      | `upper()`           | String      | String    |
| `.length()`     | `length()`          | UInt64      | String    |
| `.trim()`       | `trimBoth()`        | String      | String    |
| `.abs()`        | `abs()`             | Float64     | Math      |
| `.log2()`       | `log2()`            | Float64     | Math      |
| `.sqrt()`       | `sqrt()`            | Float64     | Math      |

<!-- req: unary-transforms-chain -->
Results chain with any operator (e.g., `await obj.year().unique()`). Runnable examples: `examples/transforms.py`.

## Group By Operations

Pandas-style two-step: `obj.group_by('key')` returns a `GroupByQuery`, then `.sum('col')` etc.

<!-- req: group-by-aggregations -->
| Method                       | Description                       | Result Column Type                     |
|------------------------------|-----------------------------------|----------------------------------------|
| `.sum(col)`                  | Sum per group                     | Preserves int types, Float64 for float |
| `.mean(col)`                 | Average per group                 | Float64                                |
| `.min(col)`                  | Minimum per group                 | Preserves source type                  |
| `.max(col)`                  | Maximum per group                 | Preserves source type                  |
| `.count()`                   | Count rows per group              | UInt64 (column named `_count`)         |
| `.std(col)`                  | Standard deviation per group      | Float64                                |
| `.var(col)`                  | Variance per group                | Float64                                |
| `.any(col)`                  | Arbitrary non-NULL per group      | Preserves source type                  |
| `.group_array_distinct(col)` | Distinct values → Array per group | `Array(T)` where T is source type      |
| `.agg({col: op})`            | Multiple aggregations             | Per-function type rules                |

<!-- req: group-by-agg-spec -->
**`agg()` spec formats** — each dict value accepts three forms, mixable in one call:

| Form                       | Example                                                              | Behavior                              |
|----------------------------|----------------------------------------------------------------------|---------------------------------------|
| `str`                      | `{'amount': GB_SUM}`                                                 | Alias = column name (backward compat) |
| `Agg(op, alias)`           | `{'amount': Agg(GB_SUM, 'total')}`                                   | Single op with explicit alias         |
| `[Agg(op, alias), ...]`    | `{'amount': [Agg(GB_SUM, 'amt_sum'), Agg(GB_MEAN, 'amt_avg')]}`      | Multiple ops on the same column       |

<!-- req: group-by-having -->
Multiple group keys, View support, and chained `.having()`/`.or_having()` for post-aggregation filtering (same AND/OR pattern as WHERE). `.or_having()` requires a prior `.having()`. Result is a normal dict Object. See `examples/group_by.py`.

**Known gap**: No `Array(T)` column support — `groupArray()`, `groupUniqArray()`, per-group concat not available.

## Memory/Disk Settings

For large datasets, ClickHouse can spill to disk via `max_bytes_before_external_sort`, `max_bytes_in_join`, `join_algorithm`.
