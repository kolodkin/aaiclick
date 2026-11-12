Object
---

# Overview

An `Object` wraps one ClickHouse table holding a scalar, an array, or a dict.
Arithmetic, comparison, and bitwise operators work element-wise and write results
to new tables. Structure is immutable: `insert()` appends data but never changes
the schema.

See [ObjectContext](object_context.md) for lifecycle, schemas, and deployment modes, and
[Ingest](ingest.md) for loading, combining and joining data.

# API Quick Reference

| API                                             | Category         | Description                                                            | Section                                                              |
|-------------------------------------------------|------------------|------------------------------------------------------------------------|----------------------------------------------------------------------|
| `+`, `-`, `*`, `/`, `//`, `%`, `**`             | Arithmetic       | Element-wise arithmetic                                                | [Operator Support](#operator-support)                                |
| `==`, `!=`, `<`, `<=`, `>`, `>=`                | Comparison       | Element-wise comparison                                                | [Operator Support](#operator-support)                                |
| `&`, `\|`, `^`                                  | Bitwise          | Bitwise AND / OR / XOR                                                 | [Operator Support](#operator-support)                                |
| `.min()`, `.max()`, `.sum()`, `.mean()`         | Aggregation      | Reduce array to scalar                                                 | [Aggregation Operators](#aggregation-operators)                      |
| `.std()`, `.var()`, `.count()`, `.quantile(q)`  | Aggregation      | Statistical reduction                                                  | [Aggregation Operators](#aggregation-operators)                      |
| `.count_if(condition)`                          | Aggregation      | Count rows matching condition(s)                                       | [Aggregation Operators](#aggregation-operators)                      |
| `.unique()`                                     | Unique           | Deduplicate values via GROUP BY                                        | [Unique](#unique)                                                    |
| `.nunique()`                                    | Unique           | Count distinct values (fused)                                          | [Unique](#unique)                                                    |
| `.isin(other)`                                  | Membership       | IN subquery test (returns UInt8 mask)                                  | [Membership Operator](#membership-operator-isin)                     |
| `.match(p)`, `.like(p)`, `.ilike(p)`            | String / Regex   | Pattern matching (returns UInt8 mask)                                  | [String/Regex Operators](#stringregex-operators)                     |
| `.extract(p)`, `.replace(p, r)`                 | String / Regex   | Capture group extraction, regex replace                                | [String/Regex Operators](#stringregex-operators)                     |
| `.year()`, `.month()`, `.day_of_week()`         | Unary Transforms | Date/time extraction → new Object                                      | [Unary Transform Operators](#unary-transform-operators)              |
| `.lower()`, `.upper()`, `.length()`, `.trim()`  | Unary Transforms | String transforms → new Object                                         | [Unary Transform Operators](#unary-transform-operators)              |
| `.abs()`, `.log2()`, `.sqrt()`                  | Unary Transforms | Math functions → new Object                                            | [Unary Transform Operators](#unary-transform-operators)              |
| `.group_by(keys).sum(col)` etc.                 | Group By         | Aggregation with GROUP BY + optional HAVING                            | [Group By Operations](#group-by-operations)                          |
| `.group_by(keys).agg({col: spec})`              | Group By         | Multiple aggregations (multi-agg per column)                           | [Group By Operations](#group-by-operations)                          |
| `.group_by(keys).any(col)`                      | Group By         | Arbitrary non-NULL value per group                                     | [Group By Operations](#group-by-operations)                          |
| `.group_by(keys).group_array_distinct(col)`     | Group By         | Distinct values → Array per group                                      | [Group By Operations](#group-by-operations)                          |
| `.view(where, limit, offset, order_by)`         | Views            | Read-only View with optional filters                                   | [Views](#views)                                                      |
| `.select_sql(columns, order_by, limit, offset)` | Views            | The SELECT text the object reads with; Views include their constraints | [Views](#views)                                                      |
| `.where(cond)` / `.or_where(cond)`              | Views            | Fluent WHERE chaining (AND / OR)                                       | [Chained WHERE Clauses](#chained-where-clauses)                      |
| `obj[key]` / `obj[[keys]]`                      | Views            | Select column(s) from dict Object → View                               | [Column Selection](#column-selection)                                |
| `.with_columns({name: Computed(type, expr)})`   | Views            | Add SQL expression columns → View                                      | [Computed Column Expansion](#computed-column-expansion-with_columns) |
| `literal(value, ch_type)`                       | Helpers          | Constant column (`Computed` with SQL literal)                          | [Computed Column Expansion](#computed-column-expansion-with_columns) |
| `.with_year(col)`, `.with_month(col)` …         | Views            | Named shortcuts for common computed columns                            | [Domain Helpers](#domain-helpers)                                    |
| `.with_split_by_char(col, sep)`                 | Views            | Split String column by separator → Array                               | [Domain Helpers](#domain-helpers)                                    |
| `.with_isin(col, other)`                        | Views            | IN subquery computed column → UInt8                                    | [Domain Helpers](#domain-helpers)                                    |
| `.rename({old: new})`                           | Views            | Alias column names in a View                                           | [Column Renaming](#column-renaming-rename)                           |
| `.explode(*columns, left=False)`                | Views            | Flatten Array column(s) into rows → View                               | [Explode](#explode)                                                  |
| `.copy()`                                       | Copy             | Materialize Object / View to new Object                                | [copy()](#the-copy-method)                                           |
| `.insert(*sources)`                             | Ingest           | Insert data from Objects / scalars / lists                             | [insert()](ingest.md#insert)                                         |
| `.insert_from_url(url)`                         | Ingest           | Insert rows from a remote URL                                          | [insert_from_url()](ingest.md#insert_from_url)                       |
| `.concat(*sources)` / `concat(a, b, …)`         | Ingest           | Concatenate sources into a new Object                                  | [concat()](ingest.md#concat)                                         |
| `.join(other, on, how)`                         | Join             | Join two Objects on key columns → dict Object                          | [join()](ingest.md#join)                                             |
| `.data(orient=…)`                               | Data Retrieval   | Fetch results to Python (scalar / list / dict)                         | [data()](#data)                                                      |
| `.markdown(truncate=…)`                         | Data Retrieval   | Render data as markdown table                                          | [markdown()](#markdown)                                              |
| `.execute(order_by, limit, offset)`             | Data Retrieval   | Run full query, discard rows → `QueryStats`                            | [execute()](#execute)                                                |
| `await .stats()`                                | Data Retrieval   | `QueryStats` of the query that made this Object                        | [stats()](#stats)                                                    |
| `.export(path, overwrite, format, ch_settings)` | Export           | Stream data to a file or bucket URL (extension → format)               | [export()](#export)                                                  |
| `open_object_from_url(url)`                     | Blob Storage     | Read-only Object over a bucket file, queried in place                  | [Blob Storage](#blob-storage)                                        |

# Operator Support

All operators work element-wise on scalar and array data, creating new Object
tables. Python scalars work on either side (`obj * 2`, `2 * obj`) and are
inlined into the SQL as typed literals, never given a table of their own. Full
script: [Examples: Basic Operators](../examples/basic_operators.md).

## Arithmetic Type Promotion

Result types match ClickHouse's native promotion rules, verified against `SELECT toTypeName()`.

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

??? note "Comparison Operators"

    | Python Operator | Description           | ClickHouse Equivalent | Python Method |
    |-----------------|-----------------------|-----------------------|---------------|
    | `==`            | Equal                 | `=`                   | `__eq__`      |
    | `!=`            | Not Equal             | `!=`                  | `__ne__`      |
    | `<`             | Less Than             | `<`                   | `__lt__`      |
    | `<=`            | Less Than or Equal    | `<=`                  | `__le__`      |
    | `>`             | Greater Than          | `>`                   | `__gt__`      |
    | `>=`            | Greater Than or Equal | `>=`                  | `__ge__`      |

    Comparison operators don't need explicit reverse methods — Python swaps `<`/`>` and `<=`/`>=` automatically (e.g., `5 < obj` becomes `obj > 5`). The result column is always `UInt8` (0/1), as in ClickHouse, whatever the operand type.

??? note "Bitwise Operators"

    | Python Operator | Description | ClickHouse Equivalent | Forward Method | Reverse Method |
    |-----------------|-------------|-----------------------|----------------|----------------|
    | `&`             | Bitwise AND | `bitAnd()`            | `__and__`      | `__rand__`     |
    | `\|`            | Bitwise OR  | `bitOr()`             | `__or__`       | `__ror__`      |
    | `^`             | Bitwise XOR | `bitXor()`            | `__xor__`      | `__rxor__`     |

## Aggregation Operators

Reduce an array to a scalar Object.

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

| Method       | ClickHouse Implementation                  | Notes                |
|--------------|--------------------------------------------|----------------------|
| `.unique()`  | `GROUP BY`                                 | Order not guaranteed |
| `.nunique()` | `SELECT count() FROM (... GROUP BY value)` | Fused count distinct |

## String/Regex Operators

Pattern matching on String columns. Each method takes a `str` pattern and returns a [`LazyOperator`](#lazy-operator-results-a--b-and-asum-are-plans-not-tables) — chainable, and nameable with `.as_(name, scope=...)`.

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

A Python list is inlined as literals (`arrayJoin([...])`) rather than loaded into a table; lists longer than `LITERAL_LIST_MAX` (1000) fall back to a table. Runnable examples: `examples/isin.py`.

## Unary Transform Operators

Apply a ClickHouse function element-wise to the value column, returning a new Object — the Object-level counterparts of the View [Domain Helpers](#domain-helpers).

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

Results chain with any operator (e.g., `await obj.year().unique()`). Runnable examples: `examples/transforms.py`.

## Group By Operations

Pandas-style two-step: `obj.group_by('key')` returns a `GroupByQuery`, then `.sum('col')` etc.

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

**`agg()` spec formats** — each dict value accepts three forms, mixable in one call:

| Form                       | Example                                                              | Behavior                              |
|----------------------------|----------------------------------------------------------------------|---------------------------------------|
| `str`                      | `{'amount': GB_SUM}`                                                 | Alias = column name (backward compat) |
| `Agg(op, alias)`           | `{'amount': Agg(GB_SUM, 'total')}`                                   | Single op with explicit alias         |
| `[Agg(op, alias), ...]`    | `{'amount': [Agg(GB_SUM, 'amt_sum'), Agg(GB_MEAN, 'amt_avg')]}`      | Multiple ops on the same column       |

Multiple group keys, View support, and chained `.having()`/`.or_having()` for post-aggregation filtering (same AND/OR pattern as WHERE). `.or_having()` requires a prior `.having()`. Result is a normal dict Object. See `examples/group_by.py`.

**Known gap**: No `Array(T)` column support — `groupArray()`, `groupUniqArray()`, per-group concat not available.

## Memory/Disk Settings

For large datasets, ClickHouse can spill to disk via `max_bytes_before_external_sort`, `max_bytes_in_join`, `join_algorithm`.

# Lazy Operator Results (`a + b` and `a.sum()` are Plans, not Tables)

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

`await` is only needed to get the materialized `Object` back — to read `.table`, hand it to an eager API, etc. Reading rows (`.data()`, `.markdown()`, `.export()`, `.result()`) or chaining operators (`(a + b) + c`, `obj.abs().sum()`) needs no `await` of the intermediate.

## `.as_(name, scope=...)`

Names the result table and chooses its lifetime. Returns a new `LazyOperator` (the receiver is unchanged):

| `scope`                  | Table name             | Lifetime                                                               |
|--------------------------|------------------------|------------------------------------------------------------------------|
| `"temp_named"` (default) | `t_<name>_<snowflake>` | Drops with the context                                                 |
| `"job"`                  | `j_<job_id>_<name>`    | Lives until the active orch job expires                                |
| `"global"`               | `p_<name>`             | Persists; remove with `delete_persistent_object(name, scope="global")` |

## `name` / `scope` on always-materializing ops

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

On a `LazyOperator` they forward to the materialized delegate, so
`(a + b).copy(name=..., scope=...)` and `(a + b).join(b, name=..., scope=...)` work.

## Invariants

- **No DB writes until awaited.** Creating a `LazyOperator` is pure-Python. A lazy that's never awaited never creates a table.
- **Sync `.table` raises pre-materialize.** Reading `.table` (or any property derived from it — `.scope`, `.persistent`, `.order_by`) on an unawaited LazyOperator raises `RuntimeError`. `.schema` works (it's precomputed).
- **Async methods auto-materialize.** `.data()`, `.sum()`, etc. first `await self` (materializes once, cached) then delegate.
- **`.as_()` is immutable.** Calling it returns a new LazyOperator; the receiver keeps its (None, None) name/scope.
- **Re-await is idempotent (single-task).** `_materialized` cache ensures sequential awaits of the same lazy produce one table.

## Chained operators

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

`asyncio.gather(lazy.data(), lazy.data())` races: both tasks see `_materialized is None` and both materialize. The result is two tables (the second wins the cache slot; the first is orphaned and dropped via refcount cleanup). To share a result across tasks, `await` once and share the returned `Object`.

# Data Retrieval

## data()

Returns: scalar → value, array → list, dict → dict or list of dicts.

**Orient** (dict Objects):

| Constant         | Value       | Description                                  |
|------------------|-------------|----------------------------------------------|
| `ORIENT_DICT`    | `'dict'`    | Dict with arrays as values (default)         |
| `ORIENT_RECORDS` | `'records'` | List of dicts (one per row)                  |

## markdown()

Returns data as a plain-text markdown table with auto-sized columns. Optional `truncate: dict[str, int]` caps column widths. Floats → 2dp, None → `N/A`.

## execute()

Runs the same SELECT `.data()` would issue (honoring `where`, `order_by`, `limit`, `offset`, computed columns, renames, field selection), discards every row server-side (`FORMAT Null`), and returns a `QueryStats`. Use it to benchmark a View, warm caches, or assert a query *runs* without paying for transport. Per-call `order_by` / `limit` / `offset` mirror `.data()`. No safety row cap applies — the whole pipeline runs unless you pass `limit`.

```python
view = obj.view(where="score > 10").rename({"score": "s"})
stats = await view.execute()
print(stats.read_rows)   # rows scanned by the full pipeline
print(stats.elapsed_s)   # server-side wall time
# view's table is unchanged; no result rows came back
```

`QueryStats` is a frozen value type (exported from `aaiclick`). Every field is best-effort — a backend fills what it can surface and leaves the rest `None`:

| Field           | Meaning                                  | clickhouse-connect (HTTP) | chdb                |
|-----------------|------------------------------------------|---------------------------|---------------------|
| `read_rows`     | rows ClickHouse scanned                  | summary                   | `storage_rows_read` |
| `read_bytes`    | uncompressed bytes scanned               | summary                   | `storage_bytes_read`|
| `elapsed_s`     | server-side wall time, seconds           | `elapsed_ns / 1e9`        | `elapsed`           |
| `result_rows`   | rows the SELECT produced                 | summary                   | `None`              |
| `written_rows`  | rows written by an INSERT                | summary                   | `None`              |
| `written_bytes` | bytes written by an INSERT               | summary                   | `None`              |

## stats()

Returns `QueryStats | None`: populated on objects **born from a server-side query**, `None` otherwise. It is async (not a property) because on an un-awaited `LazyOperator` it first materializes it, like `.data()`.

| Object origin                                                  | `await .stats()`               |
|----------------------------------------------------------------|--------------------------------|
| `.copy()` / `.concat()` / `.join()`                            | stats of the `INSERT … SELECT` |
| operator / aggregation / `group_by` result (incl. `await a + b`) | stats of the materialization   |
| plain table-backed `Object`                                    | `None`                         |
| unexecuted `View`                                              | `None` (it never ran a query)  |

```python
result = await big_view.copy()
stats = await result.stats()
print(stats.written_rows)   # how many rows the copy wrote (HTTP)
print(stats.read_rows)      # how many it scanned to produce them
```

# Export

Export data to a local file, in the format its extension names. The embedded
chdb engine writes straight to disk with no Python round-trip, so
multi-million-row exports stay memory-bounded. View constraints (`where`,
`limit`, `order_by`) are honored. An existing file is replaced. Returns the
absolute path written.

```python
await obj.export("/tmp/data.csv")
await obj.export("/tmp/data.parquet")
await obj.view(where="score > 10", limit=1000).export("/tmp/top.jsonl")
```

## Supported extensions

| Extension                       | ClickHouse format | Notes                                  |
|---------------------------------|-------------------|----------------------------------------|
| `.csv`                          | `CSV`             | no header row                          |
| `.tsv`                          | `TSV`             | no header row                          |
| `.json`                         | `JSON`            | one document: `meta`, `data`, `rows`   |
| `.jsonl` / `.ndjson`            | `JSONEachRow`     | newline-delimited JSON                 |
| `.parquet`                      | `Parquet`         | columnar, best for large datasets      |
| `.arrow`                        | `Arrow`           | Apache Arrow IPC stream                |
| `.orc`                          | `ORC`             | Apache ORC columnar                    |
| `.avro`                         | `Avro`            | Avro container with embedded schema    |
| `.md`                           | `Markdown`        | pipe-delimited markdown table          |
| `.xml`                          | `XML`             | one `<row>` element per record         |
| `.sql`                          | `SQLInsert`       | `INSERT INTO table VALUES (...)`       |

These are the formats ClickHouse itself picks for each extension. `format=`
overrides the extension with any ClickHouse output format:

```python
await obj.export("/tmp/data.csv", format="CSVWithNames")  # with a header row
await obj.export("/tmp/data.bin", format="Native")
```

`ch_settings=` sends ClickHouse settings with the export statement:

```python
await obj.export("/tmp/data.csv", ch_settings={"format_csv_delimiter": ";"})
await obj.export(url, ch_settings={"output_format_parquet_compression_method": "snappy"})
```

It cannot change whether an existing target is replaced (`overwrite` does, for
buckets), and `s3_allow_server_credentials_in_user_queries` raises: that stays
a [profile setting](#credentials).

## Compression

Append a compression suffix and ClickHouse compresses the file: `.gz`, `.xz`,
`.zst`, `.br`, `.lz4`, `.snappy`, or `.bz2` where the build supports it.

```python
await obj.export("/tmp/data.csv.gz")
await obj.export("/tmp/data.parquet.gz")
await obj.export("/tmp/data.json.xz")
```

!!! warning "Local files need the chdb backend"
    A remote ClickHouse cannot reach the client's disk, so a local path raises
    `ValueError` there. Export to a [bucket](#bucket-targets) instead.

## Bucket targets

An `http(s)://` or `s3://` path is written to object storage by ClickHouse
itself, on both backends, and `export` returns the URL:

```python
url = await daily.export("s3://lake/reports/daily_2026_10.parquet")
await daily.export(url, overwrite=True)  # an existing key raises without it
```

Compression suffixes and view constraints work as for local files.
Credentials: [Blob Storage](#blob-storage).

# Blob Storage

`open_object_from_url` opens a file in S3-compatible storage as an Object and
queries it in place — nothing is ingested. With `export` it lets one job hand
a result to the next as a plain URL:

```python
url = await daily.export("s3://lake/reports/daily.parquet")

# next job, any cluster
daily = await open_object_from_url(url)
await daily.view(where="country = 'US'")["revenue"].sum().data()
```

- **Schema** is inferred from the file; `columns=` narrows it,
  `column_types=` skips inference.
- **Format** follows the extension as in [Export](#supported-extensions).
  `format=` takes any ClickHouse input format; with an unmapped or missing
  extension ClickHouse detects it from the content.
- **CSV / TSV headers**: ClickHouse detects a header row; a headerless file
  exposes columns as `c1`, `c2`, ….
- **Read-only**: `insert` and `insert_from_url` raise.
- **Temporary**: the Object is a pointer dropped at context exit; the file is
  never touched.
- **Shape**: a dict Object, or an array when the file's only column is
  `value` (what an exported array has).
- **URLs**: `s3://bucket/key` for AWS, `http(s)://host/bucket/key` for
  S3-compatible servers such as MinIO. Globs pass through to ClickHouse;
  userinfo and query parameters (signed URLs) are rejected.

## Local files

On the embedded chdb backend the same call opens a file on local disk, so an
exported path feeds straight back in. A remote ClickHouse raises `ValueError`,
as local export does.

```python
path = await daily.export("/data/reports/daily.parquet")
daily = await open_object_from_url(path)
```

## Credentials

URLs never carry credentials; ClickHouse resolves them from its own
environment (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, or an IAM role).

| Backend           | Where the credentials go         | Extra setup                |
|-------------------|----------------------------------|----------------------------|
| chdb (embedded)   | the aaiclick process environment | none                       |
| ClickHouse server | the server process environment   | a `users.xml` profile line |

A ClickHouse server refuses to use its own S3 identity for user queries unless
the user's profile opts in. Mount this as
`/etc/clickhouse-server/users.d/s3.xml` (the repo ships it at
`docker/clickhouse/users.d/s3.xml`):

```xml
<clickhouse>
  <profiles>
    <default>
      <s3_allow_server_credentials_in_user_queries>1</s3_allow_server_credentials_in_user_queries>
    </default>
  </profiles>
</clickhouse>
```

The compose and Helm scaffolds mount the same file with the profile commented
out, next to commented `AWS_*` entries for the ClickHouse container: uncomment
both once you have bucket credentials.

!!! warning "Without the profile line a private bucket fails with `ACCESS_DENIED`"
    Only public buckets work on a server that has not opted in. aaiclick has
    no setting of its own for this: lending the server's identity is the
    ClickHouse operator's decision.

# Views

Read-only filtered projection of an Object — same table, no data copy. Created via `obj.view(where=..., limit=..., offset=..., order_by=...)`. Supports all read operations; cannot `insert()`. See `examples/view_examples.py`.

## Chained WHERE Clauses

- `obj.where(cond)` — creates View with initial WHERE
- `view.where(cond)` — AND-chains: `.where('x > 10').where('y < 20')` → `WHERE (x > 10) AND (y < 20)`
- `view.or_where(cond)` — OR-chains: `.where('x > 100').or_where('y < 5')` → `WHERE (x > 100) OR (y < 5)`

!!! warning "`or_where()` requires a prior `where()`"
    Calling `or_where()` without a preceding `where()` raises `ValueError`.
    Same applies to `or_having()` on `GroupByQuery`.

## Column Selection

Select columns from a dict Object → View (same table, restricted SELECT).

- `obj["col"]` — single column → array-like View (single `value` field)
- `obj[["col_a", "col_b"]]` — multiple columns → dict-like View

```python
obj = await create_object_from_value({"x": [1, 2, 3], "y": [10, 20, 30]})

# Single column — returns array-like View
await obj["x"].data()              # [1, 2, 3]
arr = await obj["x"].copy()        # new array Object

# Multiple columns — returns dict-like View
await obj[["x", "y"]].data()       # {'x': [1, 2, 3], 'y': [10, 20, 30]}
```

Preserves WHERE and computed column constraints when chained on a filtered View. The selection is the View's output: computed columns it omits are dropped from `data()` and `copy()`, while ones added after a multi-field selection join it. A single-field selection can't take computed columns — add them before selecting.

```python
s = {"s": Computed("Int64", "x + y")}
await obj.with_columns(s)[["x", "y"]].data()  # {'x': [...], 'y': [...]}
await obj[["x", "y"]].with_columns(s).data()  # {'x': [...], 'y': [...], 's': [...]}
```

## Computed Column Expansion: `with_columns()`

`with_columns({name: Computed(type, expression)})` adds SQL-expression columns as a View — no new table, no data copy. `Computed` comes from `aaiclick.data.models`.

- Synchronous (no `await`). Works on Objects and Views; preserves existing constraints.
- Always returns a dict-like View: array sources promote to dict; scalar sources raise `ValueError`.
- Adds, never replaces — a column name collision raises `ValueError`.
- Expressions pass verbatim to ClickHouse. Basic validation rejects semicolons and `SELECT`; type mismatches surface from ClickHouse at query time.
- Chains with every View operation: `group_by()`, `where()`, column selection, further (additive) `with_columns()`, and operators on selected columns.

## `literal()` Helper

Convenience wrapper for constant columns — handles SQL quoting:

```python
from aaiclick import literal

# Before: manual quoting
obj.with_columns({"source": Computed("String", "'dataset_a'")})

# After: literal() handles it
obj.with_columns({"source": literal("dataset_a", "String")})
obj.with_columns({"flag": literal(True, "UInt8")})
obj.with_columns({"weight": literal(1.0, "Float64")})
```

Supported types: `str` (quoted), `int`/`float` (bare), `bool` (`true`/`false`).

## Domain Helpers

Named shortcuts that delegate to `with_columns()`. Each auto-names the result column; all accept `alias=` override and return a `View`.

| Helper                                       | Default Alias          | Type            | Expression                       |
|----------------------------------------------|------------------------|-----------------|----------------------------------|
| `with_year(col)`                             | `{col}_year`           | `UInt16`        | `toYear(col)`                    |
| `with_month(col)`                            | `{col}_month`          | `UInt8`         | `toMonth(col)`                   |
| `with_day_of_week(col)`                      | `{col}_dow`            | `UInt8`         | `toDayOfWeek(col)`               |
| `with_date_diff(unit, col_a, col_b)`         | `{col_a}_{col_b}_diff` | `Int64`         | `dateDiff('unit', col_a, col_b)` |
| `with_lower(col)`                            | `{col}_lower`          | `String`        | `lower(col)`                     |
| `with_upper(col)`                            | `{col}_upper`          | `String`        | `upper(col)`                     |
| `with_length(col)`                           | `{col}_length`         | `UInt64`        | `length(col)`                    |
| `with_trim(col)`                             | `{col}_trimmed`        | `String`        | `trim(col)`                      |
| `with_abs(col)`                              | `{col}_abs`            | `Float64`       | `abs(col)`                       |
| `with_log2(col)`                             | `{col}_log2`           | `Float64`       | `log2(col)`                      |
| `with_sqrt(col)`                             | `{col}_sqrt`           | `Float64`       | `sqrt(col)`                      |
| `with_bucket(col, size)`                     | `{col}_bucket`         | `Int64`         | `intDiv(col, size)`              |
| `with_hash_bucket(col, n)`                   | `{col}_hash`           | `UInt64`        | `cityHash64(col) % n`            |
| `with_if(cond, then, else, *, alias)`        | required `alias`       | `String`        | `if(cond, then, else)`           |
| `with_multi_if(branches, *, default, alias)` | required `alias`       | `String`        | `multiIf(c1, r1, …, default)`    |
| `with_cast(col, ch_type)`                    | `{col}_{type_lower}`   | `ch_type`       | `to{Type}(col)`                  |
| `with_split_by_char(col, sep)`               | `{col}_parts`          | `Array(String)` | `splitByChar(sep, col)`          |
| `with_isin(col, other)`                      | `{col}_isin`           | `UInt8`         | `col IN (SELECT value FROM …)`   |

`with_multi_if()` takes `(condition, result)` pairs — plain 2-tuples or `Branch` from `aaiclick.data.models` — and the earliest
match wins. `default` is required: ClickHouse's `multiIf` has no implicit else. Both halves are raw
SQL, so `AND` / `OR` / `NOT` / `IN` compose in a condition, and a result may be a literal, a column,
or an expression.

!!! warning "Conditions must be `UInt8`"
    ClickHouse rejects a bare numeric column with `Illegal type Int64 of argument (condition)`.
    Write `is_member = 1`, not `is_member`.


## Column Renaming: `rename()`

Returns a **View** aliasing column names (`old AS new`). Synchronous, no `await`.

```python
from aaiclick import literal

# Rename camelCase columns to snake_case for a consolidated table
kev_view = kev.rename({
    "cveID": "cve_id",
    "vendorProject": "vendor",
    "vulnerabilityName": "vulnerability_name",
}).with_columns({
    "source": literal("kev", "String"),
})
await consolidated.insert(kev_view)
```

Chainable with `with_columns()`, `where()`, and other View operations. New names must not collide with non-renamed columns.

## Explode

`.explode(*columns, left=False)` flattens Array column(s) into rows, duplicating scalar columns; exploded columns change from `Array(T)` to `T`. Returns a **View**, so downstream operators fuse into a single query. See `aaiclick/data/examples/explode.py`.

# The copy() Method

Full data copy → new Object. Works on both Objects and Views — filters, computed columns, and ORDER BY are preserved.

```python
# Copy an array Object
obj_copy = await obj.copy()

# Materialize a View into a new Object
arr = await obj["x"].copy()         # array Object from dict column
subset = await obj.where("x > 5").copy()  # filtered copy

# Sorted copy — ORDER BY is preserved
sorted_copy = await obj.view(order_by="amount DESC").copy()
await sorted_copy.data()  # returns rows sorted by amount DESC
```

The result carries the `INSERT … SELECT` [stats()](#stats).

`Schema` = "what a table is" and `ViewSchema` = "how to read it"; the registry never sees the latter. `create_object()` rejects a View's `.schema`; materialize a View with `copy()`. See [Schema Storage](object_context.md#schema-storage).

!!! warning "`copy()` is not serialized across workers"
    Unlike `insert()` and `concat()`, `copy()` does not take a per-table
    advisory lock. Two concurrent `copy()` calls into the same named
    destination will interleave their rows non-deterministically. Structure
    your pipeline so a named destination has a single writer.

# Operation Provenance (Oplog)

Every Object operation is recorded for lineage tracing — see [Lineage via MCP](lineage.md).
