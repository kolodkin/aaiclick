Object Class Documentation
---

# Overview

**Implementation**: `aaiclick/data/object.py` — see `Object` and `View` classes

The `Object` class represents data stored in ClickHouse tables. Each Object instance corresponds to a ClickHouse table and supports operations through operator overloading that create new tables with results.

**Key Features:**
- Operator overloading for arithmetic, comparison, and bitwise operations
- Immutable operations (all operations return new Objects)
- Support for scalars, arrays, and dictionaries
- Element-wise operations on arrays

For context management, deployment modes, table schemas, data types, lifecycle tracking, and URL loading, see [DataContext documentation](data_context.md).

# API Quick Reference

| API                                                         | Category          | Description                                  | Section                                                                         |
|-------------------------------------------------------------|-------------------|----------------------------------------------|---------------------------------------------------------------------------------|
| `+`, `-`, `*`, `/`, `//`, `%`, `**`                        | Arithmetic        | Element-wise arithmetic                      | [Arithmetic Operators](#arithmetic-operators)                                   |
| `==`, `!=`, `<`, `<=`, `>`, `>=`                           | Comparison        | Element-wise comparison                      | [Comparison Operators](#comparison-operators)                                   |
| `&`, `\|`, `^`                                              | Bitwise           | Bitwise AND / OR / XOR                       | [Bitwise Operators](#bitwise-operators)                                         |
| `.min()`, `.max()`, `.sum()`, `.mean()`                     | Aggregation       | Reduce array to scalar                       | [Aggregation Operators](#aggregation-operators)                                 |
| `.std()`, `.var()`, `.count()`, `.quantile(q)`              | Aggregation       | Statistical reduction                        | [Aggregation Operators](#aggregation-operators)                                 |
| `.unique()`                                                 | Set               | Deduplicate values via GROUP BY              | [Set Operators](#set-operators)                                                 |
| `.match(p)`, `.like(p)`, `.ilike(p)`                       | String / Regex    | Pattern matching (returns UInt8 mask)        | [String/Regex Operators](#stringregex-operators)                                |
| `.extract(p)`, `.replace(p, r)`                             | String / Regex    | Capture group extraction, regex replace      | [String/Regex Operators](#stringregex-operators)                                |
| `.year()`, `.month()`, `.day_of_week()`                     | Unary Transforms  | Date/time extraction → new Object            | [Unary Transform Operators](#unary-transform-operators)                         |
| `.lower()`, `.upper()`, `.length()`, `.trim()`              | Unary Transforms  | String transforms → new Object               | [Unary Transform Operators](#unary-transform-operators)                         |
| `.abs()`, `.log2()`, `.sqrt()`                              | Unary Transforms  | Math functions → new Object                  | [Unary Transform Operators](#unary-transform-operators)                         |
| `.group_by(keys).sum(col)` etc.                             | Group By          | Aggregation with GROUP BY + optional HAVING  | [Group By Operations](#group-by-operations)                                     |
| `.group_by(keys).agg({col: op})`                            | Group By          | Multiple aggregations in one pass            | [Group By Operations](#group-by-operations)                                     |
| `.explode(*columns, left=False)`                             | Explode           | Flatten Array column(s) into rows → View     | [Explode](#explode-implemented)                                                 |
| `.view(where, limit, offset, order_by)`                     | Views             | Read-only View with optional filters         | [Views](#views)                                                                 |
| `.where(cond)` / `.or_where(cond)`                          | Views             | Fluent WHERE chaining (AND / OR)             | [Chained WHERE Clauses](#chained-where-clauses)                                 |
| `.with_columns({name: Computed(type, expr)})`               | Computed Columns  | Add SQL expression columns → View            | [Computed Column Expansion](#computed-column-expansion-with_columns-implemented)|
| `.with_year(col)`, `.with_month(col)`, `.with_lower(col)` … | Domain Helpers   | Named shortcuts for common computed columns  | [Domain Helpers](#domain-helpers-implemented)                                   |
| `.rename({old: new})`                                       | Column Renaming   | Alias column names in a View                 | [Column Renaming](#column-renaming-rename-implemented)                          |
| `.insert(*sources)`                                         | Data Ingestion    | Insert data from Objects / scalars / lists   | [insert()](#the-insert-method)                                                  |
| `.insert_from_url(url)`                                     | Data Ingestion    | Insert rows from a remote URL                | [insert_from_url()](#insert_from_url-implemented)                               |
| `.concat(*sources)` / `concat(a, b, …)`                     | Data Ingestion    | Concatenate sources into a new Object        | [concat()](#the-concat-method)                                                  |
| `.data(orient=…)`                                           | Data Retrieval    | Fetch results to Python (scalar / list / dict)| [data()](#the-data-method)                                                     |

# Operator Support

**Implementation**: `aaiclick/data/object.py` (dunder methods) delegates to `aaiclick/data/operators.py` (async functions).

All operators work element-wise on both scalar and array data. Each operator creates a new Object table with the result via `_apply_operator()` using SQL templates (`apply_op_scalar.sql`, `apply_op_array.sql`).

See [operators.md](operators.md) for complete function reference. For runnable examples, see `examples/basic_operators.py`.

## Scalar Broadcast

**Implementation**: `aaiclick/data/object.py` — see `_ensure_object()`

All binary operators accept Python scalars (`int`, `float`, `bool`, `str`) on either side. The scalar is converted to a scalar Object via `create_object_from_value`, so all data stays in ClickHouse with a unified code path. This works for both `obj + 5` (forward) and `5 + obj` (reverse).

**How it works**: `_ensure_object()` converts scalars to Objects first, then the caller uses `_get_query_info()` for all operands — no special-case SQL generation.

Reverse operators (`__radd__`, `__rsub__`, etc.) call `_apply_reverse_operator()` which swaps the operand order so the scalar appears on the left in SQL.

For runnable examples, see `examples/basic_operators.py`.

## Arithmetic Operators

| Python Operator | Description    | ClickHouse Equivalent | Forward Method  | Reverse Method    |
|-----------------|----------------|-----------------------|-----------------|-------------------|
| `+`             | Addition       | `+`                   | `__add__`       | `__radd__`        |
| `-`             | Subtraction    | `-`                   | `__sub__`       | `__rsub__`        |
| `*`             | Multiplication | `*`                   | `__mul__`       | `__rmul__`        |
| `/`             | Division       | `/`                   | `__truediv__`   | `__rtruediv__`    |
| `//`            | Floor Division | `intDiv()`            | `__floordiv__`  | `__rfloordiv__`   |
| `%`             | Modulo         | `%`                   | `__mod__`       | `__rmod__`        |
| `**`            | Power          | `power()`             | `__pow__`       | `__rpow__`        |

## Comparison Operators

| Python Operator | Description           | ClickHouse Equivalent | Python Method |
|-----------------|-----------------------|-----------------------|---------------|
| `==`            | Equal                 | `=`                   | `__eq__`      |
| `!=`            | Not Equal             | `!=`                  | `__ne__`      |
| `<`             | Less Than             | `<`                   | `__lt__`      |
| `<=`            | Less Than or Equal    | `<=`                  | `__le__`      |
| `>`             | Greater Than          | `>`                   | `__gt__`      |
| `>=`            | Greater Than or Equal | `>=`                  | `__ge__`      |

Comparison operators don't need explicit reverse methods — Python swaps `<`/`>` and `<=`/`>=` automatically (e.g., `5 < obj` becomes `obj > 5`).

## Bitwise Operators

| Python Operator | Description | ClickHouse Equivalent | Forward Method | Reverse Method |
|-----------------|-------------|-----------------------|----------------|----------------|
| `&`             | Bitwise AND | `bitAnd()`            | `__and__`      | `__rand__`     |
| `\|`            | Bitwise OR  | `bitOr()`             | `__or__`       | `__ror__`      |
| `^`             | Bitwise XOR | `bitXor()`            | `__xor__`      | `__rxor__`     |

## Aggregation Operators

Reduce an array to a scalar Object. All computation in ClickHouse, streaming O(1) memory.

| Method         | ClickHouse Function | Notes                |
|----------------|---------------------|----------------------|
| `.min()`       | `min()`             |                      |
| `.max()`       | `max()`             |                      |
| `.sum()`       | `sum()`             |                      |
| `.mean()`      | `avg()`             |                      |
| `.std()`       | `stddevPop()`       |                      |
| `.var()`       | `varPop()`          |                      |
| `.count()`     | `count()`           |                      |
| `.quantile(q)` | `quantile(q)()`     | Approximate          |

## Set Operators

| Method      | ClickHouse Implementation | Notes                          |
|-------------|---------------------------|--------------------------------|
| `.unique()` | `GROUP BY`                | Order not guaranteed           |

## String/Regex Operators

**Implementation**: `aaiclick/data/object.py` (methods) delegates to `aaiclick/data/operators.py` — see `_apply_string_op_db()`

Pattern matching on String columns. All methods take a Python `str` pattern and return a new Object. Results can be chained with further operations (e.g., `match()` → `sum()` to count matches).

| Method              | ClickHouse Function          | Result Type | Description                        |
|---------------------|------------------------------|-------------|------------------------------------|
| `.match(p)`         | `match(val, p)`              | UInt8       | RE2 regex match (0 or 1)          |
| `.like(p)`          | `val LIKE p`                 | UInt8       | SQL LIKE (`%`, `_` wildcards)     |
| `.ilike(p)`         | `val ILIKE p`                | UInt8       | Case-insensitive LIKE             |
| `.extract(p)`       | `extract(val, p)`            | String      | Extract first capture group        |
| `.replace(p, r)`    | `replaceRegexpAll(val, p, r)`| String      | Replace all regex matches          |

**Note**: ClickHouse uses RE2 regex syntax (no lookaheads/lookbehinds).

## Unary Transform Operators

**Implementation**: `aaiclick/data/object.py` (methods) delegates to `aaiclick/data/operators.py` — see `unary_transform()`

Apply a ClickHouse function element-wise to the value column, returning a new Object. These are the Object-level equivalents of [Domain Helpers](#domain-helpers--implemented) (`with_year`, `with_lower`, etc.) which operate on Views.

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

Results are full Objects — chainable with any operator (e.g., `await (await obj.year()).unique()`).

**When to use which**:
- **Object methods** (`obj.year()`) — single-column Objects, returns a new Object
- **View helpers** (`view.with_year('col')`) — multi-column dicts, adds a computed column to the View

**Tests**: `aaiclick/data/test_unary_transforms.py`. For runnable examples, see `examples/transforms.py`.

## Group By Operations

**Implementation**: `aaiclick/data/object.py` — see `GroupByQuery` class, `aaiclick/data/operators.py` — see `group_by_agg()`

Pandas-style two-step: `obj.group_by('key').sum('col')`. `GroupByQuery` is a standalone intermediate class (not an Object subclass).

**Aggregation methods on GroupByQuery:**

| Method             | Description                  | Result Column Type                     |
|--------------------|------------------------------|----------------------------------------|
| `.sum(col)`        | Sum per group                | Preserves int types, Float64 for float |
| `.mean(col)`       | Average per group            | Float64                                |
| `.min(col)`        | Minimum per group            | Preserves source type                  |
| `.max(col)`        | Maximum per group            | Preserves source type                  |
| `.count()`         | Count rows per group         | UInt64 (column named `_count`)         |
| `.std(col)`        | Standard deviation per group | Float64                                |
| `.var(col)`        | Variance per group           | Float64                                |
| `.agg({col: op})`  | Multiple aggregations        | Per-function type rules                |

**Features**: Multiple group keys, chained `.having()`/`.or_having()` for post-aggregation filtering, View support (WHERE + selected_fields). Result is a normal dict Object supporting all existing operations.

**HAVING clause chaining** — same pattern as WHERE chaining on Views. `.having(cond)` chains with AND, `.or_having(cond)` chains with OR. `.or_having()` requires a prior `.having()` — raises `ValueError` otherwise. For examples, see `examples/group_by.py`.

**Known gap**: No `Array(T)` column support — `groupArray()`, `groupUniqArray()`, per-group concat not available.

## Memory/Disk Settings

For large datasets, ClickHouse can spill to disk via `max_bytes_before_external_sort`, `max_bytes_in_join`, `join_algorithm`.

# Loading Data from URLs

For `create_object_from_url()` (creates a new Object from a URL), see [DataContext documentation](data_context.md) — "Loading Data from URLs".

## insert_from_url() ✅ IMPLEMENTED

**Implementation**: `aaiclick/data/object.py` — see `Object.insert_from_url()`

Insert data from a URL into an existing Object. Schema created once, multiple workers can insert.

# The insert() Method

**Implementation**: `aaiclick/data/object.py` — see `Object.insert()`, `aaiclick/data/ingest.py` — see `insert_objects_db()`

Inserts data from one or more sources into an existing Object. Target must be array; sources can be Objects (array or scalar), Python scalars, or lists. Sources may have a subset of target columns — missing columns get their ClickHouse default values.

- **Variadic**: `obj.insert(a, b, c)` — any number of sources in one call
- **Subset columns**: sources don't need all target columns
- **View support**: sources can be Views with `where()`, `with_columns()`, field selection, etc.
- **Type casting**: source column types are `CAST` to target column types

# The concat() Method

**Implementation**: `aaiclick/data/object.py` — see `Object.concat()`, `aaiclick/data/ingest.py` — see `concat_objects_db()`

Concatenates multiple sources into a new Object. Self must be array; args can be Objects (array or scalar), Python scalars, or lists. Also available as standalone function `concat(obj_a, obj_b, ...)`.

- **Variadic**: `obj.concat(a, b, c)` — any number of sources in one call
- **Nullable promotion**: if any source has nullable columns, the result column is promoted to `Nullable`
- **Compatible types**: all sources must have matching column names and compatible ClickHouse types

# Shared Insert Mechanics

**Implementation**: `aaiclick/data/ingest.py` — see `_insert_source()`

Both `insert()` and `concat()` delegate to `_insert_source()` which executes one `INSERT INTO target (cols) SELECT aai_id, CAST(...) FROM source` per source. This shared helper takes a `dict[str, ColumnInfo]` mapping data column names to their target types. Computed columns from Views are already resolved to `ColumnInfo` by the time they reach `_insert_source()` — the computed SQL expressions are embedded in the View's subquery.

# The data() Method

**Implementation**: `aaiclick/data/object.py` — see `Object.data()`

Returns values based on data type: scalar → value, array → list, dict → dict or list of dicts.

**Orient parameter** (for dict Objects with multiple rows):

| Constant         | Value       | Description                                  |
|------------------|-------------|----------------------------------------------|
| `ORIENT_DICT`    | `'dict'`    | Dict with arrays as values (default)         |
| `ORIENT_RECORDS` | `'records'` | List of dicts (one per row)                  |

# Explode ✅ IMPLEMENTED

**Implementation**: `aaiclick/data/object.py` — see `Object.explode()` method

Flattens Array column(s) into individual rows. Each array element becomes its own row; scalar columns are duplicated. Returns a **View** (lazy, no materialization) — downstream operators fuse into a single SQL query.

```python
obj = await create_object_from_value([
    {"user": "Alice", "tags": ["python", "rust"], "scores": [90, 85]},
    {"user": "Bob",   "tags": ["python", "go"],   "scores": [70, 95]},
])

# Explode + unique
flat = obj.explode("tags")
unique_tags = await flat["tags"].unique()
await unique_tags.data()  # ["go", "python", "rust"]

# Explode + group_by + count
tag_counts = await flat.group_by("tags").count()
await tag_counts.data()  # {"tags": [...], "_count": [...]}

# Explode multiple columns (zip, not Cartesian)
flat2 = obj.explode("tags", "scores")
await flat2.data()  # {"user": [...], "tags": [...], "scores": [...]}

# LEFT explode — preserve rows with empty arrays (ClickHouse type default, not NULL)
flat3 = obj2.explode("tags", left=True)

# Materialize
materialized = await flat.copy()
```

**Schema change**: exploded columns change from `Array(T)` to `T` in the View's effective schema.

**Tests**: `aaiclick/data/test_explode.py`

# Views

**Implementation**: `aaiclick/data/object.py` — see `View` class

Read-only filtered view of an Object — references the same table, no data copy.

Created via `obj.view(where=..., limit=..., offset=..., order_by=...)`. Supports all read operations (`.data()`, operators, aggregations). Cannot `insert()`.

For runnable examples, see `examples/view_examples.py`.

## Chained WHERE Clauses

**Implementation**: `aaiclick/data/object.py` — see `Object.where()`, `View.where()`, `View.or_where()`

Fluent API for building WHERE conditions. `Object.where()` creates a View; `View.where()` and `View.or_where()` chain additional conditions. Each call returns a **new** View (immutable).

- `obj.where(cond)` — creates View with initial WHERE condition
- `view.where(cond)` — AND-chains: `.where('x > 10').where('y < 20')` → `WHERE (x > 10) AND (y < 20)`
- `view.or_where(cond)` — OR-chains: `.where('x > 100').or_where('y < 5')` → `WHERE (x > 100) OR (y < 5)`

**Note**: `or_where()` requires a prior `where()` — raises `ValueError` otherwise.

# Computed Column Expansion: `with_columns()` ✅ IMPLEMENTED

## Motivation

`group_by()` only accepts column names, not SQL expressions. When you need to group by a derived value (e.g., `toYear(dateAdded)`, `lower(name)`), the workaround is manual: create an intermediate Object, populate it with `INSERT...SELECT`, then group. `with_columns()` automates this pattern.

See `aaiclick/example_projects/cyber_threat_feeds/__init__.py` — `analyze_kev_by_year()` for the manual workaround in production code.

## Design: SELECT Expression Approach (No Materialization)

`with_columns()` returns a **View** whose SELECT list includes `expr AS name` aliases alongside existing columns. No new table, no data copy, no schema mutation — the computed column exists only in the View's query.

**Why SELECT expressions over alternatives:**

| Aspect                    | Materialized table           | ALIAS column                        | SELECT expression (chosen)     |
|---------------------------|------------------------------|-------------------------------------|--------------------------------|
| Schema change             | New table created            | `ALTER TABLE ADD COLUMN ... ALIAS`  | None                           |
| Storage                   | Full copy                    | Zero                                | Zero                           |
| Table mutation             | No (new table)               | Yes (adds metadata to table)        | No                             |
| Persistence               | Permanent column             | Permanent column                    | Exists only in that View       |
| `SELECT *` visibility     | Yes                          | No (must name explicitly)           | Yes (in the View)              |
| Reusable across Views     | Yes (real column)            | Yes (any query can reference it)    | No (must repeat expression)    |
| Composability             | Returns Object               | Mutates table                       | Returns View (chainable)       |
| Performance               | O(n) INSERT                  | O(1) ALTER                          | O(1) View creation             |

SELECT expressions are the simplest and most composable — they align with how Views already work. The computed column is just another entry in the View's SELECT list.

## API

**Implementation**: `aaiclick/data/object.py` — see `Object.with_columns()` and `View.with_columns()` methods

**Implementation**: `aaiclick/data/models.py` — see `Computed` class (NamedTuple with `type` and `expression` fields)

`with_columns()` is synchronous — it creates a View, no database call needed. No `await`. Works on both Object and View. On Views, preserves existing constraints (WHERE, LIMIT, OFFSET, ORDER BY) and adds computed columns to the SELECT list.

**Parameters**: `columns: dict[str, Computed]` — mapping of column name to `Computed(type, expression)`. Expression is passed verbatim to ClickHouse.

**Returns**: View with original columns + computed expression aliases.

**Raises**: `ValueError` if column name collides with existing, if called on scalar Object, or if columns dict is empty. `RuntimeError` if Object is stale.

**Examples**: See `aaiclick/data/test_with_columns.py` for usage patterns including basic computed columns, chaining, group_by integration, and error cases.

## Result Schema Rules

The result is always a **View** with dict-like schema (`fieldtype='d'`):

| Source Type           | Behavior                                                          |
|-----------------------|-------------------------------------------------------------------|
| Array (`value` col)   | View selects `value` + computed columns → promotes to dict        |
| Dict (named columns)  | View selects all existing columns + computed columns              |
| Scalar                | **Rejected** — raises `ValueError`                               |
| View (single field)   | View selects source field + computed columns                      |
| View (multi field)    | View selects selected fields + computed columns                   |
| View (with WHERE)     | WHERE preserved, computed columns added to SELECT                 |

## Column Name Collision

Computed column names must not collide with existing column names. `with_columns()` adds new columns, it doesn't replace. To replace an existing column, use `drop_columns()` first (future) or work with the raw SQL pattern.

## Implementation Details

**ViewSchema extension**: `ViewSchema` gains a `computed_columns: Optional[Dict[str, Computed]]` field — see `aaiclick/data/models.py`.

**View._build_select()**: When `computed_columns` is set, expands `*` to explicit columns plus `expr AS name` for each computed column — see `aaiclick/data/object.py`.

## Chaining

`with_columns()` returns a View, so all View operations work: `group_by()`, `where()`, column selection, further `with_columns()` calls (additive), and operators on selected columns.

## Security: SQL Expression Validation

SQL expressions are passed verbatim to ClickHouse. Basic validation rejects semicolons (prevents statement injection) and subqueries (`SELECT` keyword). Type mismatches are caught by ClickHouse at query time.

**Implementation**: `aaiclick/data/object.py` — see `_validate_expression()`

## Domain Helpers ✅ IMPLEMENTED

**Implementation**: `aaiclick/data/object.py` — methods on `Object` class, delegating to `with_columns()`

Each helper auto-names the result column and auto-selects the ClickHouse type. All accept `alias=` to override the default name. All return a `View`.

| Helper                                    | Default Alias         | Type      | Expression                            |
|-------------------------------------------|-----------------------|-----------|---------------------------------------|
| `with_year(col)`                          | `{col}_year`          | `UInt16`  | `toYear(col)`                         |
| `with_month(col)`                         | `{col}_month`         | `UInt8`   | `toMonth(col)`                        |
| `with_day_of_week(col)`                   | `{col}_dow`           | `UInt8`   | `toDayOfWeek(col)`                    |
| `with_date_diff(unit, col_a, col_b)`      | `{col_a}_{col_b}_diff`| `Int64`   | `dateDiff('unit', col_a, col_b)`      |
| `with_lower(col)`                         | `{col}_lower`         | `String`  | `lower(col)`                          |
| `with_upper(col)`                         | `{col}_upper`         | `String`  | `upper(col)`                          |
| `with_length(col)`                        | `{col}_length`        | `UInt64`  | `length(col)`                         |
| `with_trim(col)`                          | `{col}_trimmed`       | `String`  | `trim(col)`                           |
| `with_abs(col)`                           | `{col}_abs`           | `Float64` | `abs(col)`                            |
| `with_log2(col)`                          | `{col}_log2`          | `Float64` | `log2(col)`                           |
| `with_sqrt(col)`                          | `{col}_sqrt`          | `Float64` | `sqrt(col)`                           |
| `with_bucket(col, size)`                  | `{col}_bucket`        | `Int64`   | `intDiv(col, size)`                   |
| `with_hash_bucket(col, n)`               | `{col}_hash`          | `UInt64`  | `cityHash64(col) % n`                |
| `with_if(cond, then, else, *, alias)`     | required `alias`      | `String`  | `if(cond, then, else)`                |
| `with_cast(col, ch_type)`                 | `{col}_{type_lower}`  | `ch_type` | `to{Type}(col)`                       |

`with_columns()` remains the public power-user interface for arbitrary expressions via `Computed(type, expression)`.

**Tests**: `aaiclick/data/test_with_columns.py`

# Column Renaming: `rename()` ✅ IMPLEMENTED

**Implementation**: `aaiclick/data/object.py` — see `Object.rename()` method

`rename()` returns a **View** whose SELECT list aliases old column names to new ones (`old AS new`). No new table, no data copy — the rename exists only in the View's query. This enables inserting data from sources with different column naming conventions into a shared target table.

```python
# Rename camelCase columns to snake_case for a consolidated table
kev_view = kev.rename({
    "cveID": "cve_id",
    "vendorProject": "vendor",
    "vulnerabilityName": "vulnerability_name",
}).with_columns({
    "source": Computed("String", "'kev'"),
})
await consolidated.insert(kev_view)
```

**Key behaviors**:

- Synchronous — creates a View, no database call needed. No `await`.
- Chainable with `with_columns()`, `where()`, `select()`, and other View operations.
- `aai_id` cannot be renamed.
- New names must not collide with non-renamed column names.
- `insert()` skips extra source columns not present in the target — no need to `select()` away unwanted columns.

**Tests**: `aaiclick/data/test_rename.py`

# Test Files

| Operator Group                  | Test File                        |
|---------------------------------|----------------------------------|
| Arithmetic, Comparison, Bitwise | `test_operators_parametrized.py` |
| Scalar Broadcast                | `test_scalar_broadcast.py`       |
| Aggregation                     | `test_aggregation.py`            |
| Set Operators                   | `test_unique_parametrized.py`    |
| URL Loading                     | `test_url.py`                    |
| String/Regex Operators          | `test_regex_operators.py`        |
| Insert (+ View flavors)         | `test_insert_parametrized.py`    |
| Concat (+ View flavors)         | `test_concat_parametrized.py`    |
| Rename + tolerant insert        | `test_rename.py`                 |
| DateTime Support                | `test_datetime.py`               |

# Operation Provenance (Oplog)

All Object operations (`create_from_value`, arithmetic, `concat`, `insert`, `copy`, etc.) are automatically instrumented to record provenance when `data_context(oplog=True)` is active. See `docs/oplog.md` for the full specification.

**Implementation**: `aaiclick/oplog/collector.py` — see `OplogCollector.record()`, `record_table()`
