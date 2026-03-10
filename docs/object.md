# Object Class Documentation

## Overview

**Implementation**: `aaiclick/data/object.py` — see `Object` and `View` classes

The `Object` class represents data stored in ClickHouse tables. Each Object instance corresponds to a ClickHouse table and supports operations through operator overloading that create new tables with results.

**Key Features:**
- Operator overloading for arithmetic, comparison, and bitwise operations
- Immutable operations (all operations return new Objects)
- Automatic table naming with Snowflake IDs
- Support for scalars, arrays, and dictionaries
- Element-wise operations on arrays
- Automatic lifecycle management and staleness detection

## Object Lifecycle and Staleness

Objects are managed by a `data_context()` and become **stale** when the context exits. All async methods call `self.checkstale()` — using a stale Object raises `RuntimeError`.

**Implementation**: `aaiclick/data/object.py` — see `checkstale()`, `stale` property, `_register()`

**Rules**: Create and use Objects within the same `data_context()`. Don't store Objects for use after context exit. Don't pass Objects between contexts.

## Table Schema and Structure

Each Object gets a dedicated ClickHouse table named `t{snowflake_id}`. All tables include an `aai_id` column (Snowflake ID) for ordering — ClickHouse doesn't guarantee insertion order in SELECT.

**Implementation**: `aaiclick/data/data_context.py` — see `create_object()` and `create_object_from_value()`

### Schema Patterns

| Data Type       | Columns                              | Rows     |
|-----------------|--------------------------------------|----------|
| Scalar          | `aai_id UInt64`, `value {type}`      | 1        |
| Array/List      | `aai_id UInt64`, `value {type}`      | N        |
| Dict of Scalars | `aai_id UInt64`, `col1`, `col2`, ... | 1        |
| Dict of Arrays  | `aai_id UInt64`, `col1`, `col2`, ... | N        |

Column names are backtick-quoted via `quote_identifier()` in `aaiclick/data/sql_utils.py`.

### Snowflake ID Structure (64 bits)

| Bits  | Field      | Range                         |
|-------|------------|-------------------------------|
| 63    | Sign       | Always 0                      |
| 62-22 | Timestamp  | 41 bits, ~69 years            |
| 21-12 | Machine ID | 10 bits, up to 1024 machines  |
| 11-0  | Sequence   | 12 bits, up to 4096 IDs/ms    |

**Implementation**: `aaiclick/snowflake_id.py`

## Operator Support

**Implementation**: `aaiclick/data/object.py` (dunder methods) delegates to `aaiclick/data/operators.py` (async functions).

All operators work element-wise on both scalar and array data. Each operator creates a new Object table with the result via `_apply_operator()` using SQL templates (`apply_op_scalar.sql`, `apply_op_array.sql`).

See [operators.md](operators.md) for complete function reference. For runnable examples, see `examples/basic_operators.py`.

### Scalar Broadcast

**Implementation**: `aaiclick/data/object.py` — see `_ensure_object()`

All binary operators accept Python scalars (`int`, `float`, `bool`, `str`) on either side. The scalar is converted to a scalar Object via `create_object_from_value`, so all data stays in ClickHouse with a unified code path. This works for both `obj + 5` (forward) and `5 + obj` (reverse).

**How it works**: `_ensure_object()` converts scalars to Objects first, then the caller uses `_get_query_info()` for all operands — no special-case SQL generation.

Reverse operators (`__radd__`, `__rsub__`, etc.) call `_apply_reverse_operator()` which swaps the operand order so the scalar appears on the left in SQL.

```python
# Forward: obj is left operand
result = await (obj * 2)      # SQL: ... obj.value * 2
result = await (obj + 100)    # SQL: ... obj.value + 100

# Reverse: obj is right operand
result = await (10 - obj)     # SQL: ... 10 - obj.value
result = await (2 ** obj)     # SQL: ... power(2, obj.value)
```

### Arithmetic Operators

| Python Operator | Description    | ClickHouse Equivalent | Forward Method  | Reverse Method    |
|-----------------|----------------|-----------------------|-----------------|-------------------|
| `+`             | Addition       | `+`                   | `__add__`       | `__radd__`        |
| `-`             | Subtraction    | `-`                   | `__sub__`       | `__rsub__`        |
| `*`             | Multiplication | `*`                   | `__mul__`       | `__rmul__`        |
| `/`             | Division       | `/`                   | `__truediv__`   | `__rtruediv__`    |
| `//`            | Floor Division | `intDiv()`            | `__floordiv__`  | `__rfloordiv__`   |
| `%`             | Modulo         | `%`                   | `__mod__`       | `__rmod__`        |
| `**`            | Power          | `power()`             | `__pow__`       | `__rpow__`        |

### Comparison Operators

| Python Operator | Description           | ClickHouse Equivalent | Python Method |
|-----------------|-----------------------|-----------------------|---------------|
| `==`            | Equal                 | `=`                   | `__eq__`      |
| `!=`            | Not Equal             | `!=`                  | `__ne__`      |
| `<`             | Less Than             | `<`                   | `__lt__`      |
| `<=`            | Less Than or Equal    | `<=`                  | `__le__`      |
| `>`             | Greater Than          | `>`                   | `__gt__`      |
| `>=`            | Greater Than or Equal | `>=`                  | `__ge__`      |

Comparison operators don't need explicit reverse methods — Python swaps `<`/`>` and `<=`/`>=` automatically (e.g., `5 < obj` becomes `obj > 5`).

### Bitwise Operators

| Python Operator | Description | ClickHouse Equivalent | Forward Method | Reverse Method |
|-----------------|-------------|-----------------------|----------------|----------------|
| `&`             | Bitwise AND | `bitAnd()`            | `__and__`      | `__rand__`     |
| `\|`            | Bitwise OR  | `bitOr()`             | `__or__`       | `__ror__`      |
| `^`             | Bitwise XOR | `bitXor()`            | `__xor__`      | `__rxor__`     |

### Aggregation Operators

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

### Set Operators

| Method      | ClickHouse Implementation | Notes                          |
|-------------|---------------------------|--------------------------------|
| `.unique()` | `GROUP BY`                | Order not guaranteed           |

### String/Regex Operators

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

### Group By Operations

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

### Memory/Disk Settings

For large datasets, ClickHouse can spill to disk via `max_bytes_before_external_sort`, `max_bytes_in_join`, `join_algorithm`.

## Loading Data from URLs

**Implementation**: `aaiclick/data/url.py` — see `create_object_from_url()`

Loads data from HTTP URLs directly into ClickHouse using the `url()` table function — **zero Python memory footprint**.

**Parameters**: `url` (required), `columns` (required), `format` (default `"Parquet"`), `where`, `limit`.

**Supported formats**: Parquet, CSV, CSVWithNames, CSVWithNamesAndTypes, TSV, TSVWithNames, TSVWithNamesAndTypes, JSON, JSONEachRow, JSONCompactEachRow, ORC, Avro.

**Schema behavior**: Single column renamed to `value`; multiple columns preserve names; types inferred by ClickHouse.

**Snowflake IDs**: Generated as `base_id + row_number() OVER ()` since data never enters Python.

**Validation**: HTTP(S) only, `aai_id` reserved, no `;` in WHERE, format must be in supported list.

### insert_from_url() ✅ IMPLEMENTED

**Implementation**: `aaiclick/data/object.py` — see `Object.insert_from_url()`

Insert data from a URL into an existing Object. Schema created once, multiple workers can insert.

## The concat() Method

**Implementation**: `aaiclick/data/object.py` — see `Object.concat()`, `aaiclick/data/ingest.py` — see `concat_objects_db()`

Concatenates multiple sources into a new Object via a single `UNION ALL`. Self must be array; args can be Objects (array or scalar), Python scalars, or lists. Also available as standalone function `concat(obj_a, obj_b, ...)`.

- **Variadic**: `obj.concat(a, b, c)` — any number of sources in one call
- **Nullable promotion**: if any source has nullable columns, the result column is promoted to `Nullable`
- **Compatible types**: all sources must have matching column names and compatible ClickHouse types

## The data() Method

**Implementation**: `aaiclick/data/object.py` — see `Object.data()`

Returns values based on data type: scalar → value, array → list, dict → dict or list of dicts.

**Orient parameter** (for dict Objects with multiple rows):

| Constant         | Value       | Description                                  |
|------------------|-------------|----------------------------------------------|
| `ORIENT_DICT`    | `'dict'`    | Dict with arrays as values (default)         |
| `ORIENT_RECORDS` | `'records'` | List of dicts (one per row)                  |

## Column Metadata

**Implementation**: `aaiclick/data/object.py` — see `FIELDTYPE_SCALAR`, `FIELDTYPE_ARRAY`, `FIELDTYPE_DICT`

Each column gets a YAML comment with fieldtype: `'s'` (scalar), `'a'` (array), `'d'` (dict).

## Views

**Implementation**: `aaiclick/data/object.py` — see `View` class

Read-only filtered view of an Object — references the same table, no data copy.

Created via `obj.view(where=..., limit=..., offset=..., order_by=...)`. Supports all read operations (`.data()`, operators, aggregations). Cannot `insert()`.

For runnable examples, see `examples/view_examples.py`.

### Chained WHERE Clauses

**Implementation**: `aaiclick/data/object.py` — see `Object.where()`, `View.where()`, `View.or_where()`

Fluent API for building WHERE conditions. `Object.where()` creates a View; `View.where()` and `View.or_where()` chain additional conditions. Each call returns a **new** View (immutable).

- `obj.where(cond)` — creates View with initial WHERE condition
- `view.where(cond)` — AND-chains: `.where('x > 10').where('y < 20')` → `WHERE (x > 10) AND (y < 20)`
- `view.or_where(cond)` — OR-chains: `.where('x > 100').or_where('y < 5')` → `WHERE (x > 100) OR (y < 5)`

**Note**: `or_where()` requires a prior `where()` — raises `ValueError` otherwise.

## Table Lifecycle Tracking

Tables are tracked via reference counting and dropped when no Objects reference them.

```
┌─────────────────────────────────────────────────────────────┐
│                    data_context()                             │
│                                                              │
│  enter  ──► Start LifecycleHandler                           │
│  exit   ──► Stop LifecycleHandler, cleanup                   │
│                                                              │
│  ┌──────────┐    incref()   ┌─────────────────────────────┐  │
│  │  Object  │──────────────►│                             │  │
│  │  __init__│               │    LifecycleHandler (ABC)   │  │
│  └──────────┘               │                             │  │
│                              │  Implementations:           │  │
│  ┌──────────┐    decref()   │  - LocalLifecycleHandler    │  │
│  │  Object  │──────────────►│    (TableWorker thread)     │  │
│  │  __del__ │               │  - PgLifecycleHandler       │  │
│  └──────────┘               │    (PostgreSQL refcounts)   │  │
│                              └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### LifecycleHandler ABC

**Implementation**: `aaiclick/data/lifecycle.py` — see `LifecycleHandler` class

Abstract interface: `start()`, `stop()`, `incref()`, `decref()`, `pin()` (no-op default), `claim()` (raises NotImplementedError default).

### Local Mode

**Implementation**: `aaiclick/data/lifecycle.py` — see `LocalLifecycleHandler` class

Default handler. Wraps `TableWorker` (background thread in `aaiclick/data/table_worker.py`). Drops tables immediately on refcount 0. `data_context()` creates this when no handler is injected.

### Distributed Mode

**Implementation**: `aaiclick/orchestration/pg_lifecycle.py` — see `PgLifecycleHandler` class

Writes refcounts to PostgreSQL. Implements pin/claim for ownership transfer. Does NOT drop tables — cleanup by `PgCleanupWorker`.

See [Orchestration documentation](orchestration.md) — "Distributed Object Lifecycle" for the full design.

### Object Registration Flow

1. `create_object()` generates table name, calls `obj._register(ctx)` → `incref` (write-ahead, before CREATE TABLE)
2. `CREATE TABLE` in ClickHouse
3. On garbage collection, `Object.__del__` → `decref`
4. Views incref/decref the source Object's table (no new tables)

### __del__ Guard Clauses

| Guard                               | Scenario                                      |
|-------------------------------------|-----------------------------------------------|
| `sys.is_finalizing()`               | Interpreter shutdown — skip for thread safety  |
| `_data_ctx_ref is None`             | Object was never registered                    |
| `_data_ctx_ref()` returns None      | Context already garbage collected              |

## Computed Column Expansion: `with_columns()` ⚠️ NOT YET IMPLEMENTED

### Motivation

`group_by()` only accepts column names, not SQL expressions. When you need to group by a derived value (e.g., `toYear(dateAdded)`, `lower(name)`), the workaround is manual: create an intermediate Object, populate it with `INSERT...SELECT`, then group. `with_columns()` automates this pattern.

See `aaiclick/example_projects/cyber_threat_feeds/__init__.py` — `analyze_kev_by_year()` for the manual workaround in production code.

### Design: SELECT Expression Approach (No Materialization)

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

### API

```python
from aaiclick.data.models import Computed

# On Object — returns a View with existing columns + computed expressions
enriched = kev.with_columns({
    "year": Computed("UInt16", "toYear(dateAdded)"),
    "vendor_lower": Computed("String", "lower(vendorProject)"),
    "days_to_fix": Computed("Int32", "dateDiff('day', dateAdded, dueDate)"),
})

# On View — preserves existing WHERE/LIMIT/OFFSET/ORDER BY + adds computed columns
filtered = kev.where("dateAdded >= '2023-01-01'")
enriched = filtered.with_columns({"year": Computed("UInt16", "toYear(dateAdded)")})

# Result is a View — all View operations work
by_year = await enriched.group_by("year").agg({"cveID": "count"})
by_vendor = await enriched.group_by("vendor_lower").agg({"cveID": "count"})
recent = enriched.where("days_to_fix < 30")
```

**Note**: `with_columns()` is synchronous — it just creates a View, no database call needed. No `await`.

### Computed NamedTuple

**Implementation**: `aaiclick/data/models.py` — see `Computed` class

```python
class Computed(NamedTuple):
    """Computed column definition for with_columns()."""
    type: str        # ClickHouse column type (e.g., "UInt16", "String")
    expression: str  # SQL expression referencing existing columns
```

Named fields make call sites self-documenting and enable IDE autocompletion. Being a `NamedTuple`, it's also a tuple — backward-compatible with `("UInt16", "toYear(dateAdded)")` syntax.

### Signature

```python
def with_columns(
    self,
    columns: dict[str, Computed],
) -> View:
    """Create a View with all existing columns plus computed expressions.

    Returns a View whose SELECT list includes `expr AS name` for each
    computed column. No table is created, no data is copied — the
    expression is evaluated at query time by ClickHouse.

    Works on both Object and View. On Views, preserves existing constraints
    (WHERE, LIMIT, OFFSET, ORDER BY) and adds computed columns to the
    SELECT list.

    Args:
        columns: Mapping of column_name -> Computed(type, expression).
            The expression can reference any existing column by name.
            Expression is passed verbatim to ClickHouse — supports any
            ClickHouse function (toYear, lower, dateDiff, if, multiIf, etc.)

    Returns:
        View with original columns + computed expression aliases.

    Raises:
        ValueError: If computed column name collides with existing column.
        ValueError: If called on a scalar Object (fieldtype 's').
        ValueError: If columns dict is empty.
        RuntimeError: If Object is stale.
    """
```

### Result Schema Rules

The result is always a **View** with dict-like schema (`fieldtype='d'`):

| Source Type           | Behavior                                                          |
|-----------------------|-------------------------------------------------------------------|
| Array (`value` col)   | View selects `value` + computed columns → promotes to dict        |
| Dict (named columns)  | View selects all existing columns + computed columns              |
| Scalar                | **Rejected** — raises `ValueError`                               |
| View (single field)   | View selects source field + computed columns                      |
| View (multi field)    | View selects selected fields + computed columns                   |
| View (with WHERE)     | WHERE preserved, computed columns added to SELECT                 |

### Column Name Collision

Computed column names must not collide with existing column names. This is intentional — `with_columns()` adds new columns, it doesn't replace. To replace an existing column, use `drop_columns()` first (future) or work with the raw SQL pattern.

```python
# Raises ValueError: "year" already exists
enriched.with_columns({"year": Computed("UInt16", "toYear(dueDate)")})
```

### Implementation

**Location**: `aaiclick/data/object.py` — `Object.with_columns()` and `View.with_columns()` methods

The approach differs from the standard operator pattern — no new table, no `INSERT...SELECT`. Instead, it creates a View with computed expressions in the SELECT list.

#### ViewSchema Extension

`ViewSchema` gains a new field to store computed column definitions:

```python
@dataclass
class ViewSchema(Schema):
    where: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    order_by: Optional[str] = None
    selected_fields: Optional[List[str]] = None
    computed_columns: Optional[Dict[str, Computed]] = None  # NEW
```

#### View._build_select() Extension

When `computed_columns` is set, the View's SELECT list includes the expressions:

```python
def _build_select(self, columns: str = "*", default_order_by=None) -> str:
    # ... existing field selection logic ...

    # If computed columns exist, expand * to explicit columns + expressions
    if self._computed_columns:
        # Get existing column names (from schema, excluding aai_id)
        existing = [quote_identifier(k) for k in self._schema.columns if k != "aai_id"]
        computed = [
            f"{c.expression} AS {quote_identifier(name)}"
            for name, c in self._computed_columns.items()
        ]
        select_cols = "aai_id, " + ", ".join(existing + computed)

    query = f"SELECT {select_cols} FROM {self.table}"
    # ... existing WHERE/ORDER BY/LIMIT/OFFSET logic ...
```

#### Object.with_columns()

```python
def with_columns(self, columns: dict[str, Computed]) -> View:
    self.checkstale()

    if not columns:
        raise ValueError("columns must not be empty")
    if self._schema.fieldtype == FIELDTYPE_SCALAR:
        raise ValueError("with_columns() not supported on scalar Objects")

    # Detect collisions with existing columns (excluding aai_id)
    existing_names = {k for k in self._schema.columns if k != "aai_id"}
    collisions = existing_names & set(columns)
    if collisions:
        raise ValueError(
            f"Computed column names collide with existing: {collisions}"
        )

    # Validate expressions
    for name, computed in columns.items():
        _validate_expression(computed.expression)

    # Build extended schema with computed columns included
    result_columns = dict(self._schema.columns)
    for name, computed in columns.items():
        result_columns[name] = parse_ch_type(computed.type)

    schema = ViewSchema(
        fieldtype=FIELDTYPE_DICT,
        columns=result_columns,
        computed_columns=columns,
    )

    # Return a View with computed columns in the SELECT list
    return View(source=self, schema=schema, computed_columns=columns)
```

### View Integration

`with_columns()` on a View preserves all existing constraints and adds computed columns:

```python
# View with WHERE + computed columns
view = kev.where("dateAdded >= '2023-01-01'")
enriched = view.with_columns({"year": Computed("UInt16", "toYear(dateAdded)")})

# Generated SQL (when data() is called):
# SELECT aai_id, vendorProject, cveID, ..., toYear(dateAdded) AS year
# FROM t_kev
# WHERE (dateAdded >= '2023-01-01')
```

For single-field Views (`kev["dateAdded"]`), the source column is selected alongside the computed columns. The result becomes a dict-like View.

### Chaining

`with_columns()` returns a View, so all View operations work:

```python
enriched = kev.with_columns({"year": Computed("UInt16", "toYear(dateAdded)")})

# group_by + agg
by_year = await enriched.group_by("year").agg({"cveID": "count"})

# where filtering (AND-chains with existing constraints)
recent = enriched.where("year >= 2023")

# column selection
years_only = recent["year"]

# further with_columns (additive)
enriched2 = enriched.with_columns({
    "month": Computed("UInt8", "toMonth(dateAdded)"),
})

# operators on selected columns
year_obj = enriched["year"]
shifted = await (year_obj - 2000)
```

### Security: SQL Expression Validation

SQL expressions are passed verbatim to ClickHouse. Basic validation:

1. **Reject semicolons** — prevents statement injection (same as View WHERE clauses)
2. **Reject subqueries** — disallow `SELECT` keyword in expressions
3. **Type validated by ClickHouse** — if the expression result type doesn't match the declared `ch_type`, ClickHouse raises a type error at query time

```python
def _validate_expression(expr: str) -> None:
    """Validate SQL expression for safety."""
    if ";" in expr:
        raise ValueError(f"SQL expression must not contain semicolons: {expr}")
    if re.search(r"\bSELECT\b", expr, re.IGNORECASE):
        raise ValueError(f"SQL expression must not contain subqueries: {expr}")
```

### Test Plan

**Test file**: `aaiclick/data/test_with_columns.py`

| Test                                    | Description                                              |
|-----------------------------------------|----------------------------------------------------------|
| `test_with_columns_basic`              | Add one computed column, verify data                     |
| `test_with_columns_multiple`           | Add multiple computed columns in one call                |
| `test_with_columns_on_view`            | Source is a View with WHERE — filters applied            |
| `test_with_columns_on_view_with_limit` | Source is a View with LIMIT — only N rows               |
| `test_with_columns_single_field_view`  | Source is `obj["col"]` — promotes to dict View           |
| `test_with_columns_group_by`           | Chain: with_columns → group_by → agg                    |
| `test_with_columns_chained`            | Two successive with_columns calls (additive)             |
| `test_with_columns_collision_error`    | Computed name matches existing → ValueError              |
| `test_with_columns_scalar_error`       | Called on scalar Object → ValueError                     |
| `test_with_columns_empty_error`        | Empty columns dict → ValueError                          |
| `test_with_columns_semicolon_error`    | Expression with `;` → ValueError                        |
| `test_with_columns_date_functions`     | toYear, toMonth, toDayOfWeek on Date columns             |
| `test_with_columns_string_functions`   | lower, upper, length on String columns                   |
| `test_with_columns_arithmetic`         | Computed column from arithmetic expression               |
| `test_with_columns_nullable`           | Computed column from nullable source preserves nulls     |
| `test_with_columns_synchronous`        | Verify no await needed — returns View immediately        |

### Use Cases

| Category              | Example Expressions                                       |
|-----------------------|-----------------------------------------------------------|
| Date extraction       | `toYear(col)`, `toMonth(col)`, `toDayOfWeek(col)`        |
| Date arithmetic       | `dateDiff('day', col_a, col_b)`, `addDays(col, 30)`      |
| String normalization  | `lower(col)`, `upper(col)`, `trim(col)`, `length(col)`   |
| Conditional           | `if(col > 0, 'pos', 'neg')`, `multiIf(...)`              |
| Type casting          | `toFloat64(col)`, `toString(col)`, `toDate(col)`         |
| Math                  | `log2(col)`, `sqrt(col)`, `abs(col_a - col_b)`           |
| Hashing/bucketing     | `cityHash64(col) % 100`, `intDiv(col, 10)`               |

## Test Files

| Operator Group                  | Test File                        |
|---------------------------------|----------------------------------|
| Arithmetic, Comparison, Bitwise | `test_operators_parametrized.py` |
| Scalar Broadcast                | `test_scalar_broadcast.py`       |
| Aggregation                     | `test_aggregation.py`            |
| Set Operators                   | `test_unique_parametrized.py`    |
| URL Loading                     | `test_url.py`                    |
| String/Regex Operators          | `test_regex_operators.py`        |
