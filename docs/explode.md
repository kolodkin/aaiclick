# Explode (Array Join) Specification

Explode Array column(s) into individual rows for aggregation.

## Problem

Dict Objects can have `Array(T)` columns (e.g., `tags: Array(String)`, `scores: Array(Int64)`).
Aggregation operators (`unique`, `max`, `sum`, `group_by`, etc.) work on **rows**, not on elements
inside array columns. To aggregate over array elements, we need an **explode** step that flattens
array columns into individual rows first.

**Example:**

```python
obj = await create_object_from_value([
    {"user": "Alice", "tags": ["python", "rust"]},
    {"user": "Bob",   "tags": ["python", "go"]},
])

# Currently impossible: get unique tags or count by tag
# After explode: each tag becomes its own row, enabling standard aggregation
```

## ClickHouse Primitives

| Mechanism              | Syntax                                    | Notes                                                        |
|------------------------|-------------------------------------------|--------------------------------------------------------------|
| `ARRAY JOIN` clause    | `SELECT ... FROM t ARRAY JOIN col`        | Table-level clause, expands Array columns into rows          |
| `LEFT ARRAY JOIN`      | `SELECT ... FROM t LEFT ARRAY JOIN col`   | Same but preserves rows with empty arrays (emits NULL)       |
| `arrayJoin(expr)` func | `SELECT arrayJoin(col) FROM t`            | Inline function, same effect, usable in any expression       |

Both produce the same result -- each array element becomes its own row, with scalar columns duplicated:

```sql
-- Input row:  user='Alice', tags=['python','rust']
-- After ARRAY JOIN tags:
--   user='Alice', tags='python'
--   user='Alice', tags='rust'
```

**Multiple columns**: `ARRAY JOIN col1, col2` zips arrays (like Python `zip`).
Shorter arrays pad with type defaults. This is NOT a Cartesian product.

**Reference**: https://clickhouse.com/docs/sql-reference/statements/select/array-join

## Approach: Hybrid View (Option C)

`explode()` returns a **View** (lazy subquery), not a materialized table.
Downstream operators fuse into a single SQL query. Materialization available via `View.copy()`.

**Why this fits the architecture:**

- `View` class already provides lazy subquery wrapping
- All operators accept `QueryInfo.source` as table name or `(subquery)`
- No changes needed to aggregation operators -- they work on the exploded view transparently
- Materialization available via `View.copy()` when needed

## Interface

### Method Signature

```python
class Object:
    def explode(self, *columns: str, left: bool = False) -> "View":
        """
        Explode Array column(s) into individual rows.

        Returns a View (lazy, no materialization). Each element in the
        specified Array column(s) becomes its own row. Scalar columns
        are duplicated per element.

        Args:
            *columns: Array column name(s) to explode.
            left: If True, use LEFT ARRAY JOIN (preserve rows with empty
                  arrays, emitting NULL for the exploded column).

        Returns:
            View with exploded rows.

        Raises:
            ValueError: If column doesn't exist.
            ValueError: If column is not an Array type.
            ValueError: If Object is not a dict type.
        """
```

### Usage Patterns

```python
obj = await create_object_from_value([
    {"user": "Alice", "tags": ["python", "rust"], "scores": [90, 85]},
    {"user": "Bob",   "tags": ["python", "go"],   "scores": [70, 95]},
])

# 1. Explode + unique
flat = obj.explode("tags")
unique_tags = await flat['tags'].unique()
await unique_tags.data()  # ["go", "python", "rust"]

# 2. Explode + group_by + count
tag_counts = await flat.group_by('tags').count()
await tag_counts.data()  # {"tags": ["go", "python", "rust"], "_count": [1, 2, 1]}

# 3. Explode + scalar aggregation
max_score = await obj.explode("scores")['scores'].max()
await max_score.data()  # 95

# 4. Explode multiple columns (zip, not cartesian)
flat2 = obj.explode("tags", "scores")
await flat2.data()
# {"user": ["Alice", "Alice", "Bob", "Bob"],
#  "tags": ["python", "rust", "python", "go"],
#  "scores": [90, 85, 70, 95]}

# 5. LEFT explode (preserve empty arrays)
obj2 = await create_object_from_value([
    {"user": "Alice", "tags": ["python"]},
    {"user": "Bob",   "tags": []},
])
flat3 = obj2.explode("tags", left=True)
await flat3.data()
# {"user": ["Alice", "Bob"], "tags": ["python", None]}

# 6. Materialize if needed
materialized = await flat.copy()  # Creates real table with new Snowflake IDs
```

### Generated SQL

**Single column explode:**

```sql
-- View source (subquery, not materialized):
(SELECT user, tags FROM {source} ARRAY JOIN tags)

-- With LEFT:
(SELECT user, tags FROM {source} LEFT ARRAY JOIN tags)
```

**Chained with unique:**

```sql
-- flat['tags'].unique() fuses into:
INSERT INTO {result} (value)
SELECT value FROM (
    SELECT tags AS value FROM {source} ARRAY JOIN tags
) GROUP BY value
```

**Chained with group_by + count:**

```sql
-- flat.group_by('tags').count() fuses into:
INSERT INTO {result} (tags, _count)
SELECT tags, count() AS _count FROM (
    SELECT user, tags FROM {source} ARRAY JOIN tags
) GROUP BY tags
```

## Implementation Details

### Schema Change After Explode

The exploded column changes type in the View's schema:

| Before explode                | After explode             |
|-------------------------------|---------------------------|
| `tags: Array(String)` (array) | `tags: String` (scalar)   |
| `scores: Array(Int64)` (array)| `scores: Int64` (scalar)  |
| `user: String` (scalar)       | `user: String` (unchanged)|

The `ColumnInfo.array` flag becomes `False` for exploded columns.
Non-exploded columns keep their original type.

### View Class Extension

The `View` class needs a new attribute to track exploded columns:

```python
class View:
    def __init__(self, source, ..., exploded_columns=None, left_explode=False):
        self._exploded_columns = exploded_columns or []
        self._left_explode = left_explode
```

The View's source subquery incorporates the `ARRAY JOIN` clause.
The View's cached schema reflects the post-explode column types.

### Snowflake ID Handling

- Exploded rows share the parent row's `aai_id`
- This is correct for View (read-only, no lifecycle)
- If materialized via `copy()`, new Snowflake IDs are generated
- ClickHouse preserves array element order within each exploded group

### Validation

- `explode()` only valid on dict Objects (`fieldtype == 'd'`)
- Each specified column must exist and have `ColumnInfo.array == True`
- At least one column must be specified

### Empty Arrays

| Mode                  | Row with `tags=[]`    |
|-----------------------|-----------------------|
| `ARRAY JOIN` (default)| Row is **dropped**    |
| `LEFT ARRAY JOIN`     | Row kept, `tags=NULL` |

The `left=True` parameter controls this behavior.
