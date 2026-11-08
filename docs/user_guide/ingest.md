Ingest
---

Loading data into Objects, combining them, and the row-order rule that
cross-table operators depend on. Operators, views and aggregations are in
[Object API](object.md).

# concat()

Unions any number of sources (`obj.concat(a, b, c)`, or standalone `concat(a, b, ...)`) into a new Object with one `UNION ALL` INSERT. Self must be array; args can be Objects, scalars, or lists.

- **Union**: row order is undefined (see [Row Order](#row-order)); source `aai_id` values are copied, so reads of an `aai_id` result follow creation order, not argument order, and `concat(obj, obj)` duplicates ids
- **Nullable promotion**: if any source has nullable columns, the result column is promoted to `Nullable`
- **Compatible types**: all sources must have matching column names and compatible ClickHouse types

# insert()

Inserts any number of sources (`obj.insert(a, b, c)`) into an existing Object, one `INSERT INTO ... SELECT CAST(...)` per source. Target must be array; sources can be Objects, scalars, or lists.

- **Union**: same row-order and `aai_id` rules as `concat()`
- **Subset columns**: sources don't need all target columns; missing ones get ClickHouse defaults
- **View support**: sources can be Views with `where()`, `with_columns()`, field selection, etc.
- **Type casting**: source column types are `CAST` to target column types

# insert_from_url()

Insert data from a URL into an existing Object. For `create_object_from_url()`, see [ObjectContext](object_context.md).

## Supported URL input formats

Both functions accept any format ClickHouse's `url()` table function reads.
Pass it explicitly with `format=...`; the file extension is not consulted.
Headerless formats expose columns as `c1`, `c2`, ….

| Format                      | Notes                                                      |
|-----------------------------|------------------------------------------------------------|
| `Parquet`                   | columnar, type-rich (default)                              |
| `ORC`                       | Apache ORC columnar                                        |
| `Arrow`                     | reading not supported — use Parquet/ORC                    |
| `Avro`                      | container file with embedded schema                        |
| `CSV`                       | no header — columns must be `c1`, `c2`, …                  |
| `CSVWithNames`              | header row provides column names                           |
| `CSVWithNamesAndTypes`      | header + types row — DESCRIBE returns full types           |
| `TSV`                       | no header — `c1`, `c2`, …                                  |
| `TSVWithNames`              | header row                                                 |
| `TSVWithNamesAndTypes`      | header + types row                                         |
| `JSON`                      | full ClickHouse JSON envelope (`{meta, data, rows}`)       |
| `JSONEachRow`               | newline-delimited JSON objects                             |
| `JSONCompactEachRow`        | newline-delimited JSON arrays — `c1`, `c2`, …              |
| `RawBLOB` / `JSONAsString`  | JSON-blob mode for nested API responses (see below)        |

```python
# Tabular formats
await create_object_from_url(
    "https://example.com/data.parquet",
    columns=["id", "price", "name"],
    format="Parquet",
)

# Headerless CSV — bind to positional column names
await create_object_from_url(
    "https://example.com/raw.csv",
    columns=["c1", "c2", "c3"],
    format="CSV",
)

# Avro container with embedded schema
await create_object_from_url(
    "https://example.com/events.avro",
    columns=["id", "ts", "payload"],
    format="Avro",
)
```

## JSON-blob mode

For JSON APIs that wrap rows in an envelope (e.g.
`{"vulnerabilities": [...]}`), pass `RawBLOB` or `JSONAsString` plus
`json_path` and `json_columns`. ClickHouse loads the document as one string
and applies `JSONExtract` per field. Dots walk nesting in both parameters — `json_path="result.vulnerabilities"` reaches an array
inside an envelope, `"cve.id"` a nested field of each element (keys
literally containing `.` are not addressable). Dotted keys become
dot-notation columns that `data()` re-nests — see
[Nested Data Flattening](#nested-data-flattening):

```python
await create_object_from_url(
    "https://services.nvd.nist.gov/rest/json/cves/2.0",
    format="JSONAsString",
    json_path="vulnerabilities",
    json_columns={
        "cve.id":               ColumnInfo("String"),
        "cve.published":        ColumnInfo("DateTime64(3)"),
        "cve.descriptions":     ColumnInfo("String", array=True),
    },
)
```

## Type inference

The loader discovers column types with `DESCRIBE (SELECT … FROM url(…) LIMIT 0)`.
Pass `column_types=` to skip that round-trip — useful for headerless CSV/TSV,
where ClickHouse may fail to infer numeric types from zero rows.

# Row Order

Array Objects have no implicit row order: reads return ClickHouse's natural source order, stable within a process but not across runs. For deterministic order, name the sort column:

```python
obj = await create_object_from_value([3, 1, 2])

await obj.data()                      # → [3, 1, 2] or [1, 2, 3] etc. (undefined)
await obj.data(order_by="value")      # → [1, 2, 3]
await obj.view(order_by="value").data()  # equivalent
```

!!! warning "Cross-table array ops need `order_by`"
    `a + b` between two array Objects from **different tables** raises `TypeError` unless both sides are wrapped with `.view(order_by=...)` — the join uses it as the row-pairing key.

```python
a = await create_object_from_value([1, 2, 3])
b = await create_object_from_value([10, 20, 30])

# a + b   ← TypeError: cross-table op without explicit row order
result = await (a.view(order_by="value") + b.view(order_by="value"))
print(await result.data(order_by="value"))  # → [11, 22, 33]
```

Ops over the same rows (`a + a`, `v['x'] + v['y']`), scalar broadcast (`a + 10`), and aggregations align naturally and skip the contract. Fields of two different Views of one table (e.g. a `with_columns()` View and its source) still need it.

# Nested Data Flattening

Nested dicts and lists-of-dicts flatten to standalone columns — better
compression, skipping indexes, and `LowCardinality` than ClickHouse
`Map`/`Tuple`/`JSON`. pyarrow infers the schema from all records in C++
(no sampling, no per-item Python work); `data()` reconstructs the nesting
by name-parsing the column names.

| Notation | Input shape                | Cardinality | Type effect                |
|----------|----------------------------|-------------|----------------------------|
| `.`      | dict value (nested object) | 1:1         | none — extends name only   |
| `.*.`    | list-of-dicts value        | 1:N         | adds one `Array()` wrapper |

Stars stack: each extra list level around a dict adds another `*` segment
and another `Array()` wrapper.

```python
obj = await create_object_from_value({"x": {"y": {"z": 1}}})
# → column x.y.z (Int64)
await obj.data()            # {"x": {"y": {"z": 1}}}

obj = await create_object_from_value({"b": [{"c": [1, 2], "d": 5}]})
# → columns b.*.c (Array(Array(Int64))), b.*.d (Array(Int64))

obj = await create_object_from_value({"a": [[{"x": 1}], [{"x": 2}]]})
# → column a.*.*.x (Array(Array(Int64)))
```

Ingest raises `ValueError` for anything that cannot round-trip: mismatched
keys across records or items, non-dict items in a list of dicts, type
conflicts, dotted keys, and empty dicts. Columns marked `FieldSpec(nullable=True)` are
exempt at the leaf level — missing or `None` values ingest as NULL and
read back as `None`; a `None` dict or list item still raises. All-`None`
values infer as `Nullable(String)` and round-trip as `None`. Name-parsing applies to any
dict-shaped read — explicit `Schema` columns, imports, and Views with
dotted column names reconstruct the same way; a plain column colliding
with a dotted prefix (`x` next to `x.y`) raises rather than dropping data.
# join()

Joins two Objects on one or more key columns into a new dict Object.

```python
async def join(
    self,
    other: Object,
    *,
    on: str | list[str] | None = None,
    left_on: str | list[str] | None = None,
    right_on: str | list[str] | None = None,
    how: Literal["inner", "left", "right", "full", "cross"] = "inner",
    suffixes: tuple[str, str] | None = None,
) -> Object
```

| Argument   | Purpose                                                                          |
|------------|----------------------------------------------------------------------------------|
| `other`    | Right-hand Object.                                                               |
| `on`       | Key column name(s) present under the same name in both Objects.                  |
| `left_on`  | Left-side key(s) when names differ. Requires matching-length `right_on`.         |
| `right_on` | Right-side key(s) when names differ.                                             |
| `how`      | Join type. Default `"inner"`.                                                    |
| `suffixes` | Suffixes for non-key column collisions — see [Column semantics](#column-semantics). |

Exactly one of `{on}` or `{left_on, right_on}` must be set, except `how="cross"` which forbids all three.

## Join types

| `how`    | ClickHouse keyword | Left cols nullable? | Right cols nullable? |
|----------|--------------------|---------------------|----------------------|
| `inner`  | `INNER JOIN`       | no                  | no                   |
| `left`   | `LEFT JOIN`        | no                  | yes (promoted)       |
| `right`  | `RIGHT JOIN`       | yes (promoted)      | no                   |
| `full`   | `FULL OUTER JOIN`  | yes (promoted)      | yes (promoted)       |
| `cross`  | `CROSS JOIN`       | no                  | no                   |

Outer-join SQL is emitted with `SETTINGS join_use_nulls = 1` so misses materialize as `NULL` instead of the outer-side type's default.

## Column semantics

- **Key dedup**: under `on=` the key appears once under its original name; under `left_on`/`right_on` both key columns survive. `FULL` joins with `on=` coalesce the merged key across sides.
- **Collisions**: non-key columns on both sides raise `ValueError` when `suffixes` is `None` / `False` (default). `suffixes=True` means `("_l", "_r")`; a tuple like `("_x", "_y")` picks custom ones. Both sides get a suffix — empty suffixes are rejected.
- **Key types**: compared via the same type-compatibility rule as `concat` (`String` ↔ `FixedString` OK, `String` ↔ `Int64` not).
- **LowCardinality / Array**: preserved; `LowCardinality(String)` on the outer side becomes `LowCardinality(Nullable(String))`. Join keys themselves must be scalar.
- **Order**: none intrinsic — pass `data(order_by=...)` or `.view(order_by=...)`.

## Examples

```python
# Inner join on a shared key
users  = await create_object_from_value({"id":  [1, 2, 3], "name": ["A", "B", "C"]})
orders = await create_object_from_value({"id":  [1, 1, 4], "total": [9.5, 14.0, 2.0]})

joined = await users.join(orders, on="id")
await joined.data(orient="records")
# → [{"id": 1, "name": "A", "total": 9.5}, {"id": 1, "name": "A", "total": 14.0}]

# Left join — unmatched rows keep left, right columns become NULL
enriched = await users.join(orders, on="id", how="left")
# → rows for id=2, 3 have total=None; schema marks 'total' Nullable(Float64)

# left_on / right_on when key names differ — both keys survive
orders2 = await create_object_from_value({"user_id": [1, 2], "total": [9.5, 3.0]})
await users.join(orders2, left_on="id", right_on="user_id")
# → rows include both 'id' and 'user_id'

# Suffixes on non-key collision — True picks the default ("_l", "_r")
a = await create_object_from_value({"id": [1, 2], "score": [10, 20]})
b = await create_object_from_value({"id": [1, 2], "score": [99, 88]})
merged = await a.join(b, on="id", suffixes=True)
# → columns: id, score_l, score_r

# Or supply a custom tuple:
merged = await a.join(b, on="id", suffixes=("_old", "_new"))

# Cross join — Cartesian product
colors = await create_object_from_value({"c": ["red", "blue"]})
sizes  = await create_object_from_value({"s": ["S", "M"]})
skus   = await colors.join(sizes, how="cross")  # 4 rows
```

## Distributed considerations

The default chdb backend is single-shard, so a plain `JOIN` suffices. A future sharded backend would use `GLOBAL JOIN` and `join_algorithm` hints (`parallel_hash`, `partial_merge`, `grace_hash`); the API leaves room for a later `strategy=` kwarg.

Semi / anti / asof joins are not supported yet — each needs its own semantics (left-only output schema, required `order_by`, tolerance).
