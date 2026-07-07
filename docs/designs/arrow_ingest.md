Arrow-Based Ingest Schema Evaluation
---

Replaces sample-based schema inference in `create_object_from_value` with
pyarrow whole-dataset inference. Today the nested path infers the schema
from the first record/item (first-item-wins: extra keys in later items are
silently dropped, non-dict items fail confusingly) and walks every record
in Python. Arrow scans all records in C++, so schema evaluation loses its
sampling bias and ingest loses its per-item Python passes.

Builds on `docs/designs/nested_dict_flattening.md` — column naming
(`x.y.z` / `x.*.z`) and the `data()` read side are unchanged.

# Dispatch

Only dict-shaped ingest changes; scalar and list-of-scalars paths keep
their current code.

| Input                                    | Arrow entry                | Rows |
|------------------------------------------|----------------------------|------|
| dict, all values lists, no nested dicts  | `pa.table(mapping)`        | N    |
| dict, any other shape                    | `pa.Table.from_pylist([d])`| 1    |
| list of dicts                            | `pa.Table.from_pylist(l)`  | N    |

The dict-of-arrays contract is preserved: mixed list/scalar values in a
flat dict still raise `ValueError`. `aai_id`, `fields=`, and `order_by`
apply to the inferred columns exactly as today.

# Architecture

New module `aaiclick/data/data_context/arrow_ingest.py`:

- `records_to_table(records)` — wraps `pa.Table.from_pylist`; converts
  `ArrowInvalid`/`ArrowTypeError` into `ValueError` with the offending
  context (covers non-dict items in list-of-dicts and cross-record type
  conflicts).
- `arrow_schema_to_columns(schema)` — recursive type-tree walk producing
  `dict[str, ColumnInfo]`: `struct` field → `prefix.name` (no Array
  level), `list<struct>` → `prefix.*.name` (adds one Array level),
  `list<T>` leaf → adds one Array level. Raises `ValueError` on field
  names containing `.` and on empty structs. Validation is a schema walk
  — zero per-item work.
- Strictness check: any leaf with `null_count > 0` raises `ValueError`
  naming the field. This replaces the identical-keys comparison and fixes
  the silent-drop gap: a key present in only some records/items surfaces
  as nulls in the unified arrow column and is rejected.
- `table_to_columns(table)` — struct flattening and list-offset
  regrouping via arrow ops, then one `.to_pylist()` per leaf column into
  the existing `ChClient.insert(column_oriented=True)`. The `ChClient`
  protocol is unchanged; both backends (chdb, clickhouse-connect) work
  as-is.

# Type mapping (parity with current inference)

| Arrow type            | ClickHouse type       | Note                       |
|-----------------------|-----------------------|----------------------------|
| int8..int64/uint      | `Int64`               | Python ints infer int64    |
| float16/32/64         | `Float64`             |                            |
| bool                  | `Bool`                |                            |
| string/large_string   | `String`              | LowCardinality by default  |
| timestamp[any]        | `DateTime64(3, 'UTC')`|                            |
| null (all-None/empty) | `String`              | Same fallback as today     |

# Behavior contract

Every existing test passes unchanged: same column names, types, and
`ValueError` conditions (dotted keys, empty dicts, mismatched key sets,
mixed dict-of-arrays). Newly fixed behavior, tested:

- `[{"c": 1}, {"c": 2, "d": 3}]` raises (was: `d` silently dropped).
- `{"b": [{"c": 1}, 5]}` raises a clear `ValueError` (was:
  `AttributeError` deep in flattening).
- A field that is int in one record and str in another raises.

Ingest-side helpers removed once the arrow path lands:
`_flatten_nested_schema`, `_flatten_nested_record`,
`_validate_nested_keys`, `_has_nested_dicts`, and the per-record loops in
the flat dict/records branches. `_infer_clickhouse_type` stays (scalar and
list paths). The read side (`_unflatten_record`, `_undot_record`) is
untouched.

# Out of scope

Native arrow insert (`insert_arrow` in the `ChClient` protocol,
zero-copy into clickhouse-connect / chdb) — tracked in
`docs/designs/future.md` as the follow-up optimization; this design keeps
the list-based insert transport.
