Nested Dict Flattening (`x.y.z` Columns)
---

Singleton nested dicts flatten to plain-dot `x.y.z` columns, complementing
the existing `x.*.z` (array-of-objects) notation. Standalone columns beat
ClickHouse `Map`/`Tuple`/`JSON` on compression, skipping indexes, and
`LowCardinality`.

# Semantics

Two name-notation rules, composable at any depth:

| Notation | Input shape                | Cardinality | Type effect                |
|----------|----------------------------|-------------|----------------------------|
| `.`      | dict value (nested object) | 1:1         | none — extends name only   |
| `.*.`    | list-of-dicts value        | 1:N         | adds one `Array()` wrapper |

| Input                      | Columns                |
|----------------------------|------------------------|
| `{"x": {"y": {"z": 1}}}`   | `x.y.z Int64`          |
| `{"x": {"y": [{"z": 1}]}}` | `x.y.*.z Array(Int64)` |
| `{"b": [{"c": {"d": 5}}]}` | `b.*.c.d Array(Int64)` |

`data()` reconstructs nesting by name-parsing alone — split on `.*.`
first, then group remaining dotted names by first segment and recurse.
No schema metadata.

# Code changes

**Implementation**: `aaiclick/data/data_context/data_context.py` — see `_flatten_nested_schema()`, `_flatten_nested_record()`, `_validate_nested_keys()`; `aaiclick/data/object/data_extraction.py` — see `_undot_record()`, `_unflatten_record()`. Tests: `aaiclick/data/object/test_nested_dicts.py`.

In `aaiclick/data/data_context/data_context.py`:

- `_has_nested_dicts` — also `True` for plain dict values.
- `_flatten_nested_schema` — dict branch recurses with prefix
  `{col_name}.`, `array_depth` unchanged.
- `_flatten_nested_record` — matching dict branch.

In `aaiclick/data/object/data_extraction.py`:

- `_unflatten_record` — new `_undot_record` helper runs after the `.*.`
  pass, grouping keys on the first `.` and recursing; star-group prefixes
  (`x.y` from `x.y.*.z`) flow through it.
- `_has_nested_columns` — triggers on any `.`, not just `.*.`.

# Validation

- **Dotted input keys raise.** `create_object_from_value` recursively
  rejects keys containing `.` at every dict level — including flat dicts
  like `{"a.b": 1}`, since `data()` would reshape them.
- **Empty dict values raise.** `{"x": {}}` has no representable columns.
  Empty lists keep the existing non-empty-sample search.
- **Records lists.** The identical-keys rule extends recursively via flat
  key-set comparison — mismatched nested keys raise `ValueError` instead
  of failing cryptically at insert.

!!! warning "Any dict-shaped read reconstructs dotted column names"
    Reconstruction is name-parsed with no schema metadata, so it applies to
    every dict-table read path, not just `create_object_from_value` output:
    explicit `Schema` columns (unrestricted, so `a.b` comes back as
    `{"a": {"b": ...}}`), URL/format imports whose source columns contain
    dots, and Views that select or rename columns into a dotted name.

# Testing

New file `aaiclick/data/object/test_nested_dicts.py`:

- Round-trip: single record, records list, both orients.
- Deep plain nesting (`x.y.z`) and mixed notations (`x.y.*.z`, `b.*.c.d`).
- Schema assertions: column names, leaf types, array depth.
- `ValueError`: dotted keys, empty dicts, mismatched nested key sets.

# Out of scope

Query-side access to nested fields (operators, views, aggregations) —
columns are physically ordinary, so name-based access may work, but is
neither extended nor tested here.
