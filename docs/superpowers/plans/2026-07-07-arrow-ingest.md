# Arrow-Based Ingest Schema Evaluation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace sample-based (first-record-wins) schema inference in `create_object_from_value` with pyarrow whole-dataset inference, eliminating per-item Python passes and the silent-drop gap.

**Architecture:** A new pure module `aaiclick/data/data_context/arrow_ingest.py` wraps `pa.array(records)` (CRITICAL: `pa.array`, NOT `pa.Table.from_pylist` — from_pylist takes top-level keys from the FIRST record only and silently drops later keys; `pa.array` struct inference unifies keys across ALL records, verified in this environment). A type-tree walk produces dot/dot-star `ColumnInfo`s; a vectorized null-count check enforces strict identical-keys semantics; arrow list-rewrap ops (`pa.ListArray.from_arrays(offsets, child)`) flatten leaves with one `to_pylist()` per column into the unchanged `ChClient.insert(column_oriented=True)`.

**Tech Stack:** pyarrow >= 23.0.0 (already a dependency), pytest + pytest-asyncio auto mode, chdb-backed `ctx` fixture from `aaiclick/conftest.py`.

**Spec:** `docs/designs/arrow_ingest.md` (deleted in Task 3 per user instruction — implemented designs are removed, with a short design note left in `docs/user_guide/object.md`).

## Global Constraints

- ALL imports at top of file, three groups (stdlib / external / current package). Never inside functions.
- Tests are flat module-level functions — `async def test_*(ctx):` only when the test needs the DB; pure-function tests are plain `def test_*():`. No classes, no `@pytest.mark.asyncio`.
- `filterwarnings = ["error"]` — any warning fails a test.
- No `Any` shortcuts; no `__all__`; no history comments about removed code.
- Error-message compatibility (existing tests match on these substrings): mismatched keys → message contains `identical keys`; dotted keys → `must not contain`; empty dicts → `Empty dict`.
- Known container facts: 3 pre-existing failures in `aaiclick/data/object/test_order_by.py` (unrelated, fail on main); pre-commit pyright hook fails on bogus `reportMissingImports` (hook venv lacks deps) — commit with `SKIP=pyright`, never blanket `--no-verify`; any ruff/other hook failure must be fixed properly.
- Committer identity `Claude <noreply@anthropic.com>` (already configured). Every commit message ends with:
  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01Qi4jo2QMYBFKFBDzFCPuDd
  ```
- Branch: `claude/clickhouse-json-structure-support-focsq0` (already checked out; push only to this branch).

---

### Task 1: `arrow_ingest` module — inference, schema walk, strict extraction

**Files:**
- Create: `aaiclick/data/data_context/arrow_ingest.py`
- Create: `aaiclick/data/data_context/test_arrow_ingest.py`

**Interfaces:**
- Consumes: `ColumnInfo` from `aaiclick/data/models.py` — `ColumnInfo(type_str, array=int)` positional type, `array` = Array() nesting depth.
- Produces (Task 2 depends on these exact signatures):
  - `infer_struct_array(records: list) -> pa.StructArray`
  - `struct_type_to_columns(struct_type: pa.StructType) -> dict[str, ColumnInfo]`
  - `struct_array_to_columns(arr: pa.StructArray) -> dict[str, list]`
  - `leaf_column_info(pa_type: pa.DataType, array_depth: int = 0) -> ColumnInfo`

- [ ] **Step 1: Write the failing tests**

Create `aaiclick/data/data_context/test_arrow_ingest.py`:

```python
"""Tests for arrow_ingest - pyarrow-based ingest schema evaluation."""

import pyarrow as pa
import pytest

from aaiclick.data.data_context.arrow_ingest import (
    infer_struct_array,
    leaf_column_info,
    struct_array_to_columns,
    struct_type_to_columns,
)


def test_schema_nested_dict():
    """Plain nested dicts map to dot columns with no Array level."""
    arr = infer_struct_array([{"a": 2, "x": {"y": {"z": 1}}}])
    cols = struct_type_to_columns(arr.type)
    assert cols["a"].type == "Int64"
    assert int(cols["a"].array) == 0
    assert cols["x.y.z"].type == "Int64"
    assert int(cols["x.y.z"].array) == 0


def test_schema_star_levels():
    """list-of-dicts adds one Array level; a list leaf inside adds another."""
    arr = infer_struct_array([{"b": [{"c": [1, 2], "d": 5}]}])
    cols = struct_type_to_columns(arr.type)
    assert cols["b.*.c"].type == "Int64"
    assert int(cols["b.*.c"].array) == 2
    assert cols["b.*.d"].type == "Int64"
    assert int(cols["b.*.d"].array) == 1


def test_schema_unifies_across_all_records():
    """Inference scans all records, not just the first."""
    arr = infer_struct_array([{"c": 1}, {"c": 2, "d": 3}])
    cols = struct_type_to_columns(arr.type)
    assert "d" in cols


def test_missing_key_across_records_raises():
    """Strict semantics: a key absent in some records is rejected."""
    arr = infer_struct_array([{"c": 1}, {"c": 2, "d": 3}])
    with pytest.raises(ValueError, match="identical keys"):
        struct_array_to_columns(arr)


def test_missing_key_inside_list_items_raises():
    """Strictness applies inside list-of-dicts items too (silent-drop fix)."""
    arr = infer_struct_array([{"b": [{"c": 1}, {"c": 2, "d": 3}]}])
    with pytest.raises(ValueError, match="identical keys"):
        struct_array_to_columns(arr)


def test_extraction_parallel_arrays():
    """Leaves come out as dot-star parallel arrays with per-row grouping."""
    arr = infer_struct_array([{"a": 1, "b": [{"c": 10}, {"c": 20}]}, {"a": 2, "b": []}])
    cols = struct_array_to_columns(arr)
    assert cols["a"] == [1, 2]
    assert cols["b.*.c"] == [[10, 20], []]


def test_extraction_deep_mixed_nesting():
    """dict inside list items and list inside dict both extract correctly."""
    arr = infer_struct_array([{"x": {"w": 1, "y": [{"z": 1}, {"z": 2}]}}])
    cols = struct_array_to_columns(arr)
    assert cols["x.w"] == [1]
    assert cols["x.y.*.z"] == [[1, 2]]


def test_non_dict_item_raises():
    with pytest.raises(ValueError, match="uniform schema"):
        infer_struct_array([{"b": [{"c": 1}, 5]}])


def test_type_conflict_raises():
    with pytest.raises(ValueError, match="uniform schema"):
        infer_struct_array([{"a": 1}, {"a": "s"}])


def test_dotted_field_name_raises():
    arr = infer_struct_array([{"a.b": 1}])
    with pytest.raises(ValueError, match="must not contain"):
        struct_type_to_columns(arr.type)


def test_nested_dotted_field_name_raises():
    arr = infer_struct_array([{"x": {"a.b": 1}}])
    with pytest.raises(ValueError, match="must not contain"):
        struct_type_to_columns(arr.type)


def test_empty_struct_raises():
    arr = infer_struct_array([{"x": {}}])
    with pytest.raises(ValueError, match="Empty dict"):
        struct_type_to_columns(arr.type)


def test_empty_list_falls_back_to_string_array():
    arr = infer_struct_array([{"b": []}])
    cols = struct_type_to_columns(arr.type)
    assert cols["b"].type == "String"
    assert int(cols["b"].array) == 1


def test_none_dict_value_raises():
    """A record where a dict field is None in some records is rejected."""
    arr = infer_struct_array([{"m": {"c": 1}}, {"m": None}])
    with pytest.raises(ValueError, match="identical keys"):
        struct_array_to_columns(arr)


def test_leaf_column_info_mapping():
    assert leaf_column_info(pa.int64()).type == "Int64"
    assert leaf_column_info(pa.float64()).type == "Float64"
    assert leaf_column_info(pa.bool_()).type == "Bool"
    assert leaf_column_info(pa.string()).type == "String"
    assert leaf_column_info(pa.timestamp("us")).type == "DateTime64(3, 'UTC')"
    assert leaf_column_info(pa.null()).type == "String"
    assert int(leaf_column_info(pa.int64(), array_depth=2).array) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest aaiclick/data/data_context/test_arrow_ingest.py -v --no-cov`
Expected: FAIL at import — `ModuleNotFoundError`/`ImportError` (module doesn't exist).

- [ ] **Step 3: Implement the module**

Create `aaiclick/data/data_context/arrow_ingest.py`:

```python
"""
aaiclick.data.data_context.arrow_ingest - Arrow-based ingest schema evaluation.

``pa.array(records)`` infers one unified nested type across ALL records in
C++ (no first-record sampling; ``pa.Table.from_pylist`` must NOT be used -
it takes top-level keys from the first record only). The type tree maps
1:1 onto dot notation: struct field -> ``x.y`` (no Array level),
list<struct> -> ``x.*.y`` (one Array level per star). Keys missing in some
records/items surface as nulls in the unified type and are rejected -
strict identical-keys semantics with no per-item Python work. Leaf data
leaves arrow through one ``to_pylist()`` per flat column.
"""

from __future__ import annotations

import pyarrow as pa

from ..models import ColumnInfo


def infer_struct_array(records: list) -> pa.StructArray:
    """Infer a unified StructArray from a list of dict records.

    Arrow conversion errors (mixed dict/non-dict items, cross-record type
    conflicts) are translated to ``ValueError``.
    """
    try:
        arr = pa.array(records)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        raise ValueError(f"Cannot infer a uniform schema from records: {e}") from e
    if not pa.types.is_struct(arr.type):
        raise ValueError(f"Records must be dicts, got arrow type {arr.type}")
    return arr


def leaf_column_info(pa_type: pa.DataType, array_depth: int = 0) -> ColumnInfo:
    """Map an arrow leaf type to a ColumnInfo (parity with legacy inference)."""
    if pa.types.is_boolean(pa_type):
        ch_type = "Bool"
    elif pa.types.is_timestamp(pa_type):
        ch_type = "DateTime64(3, 'UTC')"
    elif pa.types.is_integer(pa_type):
        ch_type = "Int64"
    elif pa.types.is_floating(pa_type):
        ch_type = "Float64"
    else:
        ch_type = "String"
    return ColumnInfo(ch_type, array=array_depth)


def _is_list_type(pa_type: pa.DataType) -> bool:
    return pa.types.is_list(pa_type) or pa.types.is_large_list(pa_type)


def struct_type_to_columns(struct_type: pa.StructType) -> dict[str, ColumnInfo]:
    """Walk the arrow type tree into flat dot-notation ColumnInfos.

    Raises ValueError for field names containing ``.`` and for empty
    structs (no representable columns).
    """
    columns: dict[str, ColumnInfo] = {}
    for i in range(struct_type.num_fields):
        field = struct_type.field(i)
        _walk_type(field.name, field.type, "", 0, columns)
    return columns


def _walk_type(
    name: str,
    pa_type: pa.DataType,
    prefix: str,
    array_depth: int,
    columns: dict[str, ColumnInfo],
) -> None:
    key_path = f"{prefix}{name}"
    if "." in name:
        raise ValueError(f"Dict keys must not contain '.': {key_path!r}")
    if pa.types.is_struct(pa_type):
        if pa_type.num_fields == 0:
            raise ValueError(f"Empty dict values are not supported: {key_path!r}")
        for i in range(pa_type.num_fields):
            field = pa_type.field(i)
            _walk_type(field.name, field.type, f"{key_path}.", array_depth, columns)
    elif _is_list_type(pa_type):
        elem = pa_type.value_type
        if pa.types.is_struct(elem):
            if elem.num_fields == 0:
                raise ValueError(f"Empty dict values are not supported: {key_path!r}")
            for i in range(elem.num_fields):
                field = elem.field(i)
                _walk_type(field.name, field.type, f"{key_path}.*.", array_depth + 1, columns)
        else:
            depth = array_depth + 1
            while _is_list_type(elem):
                depth += 1
                elem = elem.value_type
            columns[key_path] = leaf_column_info(elem, depth)
    else:
        columns[key_path] = leaf_column_info(pa_type, array_depth)


def _missing(key_path: str) -> ValueError:
    return ValueError(
        f"All records must have identical keys: field {key_path!r} "
        f"is missing or None in some records"
    )


def struct_array_to_columns(arr: pa.StructArray) -> dict[str, list]:
    """Extract flat leaf columns as Python lists, enforcing strictness.

    Any null at a struct/list level, or in a typed leaf, means a key was
    missing (or None) in some records/items -> ValueError. All-null leaves
    (arrow ``null`` type, e.g. from empty lists or all-None values) pass
    through as the String fallback, matching legacy behavior.
    """
    if arr.null_count:
        raise ValueError("Records must all be dicts (found a null record)")
    leaves = _extract_struct(arr, "")
    return {name: leaf.to_pylist() for name, leaf in leaves.items()}


def _extract_struct(arr: pa.StructArray, prefix: str) -> dict[str, pa.Array]:
    out: dict[str, pa.Array] = {}
    for i in range(arr.type.num_fields):
        field = arr.type.field(i)
        _extract_field(f"{prefix}{field.name}", arr.field(i), out)
    return out


def _extract_field(key_path: str, arr: pa.Array, out: dict[str, pa.Array]) -> None:
    pa_type = arr.type
    if pa.types.is_struct(pa_type):
        if arr.null_count:
            raise _missing(key_path)
        out.update(_extract_struct(arr, f"{key_path}."))
    elif _is_list_type(pa_type):
        if arr.null_count:
            raise _missing(key_path)
        if pa.types.is_struct(pa_type.value_type):
            values = arr.values
            sub = _extract_struct(values, f"{key_path}.*.")
            for name, leaf in sub.items():
                out[name] = pa.ListArray.from_arrays(arr.offsets, leaf)
        else:
            inner = arr
            while _is_list_type(inner.type):
                inner = inner.values
            if inner.null_count and not pa.types.is_null(inner.type):
                raise _missing(key_path)
            out[key_path] = arr
    else:
        if arr.null_count and not pa.types.is_null(pa_type):
            raise _missing(key_path)
        out[key_path] = arr
```

Note on `_extract_field`'s list-of-struct branch: struct-level nulls inside
list items cannot occur (an item is either a dict or the whole array fails
inference), so the strictness signal there is leaf nulls, caught by the
recursive `_extract_struct` -> `_extract_field` calls on the item fields.
The rewrap `pa.ListArray.from_arrays(arr.offsets, leaf)` preserves per-row
grouping because `leaf` is aligned with `arr.values` rows; recursion
composes this for deeper `.*.` levels. Arrays from a fresh `pa.array()`
call are unsliced, which `arr.offsets` assumes.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest aaiclick/data/data_context/test_arrow_ingest.py -v --no-cov`
Expected: 15 PASS

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/data_context/arrow_ingest.py aaiclick/data/data_context/test_arrow_ingest.py
SKIP=pyright git commit -m "Add arrow_ingest module for whole-dataset schema inference

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Qi4jo2QMYBFKFBDzFCPuDd"
```

(Use `SKIP=pyright` only if pre-commit fails solely on the known `reportMissingImports` container artifact; if it passes clean, commit normally.)

---

### Task 2: Rewire `create_object_from_value` through arrow_ingest

**Files:**
- Modify: `aaiclick/data/data_context/data_context.py` (dict + records branches of `create_object_from_value`; delete superseded helpers)
- Modify: `aaiclick/data/object/test_nested_dicts.py` (append behavior tests)

**Interfaces:**
- Consumes (from Task 1, exact): `infer_struct_array(records) -> pa.StructArray`, `struct_type_to_columns(struct_type) -> dict[str, ColumnInfo]`, `struct_array_to_columns(arr) -> dict[str, list]`, `leaf_column_info(pa_type, array_depth=0) -> ColumnInfo` — import at top of `data_context.py` as `from .arrow_ingest import infer_struct_array, leaf_column_info, struct_array_to_columns, struct_type_to_columns`.
- Produces: `create_object_from_value` behavior contract (all existing tests pass; new strictness behaviors below).

- [ ] **Step 1: Append failing behavior tests**

Append to `aaiclick/data/object/test_nested_dicts.py`:

```python
# =============================================================================
# Arrow-based inference — whole-dataset strictness
# =============================================================================


async def test_extra_key_in_later_record_raises(ctx):
    """Keys present only in later records are rejected, not silently dropped."""
    with pytest.raises(ValueError, match="identical keys"):
        await create_object_from_value([{"c": 1}, {"c": 2, "d": 3}])


async def test_extra_key_in_later_list_item_raises(ctx):
    """Keys present only in later list-of-dicts items are rejected (silent-drop fix)."""
    with pytest.raises(ValueError, match="identical keys"):
        await create_object_from_value({"b": [{"c": 1}, {"c": 2, "d": 3}]})


async def test_non_dict_item_in_list_of_dicts_raises(ctx):
    """A non-dict mixed into a list of dicts raises a clear ValueError."""
    with pytest.raises(ValueError, match="uniform schema"):
        await create_object_from_value({"b": [{"c": 1}, 5]})


async def test_cross_record_type_conflict_raises(ctx):
    """A field that changes type across records raises a clear ValueError."""
    with pytest.raises(ValueError, match="uniform schema"):
        await create_object_from_value([{"a": 1}, {"a": "s"}])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest aaiclick/data/object/test_nested_dicts.py -v --no-cov`
Expected: the 4 new tests FAIL (first two: no error raised / silent drop; last two: `AttributeError` or driver error instead of the matched `ValueError`). All 14 pre-existing tests still PASS.

- [ ] **Step 3: Rewire the dict branch**

In `aaiclick/data/data_context/data_context.py`:

Add to the current-package import group at top: `from .arrow_ingest import infer_struct_array, leaf_column_info, struct_array_to_columns, struct_type_to_columns`.

Replace the entire `if isinstance(val, dict):` block of `create_object_from_value` (from `if isinstance(val, dict):` down to — but not including — the `elif isinstance(val, list):` line) with:

```python
    if isinstance(val, dict):
        has_arrays = any(isinstance(v, list) for v in val.values())

        if has_arrays and not _has_nested_dicts(val):
            # Dict of parallel arrays: one row per element.
            columns = {}
            array_len = None

            for key, value in val.items():
                if isinstance(value, list):
                    if array_len is None:
                        array_len = len(value)
                    elif len(value) != array_len:
                        raise ValueError(
                            f"All arrays must have same length. Expected {array_len}, got {len(value)} for key '{key}'"
                        )
                    if "." in key:
                        raise ValueError(f"Dict keys must not contain '.': {key!r}")
                    try:
                        pa_arr = pa.array(value)
                    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
                        raise ValueError(f"Cannot infer a uniform schema from records: {e}") from e
                    elem_type = pa_arr.type
                    depth = 0
                    while pa.types.is_list(elem_type) or pa.types.is_large_list(elem_type):
                        depth += 1
                        elem_type = elem_type.value_type
                    col_def = leaf_column_info(elem_type, depth)
                else:
                    raise ValueError(
                        f"Dict of arrays requires all values to be lists. Key '{key}' has type {type(value).__name__}"
                    )
                columns[key] = col_def.with_fieldtype(FIELDTYPE_ARRAY)

            columns = _maybe_add_aai_id(_apply_field_specs(columns, fields))
            schema = Schema(fieldtype=FIELDTYPE_DICT, columns=columns, order_by=order_by_clause)
            obj = await create_object(schema, name=name, scope=scope)

            if array_len and array_len > 0:
                keys = list(val.keys())
                array_cols: list[list[Any]] = [cast("list[Any]", val[k]) for k in keys]
                async with _maybe_insert_lock(obj.table, name):
                    await ch.insert(
                        obj.table,
                        array_cols,
                        column_names=keys,
                        column_oriented=True,
                        column_type_names=[columns[k].ch_type() for k in keys],
                    )

        else:
            # Single record (flat or nested): arrow infers the schema, leaves
            # flatten to dot/dot-star columns.
            struct_arr = infer_struct_array([val])
            columns = struct_type_to_columns(struct_arr.type)
            col_map = struct_array_to_columns(struct_arr)

            columns = _maybe_add_aai_id(_apply_field_specs(columns, fields))
            schema = Schema(fieldtype=FIELDTYPE_DICT, columns=columns, order_by=order_by_clause)
            obj = await create_object(schema, name=name, scope=scope)

            keys = list(col_map.keys())
            async with _maybe_insert_lock(obj.table, name):
                await ch.insert(
                    obj.table,
                    [col_map[k] for k in keys],
                    column_names=keys,
                    column_oriented=True,
                    column_type_names=[columns[k].ch_type() for k in keys],
                )
```

Preserve whatever follows the branch in the current function (the flat and
nested dict paths previously converged on an `oplog_record(obj.table,
"create_from_value")` + return — check the current tail of the function and
keep exactly one oplog call per created object; the early
`return result` lines that belonged to `_create_nested_object` /
`_create_nested_records_object` calls disappear with them).

- [ ] **Step 4: Rewire the records branch**

Replace the records sub-branch of `elif isinstance(val, list):` — everything inside `if val and isinstance(val[0], dict):` — with:

```python
        if val and isinstance(val[0], dict):
            # Narrow: list-of-dicts (ValueRecordType). pyright can't infer this
            # from isinstance(val[0], dict) alone.
            records = cast("list[ValueDictType]", val)

            struct_arr = infer_struct_array(records)
            # List-of-records is dict-of-arrays — each column carries N values
            # across the input records, so fieldtype must be ARRAY.
            columns = {
                name_: ci.with_fieldtype(FIELDTYPE_ARRAY)
                for name_, ci in struct_type_to_columns(struct_arr.type).items()
            }
            col_map = struct_array_to_columns(struct_arr)

            columns = _maybe_add_aai_id(_apply_field_specs(columns, fields))
            schema = Schema(fieldtype=FIELDTYPE_DICT, columns=columns, order_by=order_by_clause)
            obj = await create_object(schema, name=name, scope=scope)

            keys = list(col_map.keys())
            async with _maybe_insert_lock(obj.table, name):
                await ch.insert(
                    obj.table,
                    [col_map[k] for k in keys],
                    column_names=keys,
                    column_oriented=True,
                    column_type_names=[columns[k].ch_type() for k in keys],
                )
```

- [ ] **Step 5: Delete superseded helpers**

Delete from `data_context.py`: `_flatten_nested_schema`, `_flatten_nested_record`, `_validate_nested_keys`, `_find_non_empty_nested_sample`, `_create_nested_object`, `_create_nested_records_object`, `_infer_array_clickhouse_type`.

KEEP: `_has_nested_dicts` and `_is_list_of_dicts` (dispatch predicates, O(top-level keys) — the spec listed `_has_nested_dicts` for removal but the dict-of-arrays dispatch still needs it; this deviation is authorized). KEEP `_infer_clickhouse_type` (still used by the scalar and list-of-scalars paths).

Before deleting, grep for usages to catch stragglers:
`grep -rn "_flatten_nested\|_validate_nested_keys\|_find_non_empty\|_create_nested\|_infer_array_clickhouse_type" aaiclick/`
Expected: no remaining references after the rewiring (docstrings in `docs/` are handled in Task 3).

Also update the `create_object_from_value` docstring `val:` bullet list — replace the two nested bullets with:

```
            - Dict/List with nested dicts: flattened to plain-dot columns
              (``{"x": {"y": 1}}`` → column ``x.y``)
            - Dict/List with nested list-of-dicts: flattened with dot-star
              notation (``{"b": [{"c": 1}]}`` → column ``b.*.c``)
            The schema is inferred by pyarrow across ALL records — keys must
            be identical in every record (missing keys raise ``ValueError``).
```

(keeping the existing closing sentence about keys containing ``.`` and empty dict values raising ``ValueError``).

- [ ] **Step 6: Run the covering suites**

Run: `python -m pytest aaiclick/data/object/test_nested_dicts.py aaiclick/data/object/test_nested_arrays.py aaiclick/data/data_context/ -v --no-cov`
Expected: all PASS (18 nested-dict + 8 nested-array + data_context suites). Error-message compatibility matters: `identical keys`, `must not contain`, `Empty dict` substrings are asserted by existing tests.

Run: `python -m pytest aaiclick/data/ -q`
Expected: only the 3 known `test_order_by.py` failures. If any other test fails, fix the ROOT CAUSE (likely an internal caller relying on removed helpers or looser semantics) — never weaken the new validation; if intent is unclear, report BLOCKED.

- [ ] **Step 7: Commit**

```bash
git add aaiclick/data/data_context/data_context.py aaiclick/data/object/test_nested_dicts.py
SKIP=pyright git commit -m "Route dict and records ingest through arrow whole-dataset inference

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Qi4jo2QMYBFKFBDzFCPuDd"
```

---

### Task 3: Docs consolidation, cleanup of implemented specs/plans, full suite, push

**Files:**
- Modify: `docs/user_guide/object.md` (condense design into the Nested Data Flattening section)
- Modify: `docs/designs/future.md` (remove obsolete entry; add native-arrow-insert entry)
- Delete: `docs/designs/nested_dict_flattening.md`, `docs/designs/arrow_ingest.md`, `docs/superpowers/plans/2026-07-06-nested-dict-flattening.md`, `docs/superpowers/plans/2026-07-07-arrow-ingest.md`

**Interfaces:**
- Consumes: final function names from Tasks 1-2 (`infer_struct_array`, `struct_type_to_columns`, `struct_array_to_columns` in `aaiclick/data/data_context/arrow_ingest.py`; `_unflatten_record`, `_undot_record` in `aaiclick/data/object/data_extraction.py`).
- Produces: docs end-state — `object.md` carries the short design description; implemented spec/plan files are gone (user instruction: remove implemented designs and plans from this session once done).

- [ ] **Step 1: Condense the design into `object.md`**

In `docs/user_guide/object.md`, section `# Nested Data Flattening`: replace the existing `**Implementation**:` line and the paragraph after it with:

```
**Implementation**: `aaiclick/data/data_context/arrow_ingest.py` — see `infer_struct_array()`, `struct_type_to_columns()`, `struct_array_to_columns()`; reconstruction in `aaiclick/data/object/data_extraction.py` — see `_unflatten_record()`.

Nested dicts and lists-of-dicts flatten to standalone columns — better
compression, skipping indexes, and `LowCardinality` than ClickHouse
`Map`/`Tuple`/`JSON` types. The schema is inferred by pyarrow scanning
ALL records in C++ (no first-record sampling, no per-item Python work);
keys must be identical across records — missing keys, mixed dict/non-dict
items, and cross-record type conflicts raise `ValueError` at ingest.
`data()` reconstructs the original nesting by name-parsing the dot /
dot-star column names.
```

Keep the notation table, the code example, the `ValueError` caveat sentence (including the any-dict-shaped-read caveat), and the `**Tests**` line that follow — but update the `**Tests**` line to:

```
**Tests**: `aaiclick/data/object/test_nested_dicts.py`, `aaiclick/data/object/test_nested_arrays.py`, `aaiclick/data/data_context/test_arrow_ingest.py`.
```

- [ ] **Step 2: Update `future.md`**

In `docs/designs/future.md`:
1. DELETE the whole `## Nested Dict Flattening — Item-Homogeneity Validation` section — it is now implemented (arrow strictness rejects heterogeneous items; Task 2's `test_extra_key_in_later_list_item_raises` and `test_non_dict_item_in_list_of_dicts_raises` pin it).
2. ADD (matching the file's existing entry format, near other data-layer entries):

```
## Native Arrow Insert for Ingest

`create_object_from_value` converts arrow leaves to Python lists
(`to_pylist()`) for the list-based `ChClient.insert`. A follow-up can add
an arrow-native insert to the `ChClient` protocol (clickhouse-connect
`insert_arrow`; chdb `Python()` table engine) to skip that conversion.
Worth doing only if profiling shows the conversion matters — the
per-record Python passes are already gone.
```

- [ ] **Step 3: Remove implemented specs and plans (user instruction)**

```bash
git rm docs/designs/nested_dict_flattening.md docs/designs/arrow_ingest.md
git rm docs/superpowers/plans/2026-07-06-nested-dict-flattening.md docs/superpowers/plans/2026-07-07-arrow-ingest.md
```

Then check nothing references the deleted files:
`grep -rn "nested_dict_flattening\|designs/arrow_ingest" docs/ aaiclick/ README.md 2>/dev/null`
Expected: no matches (fix any straggler by pointing it at `docs/user_guide/object.md`).

- [ ] **Step 4: Full test suite**

Run: `python -m pytest aaiclick/ -q`
Expected: only the 3 known pre-existing `test_order_by.py` failures; everything else passes. Report exact counts.

- [ ] **Step 5: Commit and push**

```bash
git add docs/user_guide/object.md docs/designs/future.md
SKIP=pyright git commit -m "Consolidate nested ingest docs into user guide, drop implemented specs

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Qi4jo2QMYBFKFBDzFCPuDd"
git push -u origin claude/clickhouse-json-structure-support-focsq0
```

(Retry push up to 4 times with 2s/4s/8s/16s backoff on network errors only.)

- [ ] **Step 6: Verify CI**

Controller runs the `devpowers:check-pr` flow against PR #359 (do not create a PR).
