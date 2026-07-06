# Nested Dict Flattening (`x.y.z` Columns) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Flatten singleton nested dicts to plain-dot `x.y.z` ClickHouse columns on ingest and reconstruct them on `data()`, complementing the existing `x.*.z` array-of-objects notation.

**Architecture:** Extend the existing flatten recursion in `aaiclick/data/data_context/data_context.py` with a dict branch (prefix `x.`, no `Array()` wrapper — unlike `.*.` which adds one), and extend the unflatten pass in `aaiclick/data/object/data_extraction.py` with a plain-dot grouping helper that runs after the existing `.*.` pass. Validation rejects dotted input keys and empty dict values at ingest.

**Tech Stack:** Python 3 (async), pytest + pytest-asyncio (auto mode), chdb-backed test fixture `ctx` from `aaiclick/conftest.py`.

**Spec:** `docs/designs/nested_dict_flattening.md`

## Global Constraints

- ALL imports at top of file, three groups (stdlib / external / current package). Never inside functions.
- Tests are flat module-level `async def test_*(ctx):` functions — no classes, no `@pytest.mark.asyncio` decorator (auto-detected).
- `filterwarnings = ["error"]` — any warning fails a test.
- No `Any` shortcuts; no `__all__`; no history comments about removed code.
- Doc references use function/class names, never line numbers.
- Committer identity must be `Claude <noreply@anthropic.com>` (`git config user.email noreply@anthropic.com && git config user.name Claude` — already set in this clone).
- Every commit message ends with:
  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01Qi4jo2QMYBFKFBDzFCPuDd
  ```
- Branch: `claude/clickhouse-json-structure-support-focsq0`. Push with `git push -u origin claude/clickhouse-json-structure-support-focsq0`.

---

### Task 1: Flatten side — singleton dicts become `x.y.z` columns

**Files:**
- Modify: `aaiclick/data/data_context/data_context.py` (`_has_nested_dicts`, `_flatten_nested_schema`, `_flatten_nested_record`)
- Create: `aaiclick/data/object/test_nested_dicts.py`

**Interfaces:**
- Consumes: existing `_is_list_of_dicts(value)`, `_infer_clickhouse_type(value)`, `ColumnInfo(type, array=int, low_cardinality=...)`.
- Produces: `_flatten_nested_schema` / `_flatten_nested_record` now handle `isinstance(val, dict)` values; `_has_nested_dicts(record)` returns `True` when any value is a plain dict OR a list-of-dicts. Task 2 relies on the flat column names these produce (`x.y.z`, `x.y.*.z`, `b.*.c.d`).

- [ ] **Step 1: Write the failing schema tests**

Create `aaiclick/data/object/test_nested_dicts.py`:

```python
"""
Tests for singleton nested dict support - creating Objects from dicts
containing plain dict values, stored with dot column notation.

Example: {a: 2, x: {y: {z: 1}}}
Flattens to columns: a (Int64), x.y.z (Int64)
"""

import pytest

from aaiclick import ORIENT_DICT, ORIENT_RECORDS, create_object_from_value

# =============================================================================
# Schema — dot notation column names
# =============================================================================


async def test_singleton_dict_schema(ctx):
    """Plain nested dicts flatten to dot-notation columns with no Array wrapper."""
    obj = await create_object_from_value({"a": 2, "x": {"y": {"z": 1}}})

    schema = obj.schema
    assert "a" in schema.columns
    assert "x.y.z" in schema.columns
    assert schema.columns["x.y.z"].type == "Int64"
    assert int(schema.columns["x.y.z"].array) == 0


async def test_dict_inside_array_items_schema(ctx):
    """A dict inside list-of-dicts items extends the name after the star."""
    obj = await create_object_from_value({"b": [{"c": {"d": 5}}, {"c": {"d": 10}}]})

    schema = obj.schema
    assert "b.*.c.d" in schema.columns
    assert schema.columns["b.*.c.d"].type == "Int64"
    assert int(schema.columns["b.*.c.d"].array) == 1


async def test_array_of_objects_inside_dict_schema(ctx):
    """A list-of-dicts inside a dict gets the star after the dot prefix."""
    obj = await create_object_from_value({"x": {"y": [{"z": 1}, {"z": 2}]}})

    schema = obj.schema
    assert "x.y.*.z" in schema.columns
    assert schema.columns["x.y.*.z"].type == "Int64"
    assert int(schema.columns["x.y.*.z"].array) == 1
```

(`pytest` import is used by Task 3 tests in this same file; keep it from the start.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/data/object/test_nested_dicts.py -v`
Expected: all 3 FAIL — `{"x": {"y": ...}}` currently routes to the flat-dict path, so `x.y.z` is absent (the `x` column falls back to `String`).

- [ ] **Step 3: Implement the dict branch in the flatten recursion**

In `aaiclick/data/data_context/data_context.py`:

Replace `_has_nested_dicts`:

```python
def _has_nested_dicts(record: dict) -> bool:
    """Check if a dict contains any dict or list-of-dicts values (nested structures)."""
    return any(isinstance(v, dict) or _is_list_of_dicts(v) for v in record.values())
```

In `_flatten_nested_schema`, add a dict branch BEFORE the `_is_list_of_dicts` check (inside the `for key, val in sample.items():` loop):

```python
        if isinstance(val, dict):
            sub_cols = _flatten_nested_schema(val, f"{col_name}.", array_depth)
            columns.update(sub_cols)
        elif _is_list_of_dicts(val):
```

(the existing `_is_list_of_dicts` branch becomes `elif`; the rest of the function is unchanged — note `array_depth` passes through unchanged for the dict branch, while the star branch keeps `array_depth + 1`).

In `_flatten_nested_record`, add the matching branch (inside the `for key, val in record.items():` loop):

```python
        if isinstance(val, dict):
            result.update(_flatten_nested_record(val, f"{col_name}."))
        elif _is_list_of_dicts(val):
```

(again the existing branch becomes `elif`, and the trailing `else: result[col_name] = val` stays).

Also update the docstrings of `_flatten_nested_schema` and `_flatten_nested_record`: both currently say "Uses dot-star notation for nested array-of-objects levels" — extend to mention plain-dot notation for dict values, e.g. "Dict values extend the name with plain-dot notation (``x.y``); list-of-dicts values use dot-star (``x.*.y``) and add one Array() level."

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest aaiclick/data/object/test_nested_dicts.py -v`
Expected: 3 PASS

- [ ] **Step 5: Verify no regression in existing nested-array tests**

Run: `pytest aaiclick/data/object/test_nested_arrays.py aaiclick/data/data_context/ -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/data_context/data_context.py aaiclick/data/object/test_nested_dicts.py
git commit -m "Flatten singleton nested dicts to plain-dot x.y.z columns

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Qi4jo2QMYBFKFBDzFCPuDd"
```

---

### Task 2: Unflatten side — `data()` reconstructs nested dicts

**Files:**
- Modify: `aaiclick/data/object/data_extraction.py` (`_has_nested_columns`, `_unflatten_record`, new `_undot_record`)
- Modify: `aaiclick/data/object/test_nested_dicts.py` (append round-trip tests)

**Interfaces:**
- Consumes: flat column names from Task 1 (`x.y.z`, `x.y.*.z`, `b.*.c.d`) arriving in `flat_record` dicts inside `_unflatten_record`.
- Produces: `_undot_record(flat: dict) -> dict` module-level helper; `_unflatten_record` returns fully nested dicts; `_has_nested_columns(column_names)` is `True` for any name containing `"."`.

- [ ] **Step 1: Append failing round-trip tests**

Append to `aaiclick/data/object/test_nested_dicts.py`:

```python
# =============================================================================
# Round-trip — data() reconstructs the original nesting
# =============================================================================


async def test_singleton_dict_round_trip(ctx):
    """Deep plain nesting reconstructs exactly."""
    obj = await create_object_from_value({"a": 2, "x": {"y": {"z": 1}}})

    data = await obj.data()

    assert data == {"a": 2, "x": {"y": {"z": 1}}}


async def test_records_with_dict_field_round_trip(ctx):
    """Records list with a dict field, both orients."""
    records = [
        {"a": 1, "meta": {"source": "s1", "score": 0.5}},
        {"a": 2, "meta": {"source": "s2", "score": 0.75}},
    ]
    obj = await create_object_from_value(records)

    as_records = await obj.data(orient=ORIENT_RECORDS)
    assert as_records == records

    as_dict = await obj.data(orient=ORIENT_DICT)
    assert as_dict["a"] == [1, 2]
    assert as_dict["meta"] == [
        {"source": "s1", "score": 0.5},
        {"source": "s2", "score": 0.75},
    ]


async def test_array_of_objects_inside_dict_round_trip(ctx):
    """Mixed notation x.y.*.z reconstructs dict-of-list-of-dicts."""
    obj = await create_object_from_value({"x": {"y": [{"z": 1}, {"z": 2}]}})

    data = await obj.data()

    assert data == {"x": {"y": [{"z": 1}, {"z": 2}]}}


async def test_dict_inside_array_items_round_trip(ctx):
    """Mixed notation b.*.c.d reconstructs list-of-dicts-of-dicts."""
    obj = await create_object_from_value({"b": [{"c": {"d": 5}}, {"c": {"d": 10}}]})

    data = await obj.data()

    assert data == {"b": [{"c": {"d": 5}}, {"c": {"d": 10}}]}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/data/object/test_nested_dicts.py -v`
Expected: Task 1 schema tests PASS; the 4 new round-trip tests FAIL — plain-dot tables miss the `_has_nested_columns` gate (checks `".*."` only), so `data()` returns flat keys like `{"x.y.z": 1}`.

- [ ] **Step 3: Implement the plain-dot unflatten pass**

In `aaiclick/data/object/data_extraction.py`:

Replace `_has_nested_columns`:

```python
def _has_nested_columns(column_names: list[str]) -> bool:
    """Check if any column names use dot notation for nested structures."""
    return any("." in name for name in column_names)
```

Add `_undot_record` after `_unflatten_record`:

```python
def _undot_record(flat: dict) -> dict:
    """Group plain-dot keys by first segment and recurse.

    ``{"a": 2, "x.y.z": 1}`` becomes ``{"a": 2, "x": {"y": {"z": 1}}}``.
    """
    result: dict = {}
    groups: dict[str, dict] = {}
    for key, val in flat.items():
        dot_pos = key.find(".")
        if dot_pos == -1:
            result[key] = val
        else:
            groups.setdefault(key[:dot_pos], {})[key[dot_pos + 1 :]] = val
    for prefix, sub in groups.items():
        result[prefix] = _undot_record(sub)
    return result
```

In `_unflatten_record`, change the final `return result` to:

```python
    return _undot_record(result)
```

and extend its docstring to note the second pass, e.g. add: "After the dot-star pass, plain-dot keys (``x.y.z`` and star-group prefixes like ``x.y`` from ``x.y.*.z``) are regrouped into nested dicts by ``_undot_record``."

Why this ordering works: the star pass consumes `.*.` splits first, leaving (a) plain columns whose names may still contain `.` and (b) star-group prefixes like `x.y` as top-level keys of `result`; `_undot_record` then regroups both. Items inside star groups recurse through `_unflatten_record` and get their own `_undot_record` pass (covers `b.*.c.d`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest aaiclick/data/object/test_nested_dicts.py -v`
Expected: all 7 PASS

- [ ] **Step 5: Verify no regression across the object test suite**

Run: `pytest aaiclick/data/object/ -v`
Expected: all PASS (`_has_nested_columns` now fires for any dotted name — existing suites must be unaffected because auto-inferred flat columns never contain dots)

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/object/data_extraction.py aaiclick/data/object/test_nested_dicts.py
git commit -m "Reconstruct plain-dot x.y.z columns into nested dicts on data()

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Qi4jo2QMYBFKFBDzFCPuDd"
```

---

### Task 3: Validation — dotted keys, empty dicts, recursive identical keys

**Files:**
- Modify: `aaiclick/data/data_context/data_context.py` (new `_validate_nested_keys`, calls in `create_object_from_value`, flat key-set check in `_create_nested_records_object`)
- Modify: `aaiclick/data/object/test_nested_dicts.py` (append validation tests)

**Interfaces:**
- Consumes: `_is_list_of_dicts`, `_flatten_nested_record` (Task 1 version).
- Produces: `_validate_nested_keys(record: dict, path: str = "") -> None` raising `ValueError`; called for every dict ingested through `create_object_from_value`.

- [ ] **Step 1: Append failing validation tests**

Append to `aaiclick/data/object/test_nested_dicts.py`:

```python
# =============================================================================
# Validation
# =============================================================================


async def test_dotted_key_raises(ctx):
    """Flat dict keys containing '.' are rejected — data() would reshape them."""
    with pytest.raises(ValueError, match="must not contain"):
        await create_object_from_value({"a.b": 1})


async def test_nested_dotted_key_raises(ctx):
    """Dotted keys are rejected at any nesting level."""
    with pytest.raises(ValueError, match="must not contain"):
        await create_object_from_value({"x": {"a.b": 1}})


async def test_dotted_key_inside_array_items_raises(ctx):
    """Dotted keys inside list-of-dicts items are rejected."""
    with pytest.raises(ValueError, match="must not contain"):
        await create_object_from_value([{"b": [{"c.d": 1}]}])


async def test_empty_dict_value_raises(ctx):
    """Empty dicts have no representable columns."""
    with pytest.raises(ValueError, match="[Ee]mpty dict"):
        await create_object_from_value({"x": {}})


async def test_mismatched_nested_keys_raises(ctx):
    """Nested dict fields must have identical key sets across records."""
    with pytest.raises(ValueError, match="identical keys"):
        await create_object_from_value(
            [
                {"m": {"a": 1}},
                {"m": {"b": 2}},
            ]
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/data/object/test_nested_dicts.py -v`
Expected: the 5 new tests FAIL (no ValueError raised, or a different error surfaces from deep inside insert)

- [ ] **Step 3: Implement validation**

In `aaiclick/data/data_context/data_context.py`, add after `_has_nested_dicts`:

```python
def _validate_nested_keys(record: dict, path: str = "") -> None:
    """Reject keys containing '.' and empty dict values before flattening.

    Reconstruction in ``data()`` is name-parsed, so a dotted input key would
    round-trip as a nested dict; an empty dict has no representable columns.
    """
    for key, val in record.items():
        key_path = f"{path}.{key}" if path else key
        if "." in key:
            raise ValueError(f"Dict keys must not contain '.': {key_path!r}")
        if isinstance(val, dict):
            if not val:
                raise ValueError(f"Empty dict values are not supported: {key_path!r}")
            _validate_nested_keys(val, key_path)
        elif _is_list_of_dicts(val):
            for item in val:
                if isinstance(item, dict):
                    _validate_nested_keys(item, key_path)
```

In `create_object_from_value`, call it at the top of both dict-shaped branches:

1. In the `if isinstance(val, dict):` branch, as the first statement (before the `_has_nested_dicts(val)` check):

```python
    if isinstance(val, dict):
        _validate_nested_keys(val)
        if _has_nested_dicts(val):
```

2. In the records branch, right after the `records = cast(...)` narrowing:

```python
            records = cast("list[ValueDictType]", val)
            for record in records:
                _validate_nested_keys(record)
```

In `_create_nested_records_object`, move the `all_flat` computation up to right after the existing top-level identical-keys loop, and add the flat key-set comparison (the later duplicate `all_flat = ...` line before `keys = list(all_flat[0].keys())` is then removed):

```python
    all_flat = [_flatten_nested_record(record) for record in val]
    first_flat_keys = set(all_flat[0].keys())
    for i, flat in enumerate(all_flat[1:], 1):
        if set(flat.keys()) != first_flat_keys:
            raise ValueError(
                f"All records must have identical keys (including nested dicts). "
                f"Record 0 has {sorted(first_flat_keys)}, "
                f"record {i} has {sorted(flat.keys())}"
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest aaiclick/data/object/test_nested_dicts.py -v`
Expected: all 12 PASS

- [ ] **Step 5: Run the whole data package to catch internal callers**

Run: `pytest aaiclick/data/ -q`
Expected: all PASS. If an internal caller feeds dotted keys or empty dicts into `create_object_from_value`, its test now fails — fix the CALLER (or surface to the user if intent is unclear), never weaken the validation.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/data_context/data_context.py aaiclick/data/object/test_nested_dicts.py
git commit -m "Validate dotted keys, empty dicts, and nested key sets at ingest

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Qi4jo2QMYBFKFBDzFCPuDd"
```

---

### Task 4: Docs, implementation references, full-suite verification, push

**Files:**
- Modify: `aaiclick/data/data_context/data_context.py` (`create_object_from_value` docstring)
- Modify: `docs/user_guide/object.md` (new section)
- Modify: `docs/designs/nested_dict_flattening.md` (implementation references)

**Interfaces:**
- Consumes: everything from Tasks 1-3, final function names as implemented.
- Produces: user-facing docs; spec updated per project convention (implementation references by name, no status icons).

- [ ] **Step 1: Update the `create_object_from_value` docstring**

In the `val:` parameter bullet list, replace:

```
            - Dict/List with nested list-of-dicts: Flattened with dot-star notation
```

with:

```
            - Dict/List with nested dicts: flattened to plain-dot columns
              (``{"x": {"y": 1}}`` → column ``x.y``)
            - Dict/List with nested list-of-dicts: flattened with dot-star
              notation (``{"b": [{"c": 1}]}`` → column ``b.*.c``)
```

Also add one sentence at the end of the `val:` bullet list (the docstring has no `Raises:` section — don't introduce one):

```
            Keys containing ``.`` and empty dict values raise ``ValueError``.
```

- [ ] **Step 2: Add a user-guide section**

In `docs/user_guide/object.md`, insert a new top-level section immediately after the `# Data Retrieval` section's content ends (before `# Views`):

````markdown
# Nested Data Flattening

**Implementation**: `aaiclick/data/data_context/data_context.py` — see `_flatten_nested_schema()` / `_flatten_nested_record()`; reconstruction in `aaiclick/data/object/data_extraction.py` — see `_unflatten_record()`.

Nested dicts and lists-of-dicts flatten to standalone columns — better
compression, skipping indexes, and `LowCardinality` than ClickHouse
`Map`/`Tuple`/`JSON` types. `data()` reconstructs the original nesting.

| Notation | Input shape                | Cardinality | Type effect                |
|----------|----------------------------|-------------|----------------------------|
| `.`      | dict value (nested object) | 1:1         | none — extends name only   |
| `.*.`    | list-of-dicts value        | 1:N         | adds one `Array()` wrapper |

```python
obj = await create_object_from_value({"x": {"y": {"z": 1}}})
# → column x.y.z (Int64)
await obj.data()            # {"x": {"y": {"z": 1}}}

obj = await create_object_from_value({"b": [{"c": [1, 2], "d": 5}]})
# → columns b.*.c (Array(Array(Int64))), b.*.d (Array(Int64))
```

Keys containing `.` and empty dict values raise `ValueError` at ingest —
reconstruction is name-parsed, so they cannot round-trip.

**Tests**: `aaiclick/data/object/test_nested_dicts.py`, `aaiclick/data/object/test_nested_arrays.py`.
````

(Follow the `markdown-style` skill: `#` sections, aligned tables. Run the `shortify` skill on `object.md` afterwards.)

- [ ] **Step 3: Add implementation references to the spec**

In `docs/designs/nested_dict_flattening.md`, under the `# Code changes` heading, add one line at the top of the section:

```markdown
**Implementation**: `aaiclick/data/data_context/data_context.py` — see `_flatten_nested_schema()`, `_flatten_nested_record()`, `_validate_nested_keys()`; `aaiclick/data/object/data_extraction.py` — see `_undot_record()`, `_unflatten_record()`. Tests: `aaiclick/data/object/test_nested_dicts.py`.
```

- [ ] **Step 4: Full test suite**

Run: `pytest aaiclick/ -q`
Expected: all PASS, no warnings-as-errors failures.

- [ ] **Step 5: Commit and push**

```bash
git add aaiclick/data/data_context/data_context.py docs/user_guide/object.md docs/designs/nested_dict_flattening.md
git commit -m "Document nested dict flattening and add implementation references

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Qi4jo2QMYBFKFBDzFCPuDd"
git push -u origin claude/clickhouse-json-structure-support-focsq0
```

- [ ] **Step 6: Verify CI**

Use the `devpowers:check-pr` skill (project convention after every push). If no PR exists, report that and stop — do not create one unprompted.
