# Phase 5 — `.data()` Kwargs and View Override

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Extend `Object.data()` with keyword-only `order_by`, `offset`, and `limit` parameters. Default `limit=1000` so callers can read from any array Object without building a `View` first, while preventing accidental full-table pulls. On a `View`, kwargs passed to `data()` override the stored `_order_by` / `_offset` / `_limit` when provided.

**Depends on:** Phase 4 (the cross-table contract makes the `.data()` relaxation coherent — determinism is now opt-in across the API).

**Unlocks:** Phase 6 (cleanup + docs).

---

## File Structure

| File                                                     | Role                                                                      |
|----------------------------------------------------------|---------------------------------------------------------------------------|
| `aaiclick/data/object/object.py`                         | Modify — `Object.data()` signature and body.                               |
| `aaiclick/data/object/data_extraction.py` (exists)       | Modify — extraction helpers accept `order_by`, `offset`, `limit`.          |
| `aaiclick/data/object/test_data.py` (create if missing)  | Create/modify — coverage for every rule in the resolution table.           |

---

### Task 5.1: Signature change — accept `order_by`, `offset`, `limit` kwargs

**Files:**
- Modify: `aaiclick/data/object/object.py` — `Object.data()` (currently lines 417-459 per Phase 0 research).

**Background:** The current signature is `async def data(self, orient: str = ORIENT_DICT)`. Preserve `orient` as the sole positional parameter; add `order_by`, `offset`, `limit` as keyword-only with `limit=1000` default.

- [ ] **Step 1: Write the failing tests**

**Use the real fixture `ctx` and the real module-level helper `create_object_from_value`** — `data_ctx` and `DataContext.create(...)` do not exist in this codebase.

Create `aaiclick/data/object/test_data.py`:

```python
import pytest

from aaiclick import create_object_from_value


async def test_data_limit_default_caps_at_1000(ctx):
    obj = await create_object_from_value(list(range(2500)))
    rows = await obj.data()
    assert len(rows) == 1000


async def test_data_limit_none_returns_all(ctx):
    obj = await create_object_from_value(list(range(2500)))
    rows = await obj.data(limit=None)
    assert len(rows) == 2500


async def test_data_order_by_returns_deterministic_rows(ctx):
    obj = await create_object_from_value([3, 1, 2])
    assert await obj.data(order_by="value") == [1, 2, 3]


async def test_data_offset_and_limit(ctx):
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    assert await obj.data(order_by="value", offset=1, limit=2) == [2, 3]


async def test_data_without_order_by_does_not_raise(ctx):
    # The spec: .data() does NOT raise on missing order_by.
    obj = await create_object_from_value([1, 2, 3])
    rows = await obj.data()
    assert sorted(rows) == [1, 2, 3]


async def test_scalar_data_ignores_kwargs(ctx):
    s = await create_object_from_value(42)
    assert await s.data(order_by="value", offset=5, limit=3) == 42


async def test_view_kwargs_override_view_attrs(ctx):
    obj = await create_object_from_value([1, 2, 3, 4, 5])
    v = obj.view(order_by="value", limit=2)
    # No override: uses the view's limit=2.
    assert await v.data() == [1, 2]
    # Override: kwarg wins.
    assert await v.data(limit=3) == [1, 2, 3]


async def test_view_attrs_used_when_kwargs_absent(ctx):
    obj = await create_object_from_value([3, 1, 2])
    v = obj.view(order_by="value")
    assert await v.data() == [1, 2, 3]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest aaiclick/data/object/test_data.py -v`
Expected: FAIL — `data()` doesn't accept `order_by`, `offset`, or `limit`.

- [ ] **Step 3: Rewrite the `Object.data()` signature and body**

In `aaiclick/data/object/object.py` (around lines 417-459):

```python
async def data(
    self,
    orient: str = ORIENT_DICT,
    *,
    order_by: str | None = None,
    offset: int | None = None,
    limit: int | None = 1000,
) -> Any:
    """
    Get the data from the object's table.

    Returns a scalar value, list, or dict depending on the object's fieldtype.
    Scalar objects return the value directly; array objects return a list;
    dict objects return a dict or list-of-dicts controlled by ``orient``.

    Args:
        orient: Output format for dict data — ``ORIENT_DICT`` (default) or
            ``ORIENT_RECORDS``.
        order_by: Optional ClickHouse ``ORDER BY`` clause for the read.
            Scalar and dict-record reads ignore this.
        offset: Optional row offset.
        limit: Row cap. Defaults to 1000 as a safety rail; pass ``None`` to
            fetch all rows. Scalar / single-row reads ignore this.
    """
    self.checkstale()

    # Resolve against View state when this Object is a View.
    if isinstance(self, View):
        order_by = order_by if order_by is not None else self._order_by
        offset = offset if offset is not None else self._offset
        if limit == 1000:  # default-sentinel: only override with the View's if caller did not pass one
            limit = self._limit if self._limit is not None else 1000

    # Load schema from the registry (Phase 2 made this the only read path).
    fieldtype, columns = await _get_table_schema(self.table, self.ch_client)

    if fieldtype == "s":
        return await data_extraction.extract_scalar_data(self)

    column_names = list(columns)
    if fieldtype == "d":
        return await data_extraction.extract_dict_data(
            self, column_names, columns, orient,
            order_by=order_by, offset=offset, limit=limit,
        )

    # Array Object: single `value` column.
    return await data_extraction.extract_array_data(
        self, order_by=order_by, offset=offset, limit=limit,
    )
```

Caveat on the sentinel: `limit == 1000` is a weak "did-the-caller-pass-it?" check. A clearer solution uses a sentinel object:

```python
_UNSET = object()

async def data(
    self,
    orient: str = ORIENT_DICT,
    *,
    order_by: Any = _UNSET,
    offset: Any = _UNSET,
    limit: Any = _UNSET,
) -> Any:
    ...
    if isinstance(self, View):
        order_by = self._order_by if order_by is _UNSET else order_by
        offset = self._offset if offset is _UNSET else offset
        limit = (self._limit if self._limit is not None else 1000) if limit is _UNSET else limit
    else:
        order_by = None if order_by is _UNSET else order_by
        offset = None if offset is _UNSET else offset
        limit = 1000 if limit is _UNSET else limit
```

Use the sentinel version — it's the only way to distinguish "caller passed `limit=None`" from "caller omitted `limit`".

- [ ] **Step 4: Update `data_extraction` helpers to accept the kwargs**

In `aaiclick/data/object/data_extraction.py`:

```python
async def extract_array_data(obj, *, order_by=None, offset=None, limit=None):
    sql = f"SELECT value FROM {obj.table}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    if offset is not None:
        sql += f" OFFSET {int(offset)}"
    result = await obj.ch_client.query(sql)
    return [r[0] for r in result.result_rows]


async def extract_dict_data(obj, column_names, columns, orient, *, order_by=None, offset=None, limit=None):
    select = ", ".join(quote_identifier(c) for c in column_names)
    sql = f"SELECT {select} FROM {obj.table}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    if offset is not None:
        sql += f" OFFSET {int(offset)}"
    result = await obj.ch_client.query(sql)
    # existing orient-based formatting below — unchanged
    ...
```

Preserve whatever the existing `extract_*` helpers already do; just thread `order_by` / `offset` / `limit` into the SQL. Scalar extraction is unchanged — it's always a single-row query.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest aaiclick/data/object/test_data.py -v`
Expected: all eight tests PASS.

- [ ] **Step 6: Run the full object suite**

Run: `pytest aaiclick/data/object/ -v`
Expected: all tests pass. Any existing test that called `.data()` without `order_by` and expected deterministic ordering will flake — fix it to pass `order_by="value"` (or similar), per the new contract.

- [ ] **Step 7: Simplify the Phase-4 arithmetic tests that used the two-step idiom**

The Phase-4 tests in `aaiclick/data/object/test_arithmetic_broadcast.py` used `await result.view(order_by="value").data()` because `.data(order_by=...)` did not yet exist. Now it does — convert each to the single-step form:

```python
# Before (Phase 4):
assert sorted(await result.view(order_by="value").data()) == [11, 22, 33]
# After (Phase 5):
assert await result.data(order_by="value") == [11, 22, 33]
```

This is a mechanical pass — both forms produce identical SQL, but the single-step form matches the docs the refactor will publish in Phase 6.

- [ ] **Step 7: Commit**

```bash
git add aaiclick/data/object/object.py aaiclick/data/object/data_extraction.py aaiclick/data/object/test_data.py
git commit -m "$(cat <<'EOF'
feature: .data() accepts order_by / offset / limit kwargs

- keyword-only order_by, offset, limit; limit=1000 default safety cap.
- when self is a View, unset kwargs fall back to the View's
  _order_by / _offset / _limit; passed kwargs override them.
- order_by=None is allowed on array Objects — determinism is opt-in.
EOF
)"
```

---

## Phase 5 Complete

At this point:

- `await obj.data()` returns up to 1000 rows in arbitrary order — no build-a-View ceremony.
- `await obj.data(order_by="value")` is the new deterministic-read idiom.
- `await obj.data(limit=None)` pulls everything.
- `View` attributes are used when `data()` kwargs are absent; kwargs override when passed.
- All existing `.data()` call sites in `aaiclick/` either pass `order_by` explicitly or are fine with arbitrary order (verify this in Phase 6's grep sweep).

Only docs + lingering `ColumnMeta` / renderer cleanup remain.
