# LazyOperator Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `LazyOperator(Object)` class returned by all 26 binary dunders on `Object`, with `.as_(name, scope=...)` to control the result table name and lifetime. Materialize on `await`; chained operations write one table per node.

**Architecture:** `a + b` (and all other binary dunders) returns a `LazyOperator` synchronously holding `(lhs, rhs, operator, schema_preview, name=None, scope=None)`. `await` triggers `_materialize()` which recursively materializes any `LazyOperator` operands first (each as its own table), then calls the existing `_apply_operator_db` with the new `name`/`scope` kwargs forwarded to `create_object`. Sync `.table` raises until materialized; async methods (`.data()`, `.result()`, `.markdown()`, `.export()`) auto-materialize. `LazyOperator` is co-located in `aaiclick/data/object/object.py` to avoid a circular import with `Object`.

**Tech Stack:** Python 3.12, pytest-asyncio, ClickHouse via `aaiclick.data.data_context`, pydantic `Schema` models.

**Spec:** `docs/lazy_operator.md`

---

## File Map

| Path | Action | Responsibility |
|------|--------|----------------|
| `aaiclick/data/object/operators.py` | Modify | Add `_preview_operator_schema()`, `_peek_schema()`; add `name`/`scope` kwargs to `_apply_operator_db()`. |
| `aaiclick/data/object/object.py` | Modify | Add `LazyOperator` class; replace `_apply_operator`/`_apply_operator_reverse` with sync `_plan_operator`/`_plan_operator_reverse`; convert 26 binary dunders to sync. |
| `aaiclick/data/object/__init__.py` | Modify | Export `LazyOperator`. |
| `aaiclick/__init__.py` | Modify | Export `LazyOperator` as public API. |
| `aaiclick/data/object/test_lazy_operator.py` | Create | All 12 tests from spec section "Testing". |

---

## Task 1: Add `name` / `scope` kwargs to `_apply_operator_db`

Smallest standalone change — proves the plumbing carries through to `create_object` before any LazyOperator code exists. The 4 SQL paths inside `_apply_operator_db` are untouched; only the `create_object(schema)` call gains kwargs.

**Files:**
- Modify: `aaiclick/data/object/operators.py:261-307`
- Test: `aaiclick/data/object/test_lazy_operator.py` (new)

- [ ] **Step 1: Write the failing test**

Create the test file with two tests that drive `_apply_operator_db` directly with `name`/`scope`, asserting the result table follows the scoped naming convention.

```python
# aaiclick/data/object/test_lazy_operator.py
"""Tests for the LazyOperator class and named operator results.

Spec: docs/lazy_operator.md
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.object import operators


async def test_apply_operator_db_with_name_uses_temp_named_scope(ctx):
    """name='foo' (no scope) → temp_named table prefix t_foo_<id>."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([10, 20, 30], aai_id=True)
    result = await operators._apply_operator_db(
        obj_a._get_query_info(),
        obj_b._get_query_info(),
        "+",
        obj_a.ch_client,
        name="foo",
    )
    assert result.table.startswith("t_foo_")
    assert await result.data() == [11, 22, 33]


async def test_apply_operator_db_with_name_and_scope_job(ctx):
    """name='bar', scope='job' → j_<job_id>_bar table."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([10, 20, 30], aai_id=True)
    result = await operators._apply_operator_db(
        obj_a._get_query_info(),
        obj_b._get_query_info(),
        "+",
        obj_a.ch_client,
        name="bar",
        scope="job",
    )
    assert result.table.startswith("j_")
    assert result.table.endswith("_bar")
    assert result.persistent is True
    assert await result.data() == [11, 22, 33]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v`

Expected: FAIL with `TypeError: _apply_operator_db() got an unexpected keyword argument 'name'`.

- [ ] **Step 3: Add `name` and `scope` kwargs to `_apply_operator_db`**

In `aaiclick/data/object/operators.py`, change the signature and the single `create_object` call inside:

```python
# Add to imports near the top of operators.py (if not already present)
from ..scope import NamedScope


async def _apply_operator_db(
    info_a: QueryInfo,
    info_b: QueryInfo,
    operator: str,
    ch_client,
    *,
    name: str | None = None,
    scope: NamedScope | None = None,
):
    """
    Apply an operator on two tables at the database level.

    Args:
        info_a: QueryInfo for first operand (contains source, fieldtype, value_type)
        info_b: QueryInfo for second operand (contains source, fieldtype, value_type)
        operator: Operator symbol (e.g., '+', '-', '**', '==', '&')
        ch_client: ClickHouse client instance
        name: Optional result table name (forwarded to ``create_object``).
        scope: Optional result table scope (forwarded to ``create_object``).
              Defaults to ``"temp_named"`` when ``name`` is set.

    Returns:
        New Object instance pointing to result table
    """
```

Then locate the single line that builds the result:

```python
    schema = Schema(fieldtype=fieldtype, columns=result_columns)
    result = await create_object(schema)
```

Change the second line to:

```python
    result = await create_object(schema, name=name, scope=scope)
```

Leave the rest of the function body unchanged.

- [ ] **Step 4: Verify imports are present**

`NamedScope` is defined in `aaiclick/data/scope.py:25`. Confirm `from ..scope import NamedScope` exists at the top of `operators.py`. If not, add it next to existing relative imports.

Run: `grep -n "from ..scope\|NamedScope" aaiclick/data/object/operators.py`

Expected: One line importing `NamedScope` is present.

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v`

Expected: both tests PASS.

Also run the existing operator suite to confirm nothing regressed:

Run: `pytest aaiclick/data/object/test_arithmetic_parametrized.py -v`

Expected: all existing tests still PASS (kwargs default to None → behaviour identical).

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/object/operators.py aaiclick/data/object/test_lazy_operator.py
git commit -m "feat: add name/scope kwargs to _apply_operator_db"
```

---

## Task 2: Pure-Python schema preview helper

Add `_preview_operator_schema(schema_a, schema_b, operator) -> Schema` and `_peek_schema(value) -> Schema`. These mirror the schema-computation block at the top of `_apply_operator_db` (operators.py:275-306) but run sync, with no DB call. Tested with a parametrized assertion that the preview equals the post-materialize schema for every operator × fieldtype combination.

**Files:**
- Modify: `aaiclick/data/object/operators.py`
- Test: `aaiclick/data/object/test_lazy_operator.py`

- [ ] **Step 1: Write the failing test**

Append to `test_lazy_operator.py`:

```python
# Add near top with other imports
from aaiclick.data.models import (
    AAI_ID_COLUMN,
    FIELDTYPE_ARRAY,
    FIELDTYPE_SCALAR,
    ColumnInfo,
    Schema,
)
from aaiclick.data.object.operators import (
    _peek_schema,
    _preview_operator_schema,
)


BINARY_OPERATORS = ["+", "-", "*", "/", "//", "%", "**",
                    "==", "!=", "<", "<=", ">", ">=",
                    "&", "|", "^"]


@pytest.mark.parametrize("operator", BINARY_OPERATORS)
async def test_preview_matches_materialized_schema_array_array(ctx, operator):
    """Pre-materialize schema preview must match the schema of the materialized result."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)

    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, operator)
    materialized = await operators._apply_operator_db(
        obj_a._get_query_info(),
        obj_b._get_query_info(),
        operator,
        obj_a.ch_client,
    )

    # Compare the bits that the preview is responsible for: fieldtype + columns.
    # Table name and engine are set by create_object and are not part of preview.
    assert preview.fieldtype == materialized.schema.fieldtype
    assert set(preview.columns.keys()) == set(materialized.schema.columns.keys())
    for col_name in preview.columns:
        assert preview.columns[col_name].type == materialized.schema.columns[col_name].type
        assert preview.columns[col_name].nullable == materialized.schema.columns[col_name].nullable


@pytest.mark.parametrize("operator", BINARY_OPERATORS)
async def test_preview_matches_materialized_schema_array_scalar(ctx, operator):
    """Scalar broadcast must produce a matching preview schema."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value(7)

    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, operator)
    materialized = await operators._apply_operator_db(
        obj_a._get_query_info(),
        obj_b._get_query_info(),
        operator,
        obj_a.ch_client,
    )

    assert preview.fieldtype == materialized.schema.fieldtype
    for col_name in preview.columns:
        assert preview.columns[col_name].type == materialized.schema.columns[col_name].type


def test_peek_schema_python_int():
    schema = _peek_schema(7)
    assert schema.fieldtype == FIELDTYPE_SCALAR
    assert schema.columns["value"].type == "Int64"


def test_peek_schema_python_float():
    schema = _peek_schema(3.14)
    assert schema.fieldtype == FIELDTYPE_SCALAR
    assert schema.columns["value"].type == "Float64"


async def test_peek_schema_existing_object_returns_its_schema(ctx):
    """For an Object, _peek_schema just returns .schema unchanged."""
    obj = await create_object_from_value([1, 2, 3], aai_id=True)
    assert _peek_schema(obj) is obj.schema
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v -k preview_matches`

Expected: FAIL with `ImportError: cannot import name '_preview_operator_schema' from 'aaiclick.data.object.operators'`.

- [ ] **Step 3: Implement `_peek_schema` and `_preview_operator_schema`**

Open `aaiclick/data/object/operators.py`. Find the existing import block and confirm these names are already imported (they are, used elsewhere in the file): `Schema`, `ColumnInfo`, `FIELDTYPE_ARRAY`, `FIELDTYPE_SCALAR`, `AAI_ID_COLUMN`, `_promote_arithmetic_type`.

Add the new helpers immediately above `_apply_operator_db`:

```python
def _peek_schema(value):
    """Return a Schema for an Object, LazyOperator, or Python scalar.

    Used by LazyOperator planners to compute the result schema synchronously
    without materializing the operand. For Python scalars, defers to
    ``_infer_clickhouse_type`` — the same routine ``create_object_from_value``
    uses — so previews can't drift from materialized scalars.
    """
    from .object import Object  # circular: operators ↔ object; restructuring deferred.
    from ..data_context.data_context import _infer_clickhouse_type

    if isinstance(value, Object):
        return value.schema

    col_info = _infer_clickhouse_type(value)
    return Schema(
        fieldtype=FIELDTYPE_SCALAR,
        columns={"value": col_info},
    )


def _preview_operator_schema(schema_a: Schema, schema_b: Schema, operator: str) -> Schema:
    """Sync preview of the Schema that ``_apply_operator_db`` will produce.

    Mirrors the schema-computation block in ``_apply_operator_db`` (lines 277-306):
    fieldtype promotion (array if either operand is array), type promotion via
    ``_promote_arithmetic_type``, nullable propagation, and aai_id propagation
    from whichever operand is array. No DB call.
    """
    a_is_array = schema_a.fieldtype == FIELDTYPE_ARRAY
    b_is_array = schema_b.fieldtype == FIELDTYPE_ARRAY
    fieldtype = FIELDTYPE_ARRAY if (a_is_array or b_is_array) else FIELDTYPE_SCALAR

    col_a = schema_a.columns.get("value")
    col_b = schema_b.columns.get("value")
    type_a = col_a.type if col_a is not None else "Float64"
    type_b = col_b.type if col_b is not None else "Float64"
    value_type = _promote_arithmetic_type(operator, type_a, type_b)

    nullable_a = col_a.nullable if col_a is not None else False
    nullable_b = col_b.nullable if col_b is not None else False
    result_nullable = nullable_a or nullable_b

    result_columns: dict[str, ColumnInfo] = {
        "value": ColumnInfo(type=value_type, nullable=result_nullable),
    }

    # aai_id propagation: LHS-preferred when both arrays, else from whichever side is array.
    aai_id_source = None
    if a_is_array and schema_a.columns.get(AAI_ID_COLUMN) is not None:
        aai_id_source = schema_a.columns[AAI_ID_COLUMN]
    elif b_is_array and schema_b.columns.get(AAI_ID_COLUMN) is not None:
        aai_id_source = schema_b.columns[AAI_ID_COLUMN]

    if aai_id_source is not None:
        # Mirror the materialize-time `model_copy(update={"default": None})`.
        result_columns[AAI_ID_COLUMN] = aai_id_source.model_copy(update={"default": None})

    return Schema(fieldtype=fieldtype, columns=result_columns)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v`

Expected: all preview / peek tests PASS. Earlier name/scope tests still PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/object/operators.py aaiclick/data/object/test_lazy_operator.py
git commit -m "feat: add sync schema-preview helpers for LazyOperator"
```

---

## Task 3: `LazyOperator` skeleton — fields, `.table` raises

Add the `LazyOperator` class to `aaiclick/data/object/object.py`. Skeleton covers `__init__`, the three operation fields (`lhs`, `rhs`, `operator`), the planned `_name`/`_scope`, and the `.table` property that raises. No `__await__` or `.as_()` yet — added in subsequent tasks.

**Files:**
- Modify: `aaiclick/data/object/object.py`
- Test: `aaiclick/data/object/test_lazy_operator.py`

- [ ] **Step 1: Write the failing test**

Append to `test_lazy_operator.py`:

```python
from aaiclick.data.object import LazyOperator


async def test_lazy_operator_holds_lhs_rhs_operator(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    schema_preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=schema_preview)

    assert lazy.lhs is obj_a
    assert lazy.rhs is obj_b
    assert lazy.operator == "+"
    assert lazy.schema.fieldtype == FIELDTYPE_ARRAY


async def test_lazy_operator_table_raises_before_materialize(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    with pytest.raises(RuntimeError, match="not.*materialized"):
        lazy.table
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v -k lazy_operator`

Expected: FAIL with `ImportError: cannot import name 'LazyOperator' from 'aaiclick.data.object'`.

- [ ] **Step 3: Add `LazyOperator` class to `object.py`**

Open `aaiclick/data/object/object.py`. Locate the end of the file (after `class View`, around line 2910). Append the new class:

```python
class LazyOperator(Object):
    """A planned binary operator that materializes into an Object on ``await``.

    Created synchronously by ``Object`` binary dunders (``__add__`` etc.); no
    ClickHouse round-trip happens until the LazyOperator is awaited. The result
    table name and lifetime can be controlled via ``.as_(name, scope=...)``.

    Spec: docs/lazy_operator.md

    Fields:
        lhs: Left operand — Object, LazyOperator, or Python scalar.
        rhs: Right operand — Object, LazyOperator, Python scalar, or None.
             ``None`` is reserved for future unary/aggregation operators.
        operator: Operator symbol (e.g. "+", "==", "&").
    """

    def __init__(
        self,
        lhs,
        rhs,
        operator: str,
        schema_preview: Schema,
    ):
        # NB: do NOT call Object.__init__ — we have no table yet. Set up the
        # Object-subclass invariants manually so inherited methods that touch
        # _stale / _registered / _owns_lifecycle_ref behave correctly.
        self.lhs = lhs
        self.rhs = rhs
        self.operator = operator
        self._schema = schema_preview
        self._name: str | None = None
        self._scope = None  # NamedScope when set via .as_()
        self._materialized: Object | None = None
        self._stale = False
        self._registered = False
        self._owns_lifecycle_ref = False

    @property
    def table(self) -> str:
        """Table name of the materialized result.

        Raises:
            RuntimeError: When the LazyOperator has not been awaited yet.
        """
        if self._materialized is None:
            raise RuntimeError(
                "LazyOperator has no table yet — `await` it before reading .table. "
                "Async methods like .data() auto-materialize; only sync table-access "
                "requires an explicit await."
            )
        return self._materialized.table

    def __repr__(self) -> str:
        if self._materialized is not None:
            return f"LazyOperator(materialized={self._materialized.table!r})"
        return f"LazyOperator(op={self.operator!r}, materialized=False)"
```

- [ ] **Step 4: Export `LazyOperator` from the subpackage**

In `aaiclick/data/object/__init__.py`, add `LazyOperator` to the import:

```python
from .object import DataResult, GroupByQuery, LazyOperator, Object, View
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v -k lazy_operator`

Expected: both tests PASS.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/object/object.py aaiclick/data/object/__init__.py aaiclick/data/object/test_lazy_operator.py
git commit -m "feat: add LazyOperator skeleton with table-access guard"
```

---

## Task 4: `.as_(name, scope)` method

Add `.as_()` to `LazyOperator`. Returns a *new* LazyOperator with `_name`/`_scope` set. Immutable — receiver is unchanged.

**Files:**
- Modify: `aaiclick/data/object/object.py`
- Test: `aaiclick/data/object/test_lazy_operator.py`

- [ ] **Step 1: Write the failing test**

Append to `test_lazy_operator.py`:

```python
async def test_as_returns_new_lazy_with_name(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    named = lazy.as_("daily_total")

    assert named is not lazy
    assert named._name == "daily_total"
    assert named._scope == "temp_named"
    # Receiver unchanged.
    assert lazy._name is None
    assert lazy._scope is None


async def test_as_with_explicit_scope(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    job_scoped = lazy.as_("yearly", scope="job")
    assert job_scoped._name == "yearly"
    assert job_scoped._scope == "job"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v -k as_`

Expected: FAIL with `AttributeError: 'LazyOperator' object has no attribute 'as_'`.

- [ ] **Step 3: Implement `.as_()`**

In `aaiclick/data/object/object.py`, inside the `LazyOperator` class, add the method below `__init__`:

```python
    def as_(self, name: str, scope: str = "temp_named") -> "LazyOperator":
        """Return a new LazyOperator that materializes with the given name and scope.

        Args:
            name: Result table name (forwarded to ``create_object``).
            scope: ``"temp_named"`` (default), ``"job"``, or ``"global"`` — see
                ``create_object`` for the lifetime semantics of each.

        Returns:
            New LazyOperator instance. The receiver is unchanged.
        """
        new = LazyOperator(
            lhs=self.lhs,
            rhs=self.rhs,
            operator=self.operator,
            schema_preview=self._schema,
        )
        new._name = name
        new._scope = scope
        return new
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v -k as_`

Expected: both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/object/object.py aaiclick/data/object/test_lazy_operator.py
git commit -m "feat: add LazyOperator.as_() for naming the materialized result"
```

---

## Task 5: `__await__` materialization

Add `_materialize()` (async) and `__await__` to `LazyOperator`. Walks `lhs` / `rhs`, materializing inner `LazyOperator`s recursively, then calls `_apply_operator_db` with the captured `_name`/`_scope`. Caches the resulting `Object` on `self._materialized` so re-await is idempotent.

**Files:**
- Modify: `aaiclick/data/object/object.py`
- Test: `aaiclick/data/object/test_lazy_operator.py`

- [ ] **Step 1: Write the failing tests**

Append to `test_lazy_operator.py`:

```python
async def test_await_unnamed_lazy_materializes_to_temp(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    result = await lazy
    assert result.table.startswith("t_")
    # Unnamed: should NOT have the temp_named prefix (which is t_<name>_<id>).
    # An unnamed temp is t_<id> with a single underscore segment after t_.
    assert result.scope == "temp"
    assert await result.data() == [5, 7, 9]


async def test_await_with_as_temp_named(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+",
                       schema_preview=preview).as_("daily_total")

    result = await lazy
    assert result.table.startswith("t_daily_total_")
    assert result.scope == "temp_named"
    assert await result.data() == [5, 7, 9]


async def test_await_with_scope_job(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+",
                       schema_preview=preview).as_("yearly", scope="job")

    result = await lazy
    assert result.table.startswith("j_")
    assert result.table.endswith("_yearly")
    assert result.persistent is True
    assert await result.data() == [5, 7, 9]


async def test_re_await_is_idempotent(ctx):
    """Awaiting the same LazyOperator twice returns the same Object — no second table."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    first = await lazy
    second = await lazy
    assert first is second


async def test_chain_two_lazies_writes_two_tables(ctx):
    """`(a + b) + c` materializes inner then outer — two separate tables."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([10, 20, 30], aai_id=True)
    obj_c = await create_object_from_value([100, 200, 300], aai_id=True)

    inner_preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    inner = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=inner_preview)

    outer_preview = _preview_operator_schema(inner.schema, obj_c.schema, "+")
    outer = LazyOperator(lhs=inner, rhs=obj_c, operator="+",
                        schema_preview=outer_preview).as_("grand_total")

    result = await outer
    assert result.table.startswith("t_grand_total_")
    assert await result.data() == [111, 222, 333]
    # Inner was materialized too.
    assert inner._materialized is not None
    assert inner._materialized.table != result.table


async def test_lazy_never_awaited_creates_no_table(ctx):
    """Building a LazyOperator without awaiting writes no rows to table_registry."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")

    # Snapshot table count via system.tables, then build a lazy and discard.
    before = await obj_a.ch_client.query("SELECT count() FROM system.tables WHERE name LIKE 't_%'")
    _ = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)
    after = await obj_a.ch_client.query("SELECT count() FROM system.tables WHERE name LIKE 't_%'")

    assert before.result_rows[0][0] == after.result_rows[0][0]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v -k "await_ or chain_two or never_awaited or re_await"`

Expected: FAIL — `TypeError: object LazyOperator can't be used in 'await' expression` (no `__await__` defined).

- [ ] **Step 3: Implement `_materialize` and `__await__`**

In `aaiclick/data/object/object.py`, inside the `LazyOperator` class, add the following methods below `as_`. Place the helper `_resolve_operand` at module level above `LazyOperator`:

```python
# Module-level helper, placed above the LazyOperator class definition.
async def _resolve_operand(value):
    """Resolve an operand to a materialized Object.

    - LazyOperator → materialize and unwrap.
    - Object → return as-is.
    - Python scalar → ``Object._ensure_object``.
    - None → raise (reserved for future unary ops; phase 1 always has both).
    """
    if value is None:
        raise RuntimeError("LazyOperator phase 1 requires both lhs and rhs to be non-None")
    if isinstance(value, LazyOperator):
        return await value._materialize()
    if isinstance(value, Object):
        return value
    return await Object._ensure_object(value)
```

Inside `LazyOperator`:

```python
    async def _materialize(self) -> Object:
        """Walk lhs/rhs and produce the materialized Object. Cached on _materialized."""
        if self._materialized is not None:
            return self._materialized

        lhs_obj = await _resolve_operand(self.lhs)
        rhs_obj = await _resolve_operand(self.rhs)
        lhs_obj.checkstale()
        rhs_obj.checkstale()
        _require_explicit_order_for_cross_table(lhs_obj, rhs_obj)

        info_a = lhs_obj._get_query_info()
        info_b = rhs_obj._get_query_info()
        result = await operators._apply_operator_db(
            info_a, info_b, self.operator,
            get_ch_client(),
            name=self._name,
            scope=self._scope,
        )
        self._materialized = result
        return result

    def __await__(self):
        return self._materialize().__await__()
```

The `get_ch_client` is already imported at the top of `object.py`. `operators` is already imported (`from . import data_extraction, ingest, operators`). `_require_explicit_order_for_cross_table` is a module-level function above `class Object`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v -k "await_ or chain_two or never_awaited or re_await"`

Expected: all 6 tests PASS.

Also re-run prior tests to make sure they still pass:

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v`

Expected: all current tests PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/object/object.py aaiclick/data/object/test_lazy_operator.py
git commit -m "feat: LazyOperator materializes on await with idempotent caching"
```

---

## Task 6: Async methods auto-materialize

Override `data()`, `result()`, `markdown()`, `export()` on `LazyOperator` to materialize first, then delegate. After materialization, the cached `_materialized` Object is reused on subsequent calls.

**Files:**
- Modify: `aaiclick/data/object/object.py`
- Test: `aaiclick/data/object/test_lazy_operator.py`

- [ ] **Step 1: Write the failing test**

Append to `test_lazy_operator.py`:

```python
async def test_data_auto_materializes(ctx):
    """Calling .data() on an unawaited lazy materializes and returns rows."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    rows = await lazy.data()
    assert rows == [5, 7, 9]
    # And only one table was created.
    assert lazy._materialized is not None
    # Second call reuses cache.
    rows_again = await lazy.data()
    assert rows_again == [5, 7, 9]


async def test_result_auto_materializes(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    preview = _preview_operator_schema(obj_a.schema, obj_b.schema, "+")
    lazy = LazyOperator(lhs=obj_a, rhs=obj_b, operator="+", schema_preview=preview)

    result = await lazy.result()
    # .result() returns the raw QueryResult; assert structure exists.
    assert result is not None
    assert lazy._materialized is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v -k auto_materializes`

Expected: At minimum, `.data()` may currently fail because the inherited `Object.data()` reads `self.table` which raises. Expected error: `RuntimeError: LazyOperator has no table yet`.

- [ ] **Step 3: Override async methods on `LazyOperator`**

In `aaiclick/data/object/object.py`, inside `LazyOperator`, add below `__await__`:

```python
    # Async-method overrides: each first materializes (cached), then delegates.
    # Sync properties (.table, .scope) still raise pre-materialize — only async
    # methods auto-materialize, because only they can `await`.

    async def data(self, *args, **kwargs):
        obj = await self._materialize()
        return await obj.data(*args, **kwargs)

    async def result(self, *args, **kwargs):
        obj = await self._materialize()
        return await obj.result(*args, **kwargs)

    async def markdown(self, *args, **kwargs):
        obj = await self._materialize()
        return await obj.markdown(*args, **kwargs)

    async def export(self, *args, **kwargs):
        obj = await self._materialize()
        return await obj.export(*args, **kwargs)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v -k auto_materializes`

Expected: both tests PASS.

Re-run the whole test_lazy_operator file:

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v`

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/object/object.py aaiclick/data/object/test_lazy_operator.py
git commit -m "feat: LazyOperator async methods auto-materialize once and delegate"
```

---

## Task 7: Wire all 26 binary dunders to return `LazyOperator`

Convert every binary dunder (object.py:710-812) from `async def → Object` to sync `def → LazyOperator`. Replace `_apply_operator` and `_apply_operator_reverse` with sync `_plan_operator` and `_plan_operator_reverse`. The existing test suites (`test_arithmetic_parametrized.py`, `test_comparison.py`, `test_bitwise.py`, etc.) — which already use `await (a + b)` — serve as the regression coverage; LazyOperator's `__await__` keeps them green.

**Files:**
- Modify: `aaiclick/data/object/object.py:667-812`
- Test: `aaiclick/data/object/test_lazy_operator.py`

- [ ] **Step 1: Write the failing tests**

Append to `test_lazy_operator.py` — assertions that the operators now return `LazyOperator` instances synchronously, including for reverse operators with naming, plus a regression test using the public `+` syntax instead of constructing LazyOperator manually:

```python
async def test_add_returns_lazy_operator(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    lazy = obj_a + obj_b
    assert isinstance(lazy, LazyOperator)
    assert await lazy.data() == [5, 7, 9]


async def test_public_as_named_temp(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
    result = await (obj_a + obj_b).as_("daily_total")
    assert result.table.startswith("t_daily_total_")
    assert await result.data() == [5, 7, 9]


async def test_public_as_scope_global(ctx):
    from aaiclick import delete_persistent_object

    # Pre-clean: a previous failed run could have left p_yearly_avg behind.
    # CREATE TABLE IF NOT EXISTS would otherwise INSERT into a pre-existing table.
    await delete_persistent_object("yearly_avg", scope="global")
    try:
        obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
        obj_b = await create_object_from_value([4, 5, 6], aai_id=True)
        result = await (obj_a + obj_b).as_("yearly_avg", scope="global")
        assert result.table == "p_yearly_avg"
        assert result.persistent is True
        assert await result.data() == [5, 7, 9]
    finally:
        await delete_persistent_object("yearly_avg", scope="global")


async def test_reverse_op_with_naming(ctx):
    """`(2 + a).as_('foo')` goes through __radd__; result is named."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    result = await (2 + obj_a).as_("rfoo")
    assert result.table.startswith("t_rfoo_")
    assert await result.data() == [3, 4, 5]


async def test_chain_via_public_operator(ctx):
    """(a + b) + c via public syntax → 2 tables, outer named."""
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([10, 20, 30], aai_id=True)
    obj_c = await create_object_from_value([100, 200, 300], aai_id=True)

    chain = (obj_a + obj_b) + obj_c
    assert isinstance(chain, LazyOperator)
    assert isinstance(chain.lhs, LazyOperator)
    assert chain.rhs is obj_c

    result = await chain.as_("total")
    assert result.table.startswith("t_total_")
    assert await result.data() == [111, 222, 333]


async def test_comparison_returns_lazy(ctx):
    obj_a = await create_object_from_value([1, 2, 3], aai_id=True)
    obj_b = await create_object_from_value([2, 2, 2], aai_id=True)
    lazy = obj_a < obj_b
    assert isinstance(lazy, LazyOperator)
    assert await lazy.data() == [True, False, False]


async def test_bitwise_returns_lazy(ctx):
    obj_a = await create_object_from_value([5, 6, 7], aai_id=True)
    obj_b = await create_object_from_value([3, 3, 3], aai_id=True)
    lazy = obj_a & obj_b
    assert isinstance(lazy, LazyOperator)
    assert await lazy.data() == [1, 2, 3]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v -k "returns_lazy or public_as or reverse_op_with_naming or chain_via_public"`

Expected: FAIL. Most likely error: `TypeError: object coroutine can't be used as ...` or `AttributeError: 'coroutine' object has no attribute 'as_'` — because today `obj_a + obj_b` returns a coroutine, not a LazyOperator.

- [ ] **Step 3: Replace `_apply_operator` and `_apply_operator_reverse` with sync planners**

In `aaiclick/data/object/object.py`, find `_apply_operator` (around line 667) and `_apply_operator_reverse` (around line 689). Replace them with sync planners.

Delete lines 667-708 (both old methods) and replace with:

```python
    def _plan_operator(self, other, operator: str) -> "LazyOperator":
        """Synchronously plan ``self op other`` — returns a LazyOperator that
        materializes on await. Schema is precomputed; no DB call.
        """
        self.checkstale()
        from .operators import _peek_schema, _preview_operator_schema
        schema_preview = _preview_operator_schema(
            self._schema, _peek_schema(other), operator,
        )
        return LazyOperator(
            lhs=self, rhs=other, operator=operator,
            schema_preview=schema_preview,
        )

    def _plan_operator_reverse(self, other, operator: str) -> "LazyOperator":
        """Synchronously plan ``other op self`` — used by __radd__ etc."""
        self.checkstale()
        from .operators import _peek_schema, _preview_operator_schema
        other_schema = _peek_schema(other)
        schema_preview = _preview_operator_schema(
            other_schema, self._schema, operator,
        )
        return LazyOperator(
            lhs=other, rhs=self, operator=operator,
            schema_preview=schema_preview,
        )
```

The two `from .operators import ...` are inline imports — explicitly allowed here because `operators.py` imports `Object` indirectly (via `_peek_schema`'s inline `from .object import Object`), so a top-of-file import creates a cycle. CLAUDE.md permits inline imports as last resort with a comment noting why; add this comment line above each `from .operators` line:

```python
        # Circular dep: operators.py imports Object; restructuring would require
        # extracting Object's base interface to a neutral module.
```

- [ ] **Step 4: Convert all 26 binary dunders to sync**

Below the planners, the 26 binary dunders begin around line 710. Replace each `async def` with sync `def`, removing the `await` and returning the LazyOperator directly. Here's the **arithmetic** group — apply the same pattern to all 26:

```python
    def __add__(self, other) -> "LazyOperator":
        """Add: self + other. Supports scalar broadcast. Returns a LazyOperator
        that materializes on ``await``."""
        return self._plan_operator(other, "+")

    def __radd__(self, other) -> "LazyOperator":
        return self._plan_operator_reverse(other, "+")

    def __sub__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "-")

    def __rsub__(self, other) -> "LazyOperator":
        return self._plan_operator_reverse(other, "-")

    def __mul__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "*")

    def __rmul__(self, other) -> "LazyOperator":
        return self._plan_operator_reverse(other, "*")

    def __truediv__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "/")

    def __rtruediv__(self, other) -> "LazyOperator":
        return self._plan_operator_reverse(other, "/")

    def __floordiv__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "//")

    def __rfloordiv__(self, other) -> "LazyOperator":
        return self._plan_operator_reverse(other, "//")

    def __mod__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "%")

    def __rmod__(self, other) -> "LazyOperator":
        return self._plan_operator_reverse(other, "%")

    def __pow__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "**")

    def __rpow__(self, other) -> "LazyOperator":
        return self._plan_operator_reverse(other, "**")
```

**Comparison** (no reverse dunders — Python handles symmetry):

```python
    def __eq__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "==")

    def __ne__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "!=")

    def __lt__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "<")

    def __le__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "<=")

    def __gt__(self, other) -> "LazyOperator":
        return self._plan_operator(other, ">")

    def __ge__(self, other) -> "LazyOperator":
        return self._plan_operator(other, ">=")
```

**Bitwise** (forward + reverse):

```python
    def __and__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "&")

    def __rand__(self, other) -> "LazyOperator":
        return self._plan_operator_reverse(other, "&")

    def __or__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "|")

    def __ror__(self, other) -> "LazyOperator":
        return self._plan_operator_reverse(other, "|")

    def __xor__(self, other) -> "LazyOperator":
        return self._plan_operator(other, "^")

    def __rxor__(self, other) -> "LazyOperator":
        return self._plan_operator_reverse(other, "^")
```

Removing the docstrings is fine — the `__add__` docstring acts as the group's reference; the others are self-evident from the operator symbol.

**Important:** `__hash__` becomes `None` when a class defines `__eq__` but not `__hash__`. Today `Object.__eq__` is async, which Python doesn't recognize as a real `__eq__` override (no hash impact). After the change, `__eq__` is a sync method returning a `LazyOperator` — Python *will* zero out `__hash__`, breaking any code that puts an `Object` in a `set` or `dict` key.

To preserve hashability, add a `__hash__` method to `Object` directly above `__eq__`:

```python
    __hash__ = object.__hash__
```

This explicit assignment tells Python to keep the default `id()`-based hash.

- [ ] **Step 5: Run the new tests to verify they pass**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v`

Expected: all tests in `test_lazy_operator.py` PASS.

- [ ] **Step 6: Run the existing operator regression suite**

Run: `pytest aaiclick/data/object/test_arithmetic_parametrized.py aaiclick/data/object/test_arithmetic_broadcast.py aaiclick/data/object/test_arithmetic_validation.py aaiclick/data/object/test_arithmetic_large.py aaiclick/data/object/test_comparison.py aaiclick/data/object/test_bitwise.py -v`

Expected: all existing arithmetic / comparison / bitwise tests still PASS. If anything fails, it's likely a hashability issue, a missing dunder conversion, or a place where someone was relying on the coroutine return shape — fix and re-run.

- [ ] **Step 7: Run the broader Object test suite**

Run: `pytest aaiclick/data/object/ -v -x`

Expected: all tests PASS. `-x` stops on first failure for easier debugging.

- [ ] **Step 8: Commit**

```bash
git add aaiclick/data/object/object.py aaiclick/data/object/test_lazy_operator.py
git commit -m "feat: convert all 26 binary dunders to return LazyOperator

  Each dunder now returns a LazyOperator synchronously, materializing on
  await. Existing call sites `await (a + b)` keep working because
  LazyOperator is awaitable. Adds explicit __hash__ = object.__hash__ on
  Object since Python zeroes the default hash when a class defines its
  own __eq__."
```

---

## Task 8: Public API export + full regression run

Surface `LazyOperator` from `aaiclick` so users can `isinstance(x, LazyOperator)`. Then run the full test suite to catch any cross-module regressions (e.g. orchestration tests that did things with operator results).

**Files:**
- Modify: `aaiclick/__init__.py`

- [ ] **Step 1: Write the failing test**

Append to `test_lazy_operator.py`:

```python
async def test_lazy_operator_is_public_api():
    """LazyOperator is importable from the top-level package."""
    import aaiclick
    assert hasattr(aaiclick, "LazyOperator")
    # And it's the same class as the internal one.
    from aaiclick.data.object import LazyOperator as InternalLazy
    assert aaiclick.LazyOperator is InternalLazy
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/data/object/test_lazy_operator.py -v -k is_public_api`

Expected: FAIL — `AttributeError: module 'aaiclick' has no attribute 'LazyOperator'`.

- [ ] **Step 3: Export from public API**

Edit `aaiclick/__init__.py` — add `LazyOperator` to the import block from `.data`:

Find this section (around line 23):

```python
from .data import (
    FIELDTYPE_ARRAY,
    ...
    Object,
    ...
)
```

And add `LazyOperator` in alphabetical order:

```python
from .data import (
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    FIELDTYPE_SCALAR,
    ORIENT_DICT,
    ORIENT_RECORDS,
    ColumnInfo,
    ColumnType,
    DataResult,
    FieldSpec,
    LazyOperator,
    Object,
    Schema,
    ...
)
```

Confirm `aaiclick/data/__init__.py` re-exports `LazyOperator` too:

Run: `grep -n LazyOperator aaiclick/data/__init__.py`

If absent, add it to the import line in `aaiclick/data/__init__.py` from `.object`.

- [ ] **Step 4: Run the new test**

Run: `pytest aaiclick/data/object/test_lazy_operator.py::test_lazy_operator_is_public_api -v`

Expected: PASS.

- [ ] **Step 5: Run the entire test suite**

Run: `pytest aaiclick/ -x --timeout=120`

Expected: all tests PASS. Any failure → diagnose; most likely a place that introspected `iscoroutine(a + b)` (we grepped — none in the codebase, but external test paths may surprise).

- [ ] **Step 6: Commit**

```bash
git add aaiclick/__init__.py aaiclick/data/__init__.py aaiclick/data/object/test_lazy_operator.py
git commit -m "feat: expose LazyOperator on the public aaiclick API"
```

- [ ] **Step 7: Push and confirm CI**

```bash
git push -u origin claude/operator-table-naming-s9QkL
```

Then use the `check-pr` skill to verify GitHub Actions are green.

---

## Self-Review Notes

**Spec coverage:**
- `LazyOperator(Object)` class with `lhs/rhs/operator` fields → Task 3.
- `.as_(name, scope=...)` returning new instance, non-mutating → Task 4.
- Sync `.table` raises pre-materialize → Task 3.
- Async methods auto-materialize → Task 6.
- `__await__` materializes once; cached → Task 5.
- No DB writes when never awaited → Task 5 (`test_lazy_never_awaited_creates_no_table`).
- Chained `(a + b) + c` produces 2 tables, inner unnamed → Tasks 5 + 7.
- Re-await idempotent → Task 5.
- Reverse op with naming → Task 7.
- `.as_()` non-mutating → Task 4.
- Schema preview matches materialized schema → Task 2 (parametrized over operators).
- `_apply_operator_db` gains `name`/`scope` kwargs → Task 1.
- All 26 binary dunders converted → Task 7.
- Public API export → Task 8.

**Known gotchas implementers should watch for:**
1. `__eq__` removes the default `__hash__` — Task 7 step 4 adds an explicit `__hash__ = object.__hash__`.
2. `operators.py` ↔ `object.py` circular import — Task 7 step 3 uses inline imports inside `_plan_operator` / `_plan_operator_reverse` (and `_peek_schema` already uses one to reach `Object`). Comment explains why restructuring is deferred.
3. The shared-inner idempotency test (`test_re_await_is_idempotent` in Task 5) covers the case where two `await`s of the same lazy don't double-write.
