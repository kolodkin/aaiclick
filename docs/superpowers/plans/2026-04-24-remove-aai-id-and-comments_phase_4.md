# Phase 4 — Operator Contract

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Remove every `aai_id` reference from operator SQL and enforce the new contract: cross-table binary elementwise ops between two array Objects require both sides to be `View` instances with `_order_by is not None`. Scalar broadcast, same-table fast path, and aggregations get their `aai_id` references cleaned up.

**Depends on:** Phase 3 (tables no longer have `aai_id`, so any remaining `ORDER BY aai_id` / `SELECT aai_id` in SQL would raise at runtime).

**Unlocks:** Phase 5 (changes to `.data()` are orthogonal but benefit from the same `View(order_by=...)` mental model).

---

## File Structure

| File                                                        | Role                                                                                                  |
|-------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| `aaiclick/data/object/operators.py`                         | Modify — cross-table array⊗array, scalar broadcast, aggregation SQL.                                  |
| `aaiclick/data/object/data_extraction.py`                   | Modify — replace `default_order_by="aai_id"` at lines 94, 110, 132 with `default_order_by=None`; drop `aai_id` filtering at line 137. |
| `aaiclick/orchestration/operators.py`                       | Modify — `order_by="aai_id"` kwargs at lines 124 and 242 become the caller-supplied order (or `None`). |
| `aaiclick/data/object/test_arithmetic_broadcast.py`         | Modify — contract assertion: cross-table without Views raises.                                        |
| `aaiclick/data/object/test_arithmetic_parametrized.py`      | Modify — happy-path: Views with `order_by` produce aligned results.                                   |
| `aaiclick/data/object/test_arithmetic_large.py`             | Modify — keep same-table fast path intact.                                                            |
| `aaiclick/data/object/test_arithmetic_validation.py`        | Modify — the existing validation tests may assert on SQL that contains `aai_id`; refresh accordingly. |
| `aaiclick/data/object/test_aggregation_table.py` (exists)   | Modify — aggregation no longer emits `aai_id`.                                                        |

Verify the exact test-file list first (earlier drafts of this plan referenced a `test_arithmetic.py` that does not exist):

```bash
ls aaiclick/data/object/test_arithmetic*.py aaiclick/data/object/test_aggregation*.py
```

Expected: `test_arithmetic_broadcast.py`, `test_arithmetic_large.py`, `test_arithmetic_parametrized.py`, `test_arithmetic_validation.py`, `test_aggregation_table.py`. No bare `test_arithmetic.py`, no bare `test_aggregation.py`.

---

### Task 4.1: Cross-table `a + b` requires two `View`s with `order_by`

**Files:**
- Modify: `aaiclick/data/object/operators.py` — cross-table array⊗array path (currently lines ~183-194 and ~297-300).
- Modify: `aaiclick/data/object/test_arithmetic_broadcast.py` (or wherever the cross-table tests live).

**Background:** Today the SQL uses `row_number() OVER (ORDER BY aai_id)` on both sides of a FULL OUTER JOIN / INNER JOIN to align rows. Without `aai_id`, we need the caller to declare the order explicitly. The spec requires a structural check at call time: both operands must be `View` and both must have `_order_by is not None`. If either check fails, raise a `TypeError` with the spec's exact wording.

- [ ] **Step 1: Write the failing tests**

**These tests use the project's real `ctx` fixture and the module-level `create_object_from_value` helper — `data_ctx` / `DataContext` do not exist.** The `.data(order_by=...)` kwargs land in Phase 5; until then, use the existing `.view(order_by=...).data()` idiom so Phase 4 stays self-contained.

Append to `aaiclick/data/object/test_arithmetic_broadcast.py`:

```python
import pytest

from aaiclick import create_object_from_value


async def test_cross_table_add_without_views_raises(ctx):
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])
    with pytest.raises(TypeError, match="explicit row order"):
        _ = a + b


async def test_cross_table_add_with_one_view_raises(ctx):
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])
    a_view = a.view(order_by="value")
    with pytest.raises(TypeError, match="explicit row order"):
        _ = a_view + b


async def test_cross_table_add_with_two_views_succeeds(ctx):
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([10, 20, 30])
    result = a.view(order_by="value") + b.view(order_by="value")
    assert await result.view(order_by="value").data() == [11, 22, 33]


async def test_same_table_add_no_views_still_works(ctx):
    a = await create_object_from_value([1, 2, 3])
    result = a + a
    assert sorted(await result.view(order_by="value").data()) == [2, 4, 6]


async def test_scalar_broadcast_no_views_still_works(ctx):
    a = await create_object_from_value([1, 2, 3])
    s = await create_object_from_value(10)
    result = a + s
    assert sorted(await result.view(order_by="value").data()) == [11, 12, 13]
```

Phase 5 will revisit these tests to switch the final assertions from `.view(order_by="value").data()` to `.data(order_by="value")` once that kwarg signature lands.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest aaiclick/data/object/test_arithmetic_broadcast.py -v -k "cross_table or scalar_broadcast or same_table"`
Expected: FAIL — current operator raises at SQL execution because `aai_id` no longer exists; TypeError isn't raised at call time.

- [ ] **Step 3: Add the structural check**

In `aaiclick/data/object/operators.py`, find the cross-table array⊗array helper (it's the one that computes `same_table = (info_a.base_table == info_b.base_table)` and branches on it). Before executing SQL, add:

```python
from aaiclick.data.object.object import View

def _require_explicit_order_for_cross_table(left, right):
    if isinstance(left, View) and isinstance(right, View) \
            and left._order_by is not None and right._order_by is not None:
        return
    raise TypeError(
        "Binary elementwise ops on array Objects from different sources "
        "require an explicit row order. Wrap both sides with "
        ".view(order_by=...) before combining.\n"
        f"  Got: {left!r} + {right!r}"
    )
```

Call `_require_explicit_order_for_cross_table(left, right)` on the cross-table branch only — the same-table fast path and the scalar-broadcast path (`a_is_array != b_is_array`) must continue to work without Views.

If `from aaiclick.data.object.object import View` creates a circular import, declare the helper inside `object.py` itself and call it from operators via the existing Object-method dispatch (grep for where the binary op is actually defined — usually `Object.__add__` etc. in `object.py`).

- [ ] **Step 4: Swap the SQL to use the Views' `order_by`**

Replace the cross-table SQL at lines ~183-194 and ~297-300:

```python
# Before (current):
# SELECT CAST(row_number() OVER (ORDER BY aai_id) AS Nullable(UInt64)) AS rn, ...

# After:
left_order = left._order_by
right_order = right._order_by
# Build the SELECT: left.value, right.value aligned by explicit row order.
await ch_client.command(f"""
    INSERT INTO {temp_table}
    SELECT a.value AS a_value, b.value AS b_value,
           a.present AS a_present, b.present AS b_present
    FROM (
        SELECT CAST(row_number() OVER (ORDER BY {left_order}) AS Nullable(UInt64)) AS rn,
               CAST(value AS Nullable({type_a})) AS value,
               CAST(1 AS Nullable(UInt8)) AS present
        FROM {source_a}
    ) AS a
    FULL OUTER JOIN (
        SELECT CAST(row_number() OVER (ORDER BY {right_order}) AS Nullable(UInt64)) AS rn,
               CAST(value AS Nullable({type_b})) AS value,
               CAST(1 AS Nullable(UInt8)) AS present
        FROM {source_b}
    ) AS b
    ON a.rn = b.rn
""")
```

And similarly for the INNER JOIN variant at ~line 297:

```python
INSERT INTO {result.table} (value)
SELECT {expression} AS value
FROM (SELECT row_number() OVER (ORDER BY {left_order}) AS rn, value FROM {info_a.source}) AS a
INNER JOIN (SELECT row_number() OVER (ORDER BY {right_order}) AS rn, value FROM {info_b.source}) AS b
ON a.rn = b.rn
```

`left_order` / `right_order` are the raw `_order_by` strings the user passed to `.view(order_by=...)`. The existing `View` stores them as-is; no further quoting is required (they're SQL fragments, like the user already uses them elsewhere).

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest aaiclick/data/object/test_arithmetic_broadcast.py -v`
Expected: PASS. Also: `pytest aaiclick/data/object/ -v -k "arithmetic"` to confirm other arithmetic tests still pass.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/data/object/operators.py aaiclick/data/object/test_arithmetic_broadcast.py
git commit -m "$(cat <<'EOF'
feature: cross-table array ops require explicit View(order_by=...)

- detect cross-table binary ops structurally (both sides must be
  View with _order_by set); raise TypeError otherwise.
- swap row_number() OVER (ORDER BY aai_id) for the user-supplied
  order in both FULL OUTER JOIN and INNER JOIN cross-table paths.
EOF
)"
```

---

### Task 4.2: Scalar broadcast drops `aai_id` preservation

**Files:**
- Modify: `aaiclick/data/object/operators.py` — scalar-broadcast branch (currently lines ~308-322).

**Background:** Today the scalar-broadcast path carries `aai_id` through from whichever side is the array, so the result keeps deterministic order. Without `aai_id` we rely on ClickHouse's natural source order (arbitrary but stable within one SELECT). Callers wanting deterministic order apply `.view(order_by=...)` to the result.

- [ ] **Step 1: Modify the SELECT**

Replace the block around lines 308-322:

```python
# Scalar broadcasting (array⊗scalar, scalar⊗array, scalar⊗scalar):
# Cross-join works for all cases. The result carries no explicit order;
# callers wanting deterministic order should .view(order_by=...).
insert_target = f"{result.table} (value)"
select_cols = f"{expression} AS value"

await ch_client.command(f"""
    INSERT INTO {insert_target}
    SELECT {select_cols}
    FROM {info_a.source} AS a, {info_b.source} AS b
""")
```

Remove the branches that varied `select_cols` to preserve `a.aai_id` / `b.aai_id`.

- [ ] **Step 2: Run existing broadcast tests**

Run: `pytest aaiclick/data/object/test_arithmetic_broadcast.py -v`
Expected: still PASS (the tests from Task 4.1 already exercise scalar broadcast).

- [ ] **Step 3: Commit**

```bash
git add aaiclick/data/object/operators.py
git commit -m "cleanup: drop aai_id preservation from scalar-broadcast operator SQL"
```

---

### Task 4.3: Aggregations drop `generateSnowflakeID()`

**Files:**
- Modify: `aaiclick/data/object/operators.py` — aggregation helper (currently lines ~451-462).
- Modify: `aaiclick/data/object/test_aggregation_table.py` (exists — no bare `test_aggregation.py` exists).

**Background:** Aggregations today produce a one-row result table where an `aai_id` column gets a fresh Snowflake ID via `DEFAULT generateSnowflakeID()`. Post-refactor the result table has no `aai_id` column at all — the column list is `(value)` only.

The `INSERT ... SELECT ... FROM ...` already inserts into `(value)` only, so the actual SQL change is minor; the important thing is that `result.table` no longer has an `aai_id` column (guaranteed by Phase 3). Confirm no aggregation test still asserts on `aai_id`.

- [ ] **Step 1: Remove any `generateSnowflakeID()` call from SELECT**

Grep: `rg "generateSnowflakeID" aaiclick/ --type py`
Delete every production-code hit. Keep only the intentional negative-check tests (if any).

- [ ] **Step 2: Confirm aggregation SQL inserts only `(value)`**

Around line 451-462:

```python
if agg_func == "count":
    agg_expr = f"{sql_func}()"
else:
    agg_expr = f"{sql_func}(value)"
insert_query = f"""
INSERT INTO {result.table} (value)
SELECT {agg_expr} AS value
FROM {info.source}
"""
await ch_client.command(insert_query)
```

No change needed to the SQL itself. The comment mentioning `aai_id DEFAULT generateSnowflakeID()` must be deleted — it's now wrong.

- [ ] **Step 3: Update tests**

Delete any `assert "aai_id" in result_columns` lines. Add one negative assertion using the real fixture and helpers:

```python
from aaiclick import create_object_from_value
from aaiclick.data.data_context import get_ch_client


async def test_aggregation_result_has_no_aai_id(ctx):
    a = await create_object_from_value([1, 2, 3, 4])
    total = await a.sum()
    result = await get_ch_client().query(
        f"SELECT name FROM system.columns WHERE table = '{total.table}'"
    )
    names = [r[0] for r in result.result_rows]
    assert "aai_id" not in names
```

- [ ] **Step 4: Run the aggregation tests**

Run: `pytest aaiclick/data/object/ -v -k "agg or sum or count or mean"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/data/object/operators.py aaiclick/data/object/test_aggregation_table.py
git commit -m "cleanup: drop generateSnowflakeID() from aggregation SQL"
```

---

### Task 4.4: `data_extraction.py` — stop defaulting to `ORDER BY aai_id`

**Files:**
- Modify: `aaiclick/data/object/data_extraction.py` — `extract_scalar_data` (line 94), `extract_array_data` (line 110), `extract_dict_data` (line 132), and the `aai_id` output-filter at line 137.

**Background:** Today these extraction helpers call `obj._build_select(columns="value", default_order_by="aai_id")`. After Phase 3 the column is gone, so the default would blow up at query time. Phase 5 will let callers specify `order_by`; for this phase, the default becomes `None` (no ORDER BY), which matches the spec's "determinism is opt-in" contract. Scalar extraction is always a single-row query — dropping the ORDER BY has no effect.

- [ ] **Step 1: Replace the defaults**

In `aaiclick/data/object/data_extraction.py`:

```python
# Line 94 (scalar):
query = obj._build_select(columns="value", default_order_by=None)

# Line 110 (array):
query = obj._build_select(columns="value", default_order_by=None)

# Line 132 (dict):
query = obj._build_select(columns="*", default_order_by=None)
```

Delete the `aai_id` output-filter at line 137 (`output_columns = [name for name in column_names if name != "aai_id"]`) — now that DDL does not produce an `aai_id` column, the filter is a no-op.

Check whether `_build_select` on `Object` / `View` still honours `default_order_by=None` cleanly. If it hard-codes appending `aai_id`, update that too (grep `_build_select`).

- [ ] **Step 2: Run the data-extraction tests**

Run: `pytest aaiclick/data/object/ -v -k "schema or extract or group_by"`
Expected: PASS. Tests that previously depended on stable row order without passing `order_by` now return rows in arbitrary order — update them to wrap in `sorted(...)` or pass `view(order_by=...)`.

- [ ] **Step 3: Commit**

```bash
git add aaiclick/data/object/data_extraction.py
git commit -m "cleanup: drop default_order_by='aai_id' from data extraction helpers"
```

---

### Task 4.5: `orchestration/operators.py` — stop passing `order_by="aai_id"` in `ViewRef`

**Files:**
- Modify: `aaiclick/orchestration/operators.py` — `order_by="aai_id"` kwargs at lines 124 and 242.

**Background:** The orchestration-layer operators build `ViewRef`s that partition a shared table across workers; today the partition ordering is `aai_id`. After Phase 3 the column is gone, so these two sites break at runtime.

- [ ] **Step 1: Inspect both call sites**

```bash
grep -n 'order_by="aai_id"' aaiclick/orchestration/operators.py
```

Expected: two hits, at lines 124 and 242. Read 20 lines of context around each — the surrounding code should know a better `order_by` candidate (a user-supplied sort key, a timestamp column, or the partition column itself).

- [ ] **Step 2: Replace**

If the caller already has an ordering column available, pass it. Otherwise, pass `order_by=None` and add a brief comment noting that the partition is order-indifferent at this layer. Both call sites are in the same orchestration operator path, so a single `order_by` argument threaded from the operator's caller is usually the right fix.

- [ ] **Step 3: Update the execution tests**

`aaiclick/orchestration/execution/test_execution.py:180,191` serializes a `View` with `order_by="aai_id ASC"` — update to match the new default.

- [ ] **Step 4: Run the orchestration tests**

Run: `pytest aaiclick/orchestration/ -v -k "operator or execution"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/operators.py aaiclick/orchestration/execution/test_execution.py
git commit -m "cleanup: drop order_by='aai_id' from orchestration operator ViewRefs"
```

---

### Task 4.6: Final grep sweep — no `aai_id` in production code under `aaiclick/data/` or `aaiclick/orchestration/`

- [ ] **Step 1: Search production code**

Run:

```bash
rg "aai_id" aaiclick/data/ aaiclick/orchestration/ --type py
```

Expected hits (these are fine):

- Negative-check tests (`assert "aai_id" not in ...`).
- `test_create_object_allows_user_column_named_aai_id` from Phase 3.
- Docstrings or comments explicitly referring to the removal (delete them unless they genuinely help a reader).

Unacceptable hits:

- Any string literal in `.py` that puts `aai_id` into SQL.
- Any `"aai_id"` referenced in a column-lookup dict.
- `order_by="aai_id"` anywhere.

Remaining hits outside these two directories (`aaiclick/locks.py`, `aaiclick/oplog/lineage.py`, `aaiclick/ai/agents/prompts.py`, AI-agent tests, `internal_api/test_objects.py:91`) are handled in Phase 6.

- [ ] **Step 2: Remove every stray reference**

For each stray production hit, delete it. If a test references `aai_id` as a plain column name (not as a negative check), update it to use a real user column.

- [ ] **Step 3: Run the full data + operator suite**

Run: `pytest aaiclick/data/ aaiclick/orchestration/ -v`
Expected: all tests pass.

- [ ] **Step 4: Commit if any changes were made**

```bash
git add -A aaiclick/data/ aaiclick/orchestration/
git commit -m "cleanup: remove remaining aai_id references from data + orchestration"
```

---

## Phase 4 Complete

At this point:

- Cross-table `a + b` between array Objects raises `TypeError` unless both operands are `View(order_by=...)`.
- Same-table fast path and scalar broadcast still work with plain Objects.
- Aggregation result tables contain only the columns their schema specifies — no `aai_id`.
- No production code in `aaiclick/data/` or `aaiclick/orchestration/` references `aai_id` in SQL or as an `order_by` argument.
- `data_extraction.py` defaults `order_by=None` — callers opt into determinism.

The `.data()` API still uses the pre-refactor signature — Phase 5 adds the new kwargs. The remaining `aai_id` references in `aaiclick/locks.py`, `aaiclick/oplog/lineage.py`, `aaiclick/ai/agents/prompts.py`, and the AI-agent / internal-api test files are cleared in Phase 6.
