# Data Layer Dedup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the systematic duplication in the data layer: the ~13 hand-rolled "create result table + INSERT…SELECT + stats + oplog" emitters, the verbatim WHERE/ORDER BY/LIMIT/OFFSET tail duplicated between `Object._build_select` and `View._build_select`, and the four-branch `GroupByQuery._get_group_by_info` that `_effective_columns` already subsumes.

**Architecture:** A new `aaiclick/data/object/emit.py` owns `emit_result(schema, select_sql, ch_client, ...)` — the single create/insert/stats/oplog sequence — used by `operators.py`, `join.py`, and the two `copy_db*` functions in `ingest.py` (concat/insert keep their bespoke advisory-lock flows). In `object.py`, `_build_select` gains an overridable `_select_head()` hook so `View` overrides only the SELECT head (projection/renames/computed/ARRAY JOIN) and the shared tail lives once; `GroupByQuery` derives its column map from `_effective_columns`; the `View._get_ingest_query_info` override collapses by pointing the base implementation at `_effective_columns`.

**Explicitly out of scope:** the full `Object`/`View` class collapse. `View` carries genuinely distinct behavior (projection, renames, computed columns, ARRAY JOIN explosion, lifecycle piggybacking on the source object) — merging it into `Object` is a high-risk public-API rewrite, not a dedup.

**Tech Stack:** Python 3.12, chdb + SQLite local backend, `uv run pytest`.

**Spec:** This plan is its own spec (behavior-preserving refactor; two knowingly-accepted behavior *improvements*: `GroupByQuery` sees correct types for renamed columns instead of a `Float64` fallback, and validates group keys against post-rename names).

## Global Constraints

- All imports at top of file; no `__all__`; no history comments (CLAUDE.md).
- No `Any` shortcuts.
- Run tests with `uv run pytest <paths> -q --no-cov -p no:cacheprovider`.
- Keep `oplog_record_sample` calls exactly where they exist today (some emitters record, some don't) — uniformity changes lineage output, out of scope.

---

### Task 1: `emit.py` + convert `operators.py` emitters

**Files:**
- Create: `aaiclick/data/object/emit.py`
- Modify: `aaiclick/data/object/operators.py`

**Interfaces:**
- Produces:
  ```python
  async def emit_result(
      schema: Schema,
      select_sql: str,
      ch_client,
      *,
      insert_cols: str | None = None,   # None → ", ".join(schema.columns)
      name: str | None = None,
      scope: NamedScope | None = None,
      oplog_op: str | None = None,
      oplog_kwargs: dict | None = None,
  ) -> Object   # returned by create_object; _stats populated
  ```

- [ ] **Step 1: Write `emit.py`:**

```python
"""Shared result-table emission for database-level operations.

Every operator/aggregation/copy helper ends the same way: create the result
table from a schema, run ``INSERT INTO … <select>`` capturing
:class:`QueryStats`, optionally record an oplog sample. That sequence lives
here so the ~13 emitters in ``operators.py`` / ``ingest.py`` / ``join.py``
supply only their schema and SELECT.
"""

from __future__ import annotations

from aaiclick.oplog.oplog_api import oplog_record_sample

from ..data_context import create_object
from ..data_context.ch_client import execute_for_stats
from ..models import Schema
from ..scope import NamedScope


async def emit_result(
    schema: Schema,
    select_sql: str,
    ch_client,
    *,
    insert_cols: str | None = None,
    name: str | None = None,
    scope: NamedScope | None = None,
    oplog_op: str | None = None,
    oplog_kwargs: dict | None = None,
):
    """Create the result table for ``schema`` and fill it from ``select_sql``.

    Args:
        schema: Result table schema (also defines the default insert columns).
        select_sql: Complete ``SELECT …`` statement producing the rows.
        ch_client: ClickHouse client instance.
        insert_cols: Explicit insert column list; defaults to ``schema.columns``.
        name: Optional result table name (forwarded to ``create_object``).
        scope: Optional result table scope (forwarded to ``create_object``).
        oplog_op: When set, record an oplog sample for the result under this op.
        oplog_kwargs: kwargs for the oplog sample.

    Returns:
        The new Object, with ``_stats`` populated from the INSERT.
    """
    result = await create_object(schema, name=name, scope=scope)
    cols = insert_cols if insert_cols is not None else ", ".join(schema.columns)
    result._stats = await execute_for_stats(
        f"INSERT INTO {result.table} ({cols}) {select_sql}", client=ch_client
    )
    if oplog_op is not None:
        oplog_record_sample(result.table, oplog_op, kwargs=oplog_kwargs or {})
    return result
```

  (Return type is left unannotated to avoid importing `Object` — `create_object`'s own return annotation carries it; if `create_object` is typed, annotate the same type.)

- [ ] **Step 2: Convert the `operators.py` emitters** to `emit_result`, preserving each site's exact SELECT and oplog behavior. Sites and their arguments:
  - `_apply_aggregation` → `emit_result(schema, f"SELECT {agg_expr} AS value FROM {info.source}", ch_client, name=name, scope=scope, oplog_op=agg_func, oplog_kwargs={"source": info.base_table})`
  - `count_if_agg` str branch → `oplog_op=None`; dict branch → `insert_cols=", ".join(condition.keys())`
  - `quantile_agg`, `unique_group`, `unary_transform`, `_apply_string_op_db`, `is_null_op`, `is_not_null_op`, `array_map_db` → plain conversions (no oplog today — keep none)
  - `nunique_agg` → `oplog_op="nunique", oplog_kwargs={"source": info.base_table}`
  - `isin_op` → `oplog_op="isin", oplog_kwargs={"source": info.base_table, "other": other_info.base_table}`
  - `group_by_agg` → `insert_cols=insert_cols_str`, select is the existing `query`
  - Leave `_apply_operator_db` and `coalesce_op` as-is (multi-path bodies with temp-table lifecycles and aai_id projection; the quad is not their dominant shape).
  - Note: sites that previously wrote `INSERT INTO {result.table}` with no column list get an explicit single-column `(value)` list — identical semantics, their schemas are single-column.
  - Drop imports that become unused in `operators.py` (`create_object`, `execute_for_stats`, `oplog_record_sample` — verify each; `create_object`/`execute_for_stats` remain used by `_apply_operator_db`/`coalesce_op`).

- [ ] **Step 3: Run tests**

Run: `uv run pytest aaiclick/data -q --no-cov -p no:cacheprovider`
Expected: all pass.

- [ ] **Step 4: Commit** — `refactor: extract shared emit_result for operator result tables`

### Task 2: Convert `join.py` and `ingest.py` copy emitters

**Files:**
- Modify: `aaiclick/data/object/join.py` (`join_objects_db` tail)
- Modify: `aaiclick/data/object/ingest.py` (`copy_db`, `copy_db_selected_fields`)

- [ ] **Step 1:** In `join_objects_db`, replace the `create_object` + `execute_for_stats` + `oplog_record_sample` sequence with one `emit_result(schema, f"SELECT {select_sql_cols} FROM {left.source} AS l {HOW_TO_SQL[how]} {right.source} AS r{on_clause}{settings_clause}", ch_client, insert_cols=insert_cols_sql, name=name, scope=scope, oplog_op="join", oplog_kwargs={...existing dict...})`. Move the `create_object` call site accordingly (the SELECT depends only on `schema`, not `result`).
- [ ] **Step 2:** In `copy_db` / `copy_db_selected_fields`, replace the create+insert+stats pairs with `emit_result(...)` (`insert_cols=cols_str` / `(value)` / `fields_str`). `concat_objects_db` and `insert_objects_db` stay as-is (advisory-lock flows).
- [ ] **Step 3:** Run: `uv run pytest aaiclick/data -q --no-cov -p no:cacheprovider` — all pass.
- [ ] **Step 4: Commit** — `refactor: join and copy emitters use emit_result`

### Task 3: `object.py` — shared SELECT tail via `_select_head` hook

**Files:**
- Modify: `aaiclick/data/object/object.py`

- [ ] **Step 1:** In `Object`, restructure `_build_select` to call a new overridable hook, keeping the public signature and behavior:

```python
    def _select_head(self, columns: str) -> str:
        """SELECT head (projection + FROM, before WHERE). View overrides to
        apply field selection, renames, computed columns, and ARRAY JOIN."""
        return f"SELECT {columns} FROM {self.table}"
```

  and have `_build_select` use `query = self._select_head(columns)` in place of the inline f-string. The WHERE/ORDER BY/LIMIT/OFFSET tail stays exactly as it is.
- [ ] **Step 2:** In `View`, delete the entire `_build_select` override and replace it with a `_select_head(columns)` override containing lines 2574–2643's head logic (select_cols computation, computed columns, `FROM {self.table}`, ARRAY JOIN clause) — ending by returning the query string just before the current `eff_order_by` resolution. Keep the docstring notes about single-field `AS value`, computed/exploded interplay, and aai_id carry-through on the override.
- [ ] **Step 3:** Run: `uv run pytest aaiclick/data -q --no-cov -p no:cacheprovider` — all pass.
- [ ] **Step 4: Commit** — `refactor: share the SELECT constraint tail between Object and View`

### Task 4: `object.py` — `GroupByQuery` and ingest-info collapse

**Files:**
- Modify: `aaiclick/data/object/object.py`

- [ ] **Step 1:** Replace `GroupByQuery._get_group_by_info`'s four-branch body with:

```python
    def _get_group_by_info(self) -> GroupByInfo:
        """Build GroupByInfo from the source Object.

        ``_effective_columns`` already resolves field selection, renames,
        computed columns, and explode depth for any Object or View.
        """
        source = self._source
        source_query = f"({source._build_select()})" if source.has_constraints else source.table
        columns = {name: info.type for name, info in source._effective_columns.items()}
        return GroupByInfo(
            source=source_query,
            base_table=source.table,
            group_keys=self._keys,
            columns=columns,
            fieldtype=source._schema.fieldtype,
            having=self._build_having(),
        )
```

- [ ] **Step 2:** Replace `GroupByQuery.__init__`'s branchy `available` computation with `available = set(source._effective_columns)` (drop the manual computed-columns union — `_effective_columns` includes them). Keep the empty-keys and schema checks.
- [ ] **Step 3:** In `Object._get_ingest_query_info`, use `self._effective_columns` instead of `self._schema.columns`, and delete the `View._get_ingest_query_info` override (base Object's `_effective_columns` is `self._schema.columns`, so base behavior is unchanged).
- [ ] **Step 4:** Run the group-by/aggregation/data suites:

Run: `uv run pytest aaiclick/data -q --no-cov -p no:cacheprovider`
Expected: all pass. Two accepted improvements: renamed columns get real types (was `Float64` fallback) and group-key validation uses post-rename names. If a test asserts the old fallback behavior, inspect it — the new behavior is the correct one; align the test only if it was pinning the bug.

- [ ] **Step 5:** Full suite: `uv run pytest aaiclick -q --no-cov -p no:cacheprovider` — all pass.
- [ ] **Step 6: Commit** — `refactor: derive group-by and ingest column maps from _effective_columns`

## Self-Review

- Coverage: emitter dedup (Tasks 1–2), `_build_select` tail (Task 3), group-by/ingest info (Task 4). Class collapse consciously excluded with rationale.
- Types: `emit_result` returns what `create_object` returns; `GroupByInfo.columns` stays `dict[str, str]` (`info.type` strings) matching `group_by_agg`'s `isinstance(source_type, str)` handling.
- Placeholders: none.
