# Phase 6 — Cleanup: `ColumnMeta`, Renderer, Docs

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Delete `ColumnMeta` (the YAML-comment helper), remove the `aai_id` skip from the CLI renderer, purge `aai_id` from every documentation page, and run one final project-wide grep to catch any straggler. Finish with a repo-wide test pass.

**Depends on:** Phases 1-5 (all runtime paths are registry-driven and `aai_id`-free).

**Unlocks:** PR-ready.

---

## File Structure

| File                                                      | Role                                                                  |
|-----------------------------------------------------------|-----------------------------------------------------------------------|
| `aaiclick/data/models.py`                                 | Modify — delete `ColumnMeta` and `yaml` import.                       |
| `aaiclick/cli_renderers.py`                               | Modify — remove `aai_id` skip in `render_object_detail` (line 223).   |
| `aaiclick/test_cli_renderers.py` (**new — create**)       | Create — no such test file exists today; add coverage for the line-223 change. |
| `aaiclick/locks.py`                                       | Modify — doc comment at line 7 references `aai_id`; rewrite.          |
| `aaiclick/oplog/lineage.py`                               | Modify — runtime warning string at line 157 (`"fresh aai_id values"`). |
| `aaiclick/ai/agents/prompts.py`                           | Modify — delete `AAI_ID_WARNING` (lines 5-9) and its inclusion (line 53). |
| `aaiclick/ai/agents/test_lineage_agent.py:115`, `aaiclick/ai/agents/test_tools.py:26,35,60`, `aaiclick/internal_api/test_objects.py:91`, `aaiclick/orchestration/execution/test_execution.py:180,191` | Modify — each references `aai_id` as a schema column or an `order_by` string; update. |
| `docs/object.md`                                          | Modify — remove aai_id order-preservation section; document new API.  |
| `docs/data_context.md`                                    | Modify — describe schema storage via `table_registry.schema_json`.    |
| `docs/glossary.md`                                        | Modify — remove the `aai_id` entry.                                   |
| `docs/insert_advisory_lock.md`                            | Modify — drop `aai_id` references.                                    |
| `docs/lineage.md`                                         | Modify — drop the `AAI_ID_WARNING` reference (if present).            |
| `docs/future.md`                                          | Modify — remove items made obsolete by this refactor.                 |

---

### Task 6.1: Delete `ColumnMeta`

**Files:**
- Modify: `aaiclick/data/models.py` — delete the `ColumnMeta` dataclass (lines ~337-380) and the `import yaml` line at the top of the file if it has no other users.

**There are no `ColumnMeta`-specific tests to delete** — `aaiclick/data/test_models.py` does not exist. `ColumnMeta`'s YAML round-trip is exercised indirectly via `test_schema.py`, which Phase 2's rewrite already handled. An earlier draft of this plan said otherwise; it was wrong.

- [ ] **Step 1: Inventory uses**

Run:

```bash
rg "ColumnMeta" aaiclick/ --type py
```

Expected hits after Phases 2-5: only the class definition itself in `aaiclick/data/models.py`. Every production reference should be gone.

If any production file still imports `ColumnMeta`, that import is dead — delete it.

- [ ] **Step 2: Delete the class**

In `aaiclick/data/models.py`, delete the `@dataclass class ColumnMeta` block (≈lines 337-380) and — if no other code in this module uses PyYAML — delete `import yaml` at the top. Grep `rg "import yaml" aaiclick/ --type py` to confirm the second removal is safe.

- [ ] **Step 3: Run tests**

Run: `pytest aaiclick/ -v`
Expected: all pass. If an import error surfaces, that's a dead import that was missed — delete it.

- [ ] **Step 4: Commit**

```bash
git add aaiclick/data/models.py
git commit -m "cleanup: delete ColumnMeta YAML helper"
```

---

### Task 6.2: CLI renderer drops `aai_id` skip

**Files:**
- Modify: `aaiclick/cli_renderers.py` — `render_object_detail`, line 223 (the `if col.name == "aai_id": continue` branch).
- Create: `aaiclick/test_cli_renderers.py` — no such file exists today.

- [ ] **Step 1: Write the failing test**

Create or append in the renderer test file:

```python
import io
from contextlib import redirect_stdout

from aaiclick.cli_renderers import render_object_detail
from aaiclick.view_models import ColumnView, ObjectDetail, SchemaView  # adjust to real import path


def test_render_object_detail_does_not_filter_aai_id_column():
    detail = ObjectDetail(
        name="x", table="t_1", scope="local",
        row_count=None, size_bytes=None, created_at=None,
        table_schema=SchemaView(
            columns=[
                ColumnView(name="aai_id", type="UInt64", fieldtype="s"),
                ColumnView(name="value", type="Int64", fieldtype="a"),
            ],
            order_by=None, engine="MergeTree", fieldtype="a",
        ),
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        render_object_detail(detail)

    output = buf.getvalue()
    # After the refactor aai_id is a user-level column name if present,
    # and must be rendered like any other column.
    assert "aai_id: UInt64" in output
    assert "value: Int64" in output
```

Adjust constructor kwargs to match the real `ObjectDetail` shape (grep for `class ObjectDetail`).

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest aaiclick/test_cli_renderers.py -v -k aai_id`
Expected: FAIL — the current `render_object_detail` skips the column.

- [ ] **Step 3: Remove the skip**

In `aaiclick/cli_renderers.py` (line ~217-220):

```python
# Before:
for col in detail.table_schema.columns:
    if col.name == "aai_id":
        continue
    print(f"  {col.name}: {col.type}")

# After:
for col in detail.table_schema.columns:
    print(f"  {col.name}: {col.type}")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest aaiclick/test_cli_renderers.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/cli_renderers.py aaiclick/test_cli_renderers.py
git commit -m "cleanup: CLI renderer no longer skips aai_id column"
```

---

### Task 6.2.5: Out-of-data production code — `locks.py`, `oplog/lineage.py`, `ai/agents/prompts.py`

**Files:**
- Modify: `aaiclick/locks.py` — doc comment at line 7 references `aai_id` marking insert-batch boundaries. Rewrite to point at whatever the advisory-lock mechanism actually uses post-refactor (see `docs/insert_advisory_lock.md`).
- Modify: `aaiclick/oplog/lineage.py` — the `f"- ⚠ \`{node.operation}\` generates fresh aai_id values..."` warning at line 157 is no longer true. Delete the branch that emits it.
- Modify: `aaiclick/ai/agents/prompts.py` — delete `AAI_ID_WARNING` (lines 5-9) and its inclusion in the prompt templates (line 53).

**Background:** These three sites are LLM / lineage concerns outside `aaiclick/data/` and `aaiclick/orchestration/`. Phase 4's grep sweep explicitly excluded them because their fixes are non-mechanical (changing a prompt can change agent behaviour; deleting a lineage warning changes the lineage graph output).

- [ ] **Step 1: Update the lineage warning emission**

In `aaiclick/oplog/lineage.py`, find the `node.operation` branch at line 157 that emits the "fresh aai_id values" warning. With `aai_id` gone, operations no longer generate fresh IDs — delete the warning line entirely. If the enclosing `if` becomes an empty pass-through, simplify.

- [ ] **Step 2: Update the agent prompt**

In `aaiclick/ai/agents/prompts.py`, delete `AAI_ID_WARNING` at lines 5-9. At line 53 (inside the prompt template f-string), remove the `{AAI_ID_WARNING}\n` insertion. Check the surrounding blank lines to keep the template visually clean.

Update `aaiclick/ai/agents/test_lineage_agent.py:115` and `aaiclick/ai/agents/test_tools.py:26,35,60` — grep for `aai_id` and for `AAI_ID_WARNING` inside those tests. Each hit is either (a) a string literal asserting the warning appears in agent output (delete the assertion), or (b) a fixture that constructs a schema containing `aai_id` (replace with a real column name).

- [ ] **Step 3: Update the locks doc comment**

In `aaiclick/locks.py`, rewrite the comment at line 7 so it doesn't claim `aai_id` ranges mark insert-batch boundaries. Keep it short — one line explaining what the advisory lock actually guards.

- [ ] **Step 4: Update stragglers in internal_api / execution tests**

- `aaiclick/internal_api/test_objects.py:91` — asserts `aai_id` appears in schema output; flip to asserting absence or remove if the assertion's whole point was column filtering.
- `aaiclick/orchestration/execution/test_execution.py:180,191` — `View` serialisation with `order_by="aai_id ASC"`. Phase 4 Task 4.5 already touches this file; reconfirm the fix here.

- [ ] **Step 5: Run the relevant suites**

```bash
pytest aaiclick/ai/ aaiclick/oplog/ aaiclick/internal_api/ -v
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/locks.py aaiclick/oplog/lineage.py aaiclick/ai/agents/prompts.py \
        aaiclick/ai/agents/test_lineage_agent.py aaiclick/ai/agents/test_tools.py \
        aaiclick/internal_api/test_objects.py
git commit -m "cleanup: purge aai_id from lineage warnings, agent prompts, locks doc"
```

---

### Task 6.3: Docs — `docs/object.md`

**Files:**
- Modify: `docs/object.md` — lines 325, 329, 388, 390, 448, 455, 624 (per Phase 0 research).

- [ ] **Step 1: Read the current sections**

Open `docs/object.md` and read every section that mentions `aai_id` or "order preservation". Sections to rewrite:

- The block around line 325-329 titled "Order preservation" (or similar) — today it describes the implicit `aai_id` ordering.
- The block around line 388-390 describing `data()` row order.
- The block around line 448-455 discussing schema display and `aai_id` exclusion.
- Line ~624 saying `aai_id` cannot be renamed.

- [ ] **Step 2: Rewrite the "Order preservation" section**

Replace with a section titled "Row order" explaining:

- Array Objects have no implicit row order.
- Binary elementwise ops between two array Objects from **different tables** require each side to be `View(order_by=...)`. Otherwise the operator raises `TypeError` at call time.
- Same-table ops, scalar broadcast, and aggregations keep working without `View`.
- For reads, `.data()` has `order_by`, `offset`, `limit` kwargs — `limit=1000` by default.

Include a worked example:

```python
a = await ctx.create("a", [1, 2, 3])
b = await ctx.create("b", [10, 20, 30])

# Error — no explicit row order across tables:
# a + b  -> TypeError

# Correct — both sides declare their ordering:
result = a.view(order_by="value") + b.view(order_by="value")
print(await result.data(order_by="value"))  # → [11, 22, 33]
```

Include a warning admonition only if a reader would naturally hit this error without it (per CLAUDE.md admonition policy):

```markdown
!!! warning "Cross-table array ops need `order_by`"
    `a + b` between two array Objects from different tables raises `TypeError`
    unless both sides are wrapped with `.view(order_by=...)`. Row order is
    opt-in, not implicit.
```

- [ ] **Step 3: Rewrite the `data()` section**

Document the new signature:

```python
await obj.data()                           # up to 1000 rows, arbitrary order
await obj.data(order_by="value")           # deterministic
await obj.data(order_by="value", limit=None)   # all rows
await obj.data(order_by="value", offset=10, limit=5)

await obj.view(order_by="value").data()    # view attrs used when kwargs absent
await obj.view(order_by="value").data(limit=10)  # kwarg overrides view
```

Summarise the resolution rules: "kwargs override the view; when absent, the view's values apply; scalar Objects ignore these kwargs".

- [ ] **Step 4: Remove every `aai_id` mention**

Run:

```bash
rg "aai_id" docs/object.md
```

Expected: zero remaining hits.

- [ ] **Step 5: Commit**

```bash
git add docs/object.md
git commit -m "docs: rewrite object.md order-preservation + data() sections"
```

---

### Task 6.4: Docs — `docs/data_context.md`, `docs/glossary.md`

- [ ] **Step 1: `docs/data_context.md`**

- Lines 110, 116-119, 172-173 currently describe `aai_id`. Replace the schema-storage description so it references `table_registry.schema_json` instead of "per-column YAML COMMENT".
- Remove any example SQL that defines an `aai_id UInt64` column.
- Remove the "`aai_id` cannot be overridden in `fields`" warning.

Add one concise block (no admonition unless it solves a real pitfall):

```markdown
Each aaiclick-managed table has one row in the SQL `table_registry`. The
`schema_json` column on that row stores the serialised `SchemaView` — column
types, fieldtypes, `order_by`, and engine — read back via
`_get_table_schema` when an Object is attached to an existing table.
```

- [ ] **Step 2: `docs/glossary.md`**

Delete the `` `aai_id` `` entry (the one around lines 6-8 and the line-80 mention).

- [ ] **Step 3: Run docs grep**

```bash
rg "aai_id" docs/data_context.md docs/glossary.md
```

Expected: zero hits.

- [ ] **Step 4: Commit**

```bash
git add docs/data_context.md docs/glossary.md
git commit -m "docs: update data_context and glossary for registry-based schema"
```

---

### Task 6.5: Docs — `docs/insert_advisory_lock.md`, `docs/lineage.md`, `docs/future.md`

- [ ] **Step 1: `docs/insert_advisory_lock.md`**

Lines 24, 166, 226 currently describe `aai_id` ordering guarantees. Rewrite each to use whatever identifier the advisory-lock mechanism actually uses post-refactor (likely `insert_id` or a snapshot timestamp — check the code). If the rationale for that doc genuinely depended on `aai_id` being contiguous, and the refactor removes that guarantee, note the change explicitly and link to this plan.

- [ ] **Step 2: `docs/lineage.md`**

Any passage that discusses the `AAI_ID_WARNING` emitted by the lineage agent becomes inaccurate once Task 6.2.5 deletes the constant. Update or drop. (Note: the earlier plan draft referenced `docs/lineage_implementation_plan.md`, which was deleted in commit `2a75c4c`. That file is gone; the current home for lineage documentation is `docs/lineage.md`.)

- [ ] **Step 3: `docs/future.md`**

Remove any item that mentioned `aai_id` or "per-column COMMENT YAML" — those are now obsolete.

- [ ] **Step 4: Grep the docs tree**

```bash
rg "aai_id" docs/
```

Expected: zero hits.

- [ ] **Step 5: Commit**

```bash
git add docs/insert_advisory_lock.md docs/lineage.md docs/future.md
git commit -m "docs: purge aai_id references from advisory-lock, lineage, and future docs"
```

---

### Task 6.6: Final grep sweep + full test run

- [ ] **Step 1: Grep the whole repo**

```bash
rg "aai_id" aaiclick/ docs/ --type py --type md
```

Expected hits:

- Negative assertions in tests (`assert "aai_id" not in ...`). OK.
- The test asserting `aai_id` is no longer reserved as a column name (from Phase 3). OK.
- Possibly `aaiclick/data/object/test_data.py` if it references `aai_id` as a user column in a test. OK.

Any other hit is a missed reference — fix it. Also run the scope-specific greps against the directories Phase 4 excluded:

```bash
rg "aai_id" aaiclick/locks.py aaiclick/oplog/ aaiclick/ai/ aaiclick/internal_api/
```

Expected: zero production hits; only test negative-assertions.

- [ ] **Step 2: Search for lingering YAML / COMMENT code**

```bash
rg "to_yaml|from_yaml|ColumnMeta" aaiclick/ --type py
rg "COMMENT '" aaiclick/ --type py
rg "generateSnowflakeID" aaiclick/ --type py
```

Each must return zero hits in production code. Tests may reference them only for negative assertions.

- [ ] **Step 3: Run the full test suite**

```bash
pytest aaiclick/ -v
```

Expected: all pass.

- [ ] **Step 4: Validate SQL patterns via chdb-eval**

Per the spec: use the `chdb-eval` skill to check:

- No `ORDER BY aai_id` anywhere in operator SQL.
- Cross-table operator SQL emits `row_number() OVER (ORDER BY <user_order>)`.
- `CREATE TABLE` statements for tables without a user-supplied order key use `ORDER BY tuple()`.

Capture the SQL strings that `operators.py` and `data_context.py` actually produce (print them during a test run, or construct representative cases) and feed them to chdb-eval.

- [ ] **Step 5: Run check-pr for the branch**

Per `CLAUDE.md`: "After completing each task, use the `check-pr` skill to verify GitHub Actions workflows are successful." Push the branch, then run the skill.

```bash
git push -u origin claude/remove-aai-id-SQAqv
```

Then invoke the `check-pr` skill and resolve any failing workflows.

- [ ] **Step 6: Final commit (docs polish if needed)**

If chdb-eval or check-pr surfaced anything, fix it and commit:

```bash
git add -A
git commit -m "cleanup: final polish after chdb-eval and CI feedback"
```

---

## Phase 6 Complete — Refactor Done

Final state:

- Tables carry only user columns; no `aai_id`, no ClickHouse COMMENTs.
- `table_registry.schema_json` is the single source of truth for schema metadata.
- Cross-table array ops require `View(order_by=...)`; same-table, scalar broadcast, aggregations unchanged.
- `.data()` is safe to call without any kwargs (capped at 1000 rows, arbitrary order); callers opt into determinism with `order_by`.
- `ColumnMeta` and the YAML-comment code path are gone.
- Every doc page is aligned with the new contract.
- CI is green.

Open the PR with a summary pointing at the six phase files, the spec, and the final grep results showing zero `aai_id` hits outside negative-assertion tests.
