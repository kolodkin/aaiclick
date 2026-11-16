# Task Values in the Data View Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task (inline, per CLAUDE.md). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** List every task input and result next to a job's objects in `@data job <ref>` and open each in the results grid.

**Architecture:** A new `internal_api/task_values.py` turns the job's `tasks.kwargs` / `tasks.result` JSON into `ObjectView` rows and reads one value by address. `list_objects` appends those rows when `kind != "object"`; `query_object` short-circuits for an address and returns the kernel's `{meta, data}` shape without SQL. The SPA adds two columns, kind chips and a one-cell value view.

**Tech Stack:** Python (SQLModel, Pydantic), React 19 + TypeScript, pytest, vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-11-16-task-values-in-data-view-design.md`

## Global Constraints

- Kinds: `"object"`, `"value"`, `"ref"`; filter also takes `"all"`; default `"object"` keeps today's listing.
- Address: `<task_id>.<kwarg>` for an input, `<task_id>` for a result. Label: `<task_name>.<kwarg>` / `<task_name>.result`.
- Ref type tags: `object` (for `object` and `view`), `task` (`upstream`), `group` (`group_results`), `function` (`callable`), else the raw `_aaiclick_type`.
- Preview cut: 120 characters. Opened value cap: 1 MiB → `{"truncated": true, "bytes": <n>}`.
- Nothing new is stored; no migration.
- CLAUDE.md: imports at top, `Literal` + module constants, no `Any` shortcuts beyond the JSON payload itself, no `__all__`.
- Tests follow the `python-testing-style` skill; docs follow `markdown-style`.

## Review Focus

- A kwarg named `result` and the task's result must be two rows with different addresses (`<id>.result` vs `<id>`).
- An unknown `_aaiclick_type` must list as a `ref` with the raw type, not raise (`ref_kind()` raises — do not call it unguarded).
- An address in a non-job scope, a malformed address, or one naming a task of another job must raise `NotFound`, never read another job's task.
- A task with `result = None` (pending/failed) lists no result row; a task with `kwargs = {}` lists no input rows.
- A 64-bit task id must survive the SPA round trip as a string (addresses are strings end to end).

---

### Task 1: Task value rows and single read

**Files:**
- Create: `aaiclick/internal_api/task_values.py`
- Modify: `aaiclick/data/view_models.py` (`ObjectView`), `aaiclick/view_models.py` (`ObjectFilter`)
- Test: `aaiclick/internal_api/test_task_values.py`

**Interfaces:**
- Produces (in `aaiclick/data/view_models.py`):
  - `OBJECT_KIND_OBJECT = "object"`, `OBJECT_KIND_VALUE = "value"`, `OBJECT_KIND_REF = "ref"`, `OBJECT_KIND_ALL = "all"`, `ObjectKind = Literal["object", "value", "ref"]`
  - `ObjectView` new fields: `kind: ObjectKind = "object"`, `label: str | None`, `task_id: SnowflakeId | None`, `direction: Literal["in", "out"] | None`, `ref_type: str | None`, `preview: str | None`, `target: str | None` (all default `None`)
  - `ObjectFilter.kind: ObjectKind | Literal["all"] = "object"`
- Produces (in `task_values.py`):
  - `PREVIEW_CHARS = 120`, `MAX_VALUE_BYTES = 1024 * 1024`
  - `async def list_task_values(job: JobView) -> list[ObjectView]` — every row of the job, ordered by task id; within a task, inputs by kwarg name, then the result. `scope="job"`, `persistent=False`, `table=""`, `job_id` / `job_name` set, `created_at` = the task's.
  - `async def read_task_value(job_id: int, address: str) -> JsonValue` — the stored value, `native` unwrapped, capped; raises `NotFound`.
  - `def is_address(name: str) -> bool` — `^\d+(\.[A-Za-z_][A-Za-z0-9_]*)?$`.

- [ ] **Step 1: Write the failing tests**

Seed with `create_job` + `create_task` + `commit_tasks` (see `test_e2e/web/seed.py` for the pattern), writing `kwargs` / `result` directly on the rows.

```python
async def test_list_task_values_kinds_refs_and_targets():
    # load: kwargs {"limit": 1000, "result": "kw"}, result object ref to j_<job>_orders
    # clean: kwargs {"df": upstream(load.id), "rules": pydantic, "fmt": callable("m.f"),
    #                "parts": group_results(7), "odd": {"_aaiclick_type": "mystery"}},
    #        result native {"dropped": 12}
    # idle: kwargs {}, result None
    rows = await task_values.list_task_values(job)
    assert [(r.name, r.label, r.kind, r.direction, r.ref_type, r.preview, r.target) for r in rows] == [
        (f"{load.id}.limit", "load.limit", "value", "in", None, "1000", None),
        (f"{load.id}.result", "load.result", "value", "in", None, '"kw"', None),
        (f"{load.id}", "load.result", "ref", "out", "object", "orders", "orders"),
        (f"{clean.id}.df", "clean.df", "ref", "in", "task", "load.result", f"{load.id}"),
        (f"{clean.id}.fmt", "clean.fmt", "ref", "in", "function", "m.f", None),
        (f"{clean.id}.odd", "clean.odd", "ref", "in", "mystery", "mystery", None),
        (f"{clean.id}.parts", "clean.parts", "ref", "in", "group", "group 7 results (0 tasks)", None),
        (f"{clean.id}.rules", "clean.rules", "value", "in", None, 'm.Rules {"dedupe": true}', None),
        (f"{clean.id}", "clean.result", "value", "out", None, '{"dropped": 12}', None),
    ]


async def test_list_task_values_preview_is_cut():
    # one kwarg holding "x" * 500
    assert len(row.preview) == 120


async def test_read_task_value():
    assert await task_values.read_task_value(job.id, f"{clean.id}") == {"dropped": 12}       # native unwrapped
    assert await task_values.read_task_value(job.id, f"{load.id}.result") == "kw"            # the kwarg, not the result
    assert await task_values.read_task_value(job.id, f"{clean.id}.df") == upstream_ref(load.id)  # ref as stored


@pytest.mark.parametrize("address", ["1.nope", "nope", "1.2.3"], ids=["missing_kwarg", "not_address", "malformed"])
async def test_read_task_value_not_found(address): ...   # also: a real task id of another job; a task with result None


async def test_read_task_value_over_cap_is_truncated():
    # result native of 2 MiB string → {"truncated": True, "bytes": <n>} with n > MAX_VALUE_BYTES
```

- [ ] **Step 2: Run** `uv run pytest aaiclick/internal_api/test_task_values.py -q` — expect ImportError.

- [ ] **Step 3: Add the `ObjectView` / `ObjectFilter` fields and constants**

- [ ] **Step 4: Implement `task_values.py`**

One `select(Task.id, Task.name, Task.group_id, Task.kwargs, Task.result, Task.created_at).where(Task.job_id == job.id).order_by(Task.id)`. Classify with `value.get(AAICLICK_TYPE)` (not `ref_kind`): absent / `native` / `pydantic` → `value`; anything else → `ref`. Upstream label comes from the same query's id → name map (`task <id>` when the id is a handle or unknown, no target). Object / view target is `name_from_table(value["table"])`. Group count is the number of the job's tasks with that `group_id`. Preview is `json.dumps(v, ensure_ascii=False)[:PREVIEW_CHARS]`. `read_task_value` filters by both `Task.id` and `Task.job_id`.

- [ ] **Step 5: Run the tests** — expect PASS.

- [ ] **Step 6: Commit** `feat: build task value rows from a job's kwargs and results`

### Task 2: `list_objects` and `query_object`

**Files:**
- Modify: `aaiclick/internal_api/objects.py` (`list_objects`), `aaiclick/internal_api/viewer.py` (`query_object_bytes`), `aaiclick/__main__.py` (objects list: `--kind` with `choices=`)
- Test: `aaiclick/internal_api/test_objects.py`, `aaiclick/internal_api/test_viewer.py`

**Interfaces:**
- Consumes: `list_task_values`, `read_task_value`, `is_address`, `OBJECT_KIND_*` from Task 1.
- Produces: `GET /objects?job=<ref>&kind=all` returns objects then task rows; `POST /viewer/query` with `scope="job:<ref>"` and an address in `object` returns `{"meta": [{"name": "value", "type": "JSON"}], "data": [[<value>]]}`.

- [ ] **Step 1: Write the failing tests**

```python
async def test_list_objects_kind_adds_task_rows():
    # job with one object "orders" and one task: kwargs {"n": 1}, result native 2
    default = await objects.list_objects(ObjectFilter(job=job.id))
    assert [o.name for o in default.items] == ["orders"]
    everything = await objects.list_objects(ObjectFilter(job=job.id, kind="all"))
    assert ([(o.kind, o.label or o.name) for o in everything.items], everything.total) == (
        [("object", "orders"), ("value", "t.n"), ("value", "t.result")], 3)
    values = await objects.list_objects(ObjectFilter(job=job.id, kind="value", limit=1))
    assert ([o.label for o in values.items], values.total) == (["t.n"], 2)
    prefixed = await objects.list_objects(ObjectFilter(job=job.id, kind="all", prefix="t.r"))
    assert [o.label for o in prefixed.items] == ["t.result"]

# add to test_list_objects_invalid_filter_raises params:
pytest.param(ObjectFilter(kind="all"), id="kind_without_job")


async def test_query_object_task_value():
    res = await viewer.query_object(ObjectQueryRequest(scope=f"job:{job.id}", object=f"{task.id}"))
    assert [(c.name, c.type) for c in res.meta] == [("value", "JSON")]
    assert res.data == [[2]]
    csv = await viewer.query_object(ObjectQueryRequest(scope=f"job:{job.id}", object=f"{task.id}.n", fmt="csv"))
    assert csv.text == "value\n1\n"


@pytest.mark.parametrize("extra", [{"where": "1"}, {"fields": ["value"]}, {"order_by": [OrderBy("value", "ASC")]}])
async def test_query_object_task_value_rejects_constraints(extra): ...   # errors.Invalid

async def test_query_object_address_outside_job_scope_is_not_found(): ...  # scope="persistent", object="123"
```

- [ ] **Step 2: Run both test files** — expect the new tests to FAIL.

- [ ] **Step 3: Implement**

`list_objects`: when `filter.kind != "object"` and `filter.job is None` → `Invalid("kind requires job (id or name)")`. Object part runs only for `kind in ("object", "all")`; task part is `list_task_values(job)` filtered by kind and by `label.startswith(prefix)`; `items = (objects + task_rows)[: filter.limit]`, `total` = both counts. `query_object_bytes`: when `is_address(request.object)`, reject constraints, resolve the job from the scope (`NotFound` when the scope has none), and return `json.dumps({"meta": [...], "data": [[value]]}).encode()`; CSV is `csv.writer` output of header `value` and one cell holding `json.dumps(value)`.

- [ ] **Step 4: Run** `uv run pytest aaiclick/internal_api -q` — expect PASS.

- [ ] **Step 5: Commit** `feat: list and read task values through list_objects and query_object`

### Task 3: SPA

**Files:**
- Modify: `src/api/schema.ts` (regenerate: `npm run gen-types`), `src/api/hooks.ts` (`useObjects`), `src/components/ObjectsTable.tsx`, `src/views/Data.tsx`, `src/styles/globals.css`
- Test: `src/lib/viewer.test.ts`, `test_e2e/web/test_viewer.py`, `test_e2e/web/seed.py`

**Interfaces:**
- Consumes: Task 2's REST shapes.
- Produces: `data-testid`s `kind-chip-<all|object|value|ref>`, `value-view`; `isAddress(name: string): boolean` in `src/lib/viewer.ts` (same regex as the server).

- [ ] **Step 1: Write the failing tests**

```ts
// src/lib/viewer.test.ts
it.each([["7302", true], ["7302.df", true], ["orders", false], ["7302.", false]])("isAddress(%s)", (n, want) =>
  expect(isAddress(n)).toBe(want));
```

```python
# test_e2e/web/test_viewer.py — seed.py's OBJECTS_JOB gains a task with kwargs {"limit": 1000}
def test_data_job_lists_and_opens_task_value(page, shot, base_url):
    open_page(page, f"{base_url}/?p=@data job {OBJECTS_JOB}")
    row = page.get_by_test_id("objects-table").locator("tr", has_text=".limit").first
    playwright_api.expect(row).to_contain_text("value")
    playwright_api.expect(row).to_contain_text("1000")
    page.get_by_test_id("kind-chip-object").click()
    playwright_api.expect(page.get_by_test_id("objects-table").locator("tr", has_text=".limit")).to_have_count(0)
    page.get_by_test_id("kind-chip-all").click()
    row.click()
    playwright_api.expect(page.get_by_test_id("value-view")).to_contain_text("1000")
    shot("data-task-value")
```

- [ ] **Step 2: Run** `npm test` — expect `isAddress` to fail.

- [ ] **Step 3: Implement**

- `useObjects(scope)`: add `kind=all` to the request when the scope is a job.
- `ObjectsTable`: when any row has `kind !== "object"`, show the chips (client-side filter, counts in the label) and the Kind and Value / Target columns. Name cell shows `label ?? name` plus the `in` / `out` marker; a ref with `target` renders a link that calls `onPrompt(dataPrompt(scope, target))` without triggering the row click; `ref_type` renders as a tag before the target.
- `Data`: when `isAddress(object)`, render a `ValueView` instead of `ObjectExplorer` — header from the list row (kind, task id, direction, ref type, points-to link), then `ResultsTable` fed by `useObjectRows({scope, object, order_by: [], limit: 1, offset: 0})`; no Limit / paging / pickers. If the kernel's cell does not render a `JSON`-typed object readably, show `<pre>{JSON.stringify(value, null, 2)}</pre>` in place of `ResultsTable`.
- CSS: kind badges reuse the status colour tokens (`--gray`, `--green`, `--purple`).

- [ ] **Step 4: Run** `npm run check && npm test && npm run build`, then `uv run pytest test_e2e/web/test_viewer.py -q` — expect PASS.

- [ ] **Step 5: Commit** `feat: show task values in the Data view`

### Task 4: Docs and cleanup

**Files:**
- Modify: `docs/designs/viewer.md` (Scope Model: task rows, addresses, the one-cell read), `docs/designs/ui.md` (Data section: columns, chips, value view; prompt `@data job <ref> <address>`), `docs/designs/future.md` (add: Inputs / Result panels on `@task <id>` linking to the rows)
- Delete: this plan and the spec (CLAUDE.md: remove once implemented).

- [ ] **Step 1:** Edit the three docs per `markdown-style`; add the `See docs/designs/viewer.md — "<heading>"` line to `task_values.py`'s docstring.
- [ ] **Step 2:** `git rm` the plan and spec; grep for references to either path.
- [ ] **Step 3:** Run `uv run ruff check aaiclick && uv run pytest aaiclick/internal_api -q`.
- [ ] **Step 4:** Commit `docs: task values in the Data view`, push, run the `check-pr` skill.
