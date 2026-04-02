Documentation Improvement Plan
---

Domain gap analysis between aaiclick, FastAPI, and SQLAlchemy docs, with
actionable tasks organized by priority.

# Reuse Strategy: Named Snippets

`pymdownx.snippets` (already configured with `base_path: .`) supports **named sections**.
By adding section markers to existing example files, tutorial and guide pages can include
specific snippets without duplicating code.

**Marker syntax in `.py` files:**

```python
# --8<-- [start:arithmetic]
obj_a = await create_object_from_value([10.0, 20.0, 30.0])
obj_b = await create_object_from_value([2.0, 4.0, 5.0])
result = await (obj_a + obj_b)
print(await result.data())  # [12.0, 24.0, 35.0]
# --8<-- [end:arithmetic]
```

**Include syntax in `.md` files:**

```markdown
```python
;--8<-- "aaiclick/examples/basic_operators.py:arithmetic"
```                                          (closing fence)
```

**Existing example files to annotate with section markers:**

| Example File               | Sections to Mark                                                       |
|----------------------------|------------------------------------------------------------------------|
| `basic_operators.py`       | `scalar_creation`, `list_creation`, `dict_creation`, `arithmetic`, `comparison` |
| `statistics.py`            | `basic_stats`, `temperature_analysis`                                  |
| `views.py`                 | `where_clause`, `limit_offset`, `order_by`, `mixed_constraints`        |
| `group_by.py`              | `basic_groupby`, `multi_agg`, `having`                                 |
| `selectors.py`             | `column_select`, `rename`                                              |
| `data_manipulation.py`     | `insert`, `concat`, `copy`                                             |
| `orchestration_basic.py`   | `task_job_intro`                                                       |

Each existing example page (`docs/examples/*.md`) continues to include the **full file**
unchanged. Tutorial pages include only the **named section** they need. One source of truth,
zero duplication.

# Domain Gap Table

| #  | Gap                              | FastAPI Pattern                                                   | SQLAlchemy Pattern                                                    | aaiclick Current                                        | Proposed Fix                                           | Priority | Effort |
|----|----------------------------------|-------------------------------------------------------------------|-----------------------------------------------------------------------|---------------------------------------------------------|--------------------------------------------------------|----------|--------|
| 1  | Landing page                     | Hero section, feature cards, badges, code snippet, social proof   | Getting Started links, architectural overview                         | 5-line README included via `--8<--`                     | Dedicated `index.md` with feature cards, badges, wow snippet | P0       | S      |
| 2  | Example output                   | Shows response bodies inline under each code block                | Shows generated SQL below Python code                                 | Raw `.py` includes, no output                           | Add `??? example "Expected Output"` blocks to each example page | P0       | S      |
| 3  | Admonitions                      | `tip`, `info`, `check`, `warning`, `technical details` throughout | Notes, warnings, deprecated markers on every page                     | Extensions configured, never used                       | Add admonitions to `getting_started.md`, `object.md`, `data_context.md` | P0       | S      |
| 4  | Progressive tutorial             | 30+ page step-by-step, each page self-contained + sequential      | Unified tutorial building engine → metadata → operations → ORM       | Quick Example → 30KB reference wall                     | 7-page tutorial using named snippets from existing examples | P1       | L      |
| 5  | Troubleshooting / FAQ            | —                                                                 | Error reference with root cause analysis, FAQ by workflow              | No error docs; `technical_debt.md` not in nav           | Defer until project matures — not enough user-reported issues yet | P3       | M      |
| 6  | Cross-page links                 | Parenthetical forward refs, direct backward refs, lateral links   | "See also" sections on nearly every method/page                       | Pages are siloed; minimal cross-referencing              | Add "See Also" footers + inline links between guide, examples, API | P1       | S      |
| 7  | Tabbed content                   | Tabs for Python 3.9+ vs 3.10+ syntax                              | —                                                                     | Extension configured, never used                        | Use tabs for local vs distributed install/config        | P2       | S      |
| 8  | Comparison page                  | Explicit comparison vs Flask, Django REST, Express                 | —                                                                     | README mentions "Pandas/SQLAlchemy-inspired" with no elaboration | `comparison.md` with feature matrix vs Pandas, Spark, Dask | P2       | M      |
| 9  | Glossary                         | —                                                                 | 100+ terms with cross-references and aliases                          | ClickHouse terms unexplained                            | `glossary.md` with ~20 key terms                       | P2       | M      |
| 10 | `object.md` structure            | Progressive: simple first, advanced later, collapsible details    | Quickstart → deep dive separation                                     | 30KB flat wall mixing ref tables + impl notes           | Restructure: Quick Reference → Common Patterns → Details (collapsible) | P2       | M      |
| 11 | Changelog                        | —                                                                 | Per-version "What's New" + migration guide                            | No version history in docs                              | `changelog.md` in Keep a Changelog format              | P3       | S      |
| 12 | Contributing guide               | —                                                                 | Full contributor guide with dev setup, test conventions                | `CLAUDE.md` has guidelines but isn't user-facing        | `contributing.md` extracted from relevant `CLAUDE.md` sections | P3       | S      |
| 13 | Navigation link 404s             | —                                                                 | —                                                                     | `site_url` missing `/en/latest/` prefix                 | **FIXED** in this branch                               | P0       | S      |

**Effort key**: S = small (< 1 hour), M = medium (1–3 hours), L = large (3+ hours)

# Implementation Plan

## Phase 1 — Quick Wins (P0, all small effort)

### 1a. Landing page (`docs/index.md`)

Replace README include with a dedicated page:

- Project tagline + one-paragraph pitch
- Feature cards (MkDocs Material grid) for: ClickHouse-Powered, Familiar API, Local-First, AI-Ready
- "Wow factor" code snippet (use named snippet `basic_operators.py:arithmetic`)
- PyPI badge, CI badge, license badge
- Quick links to Getting Started, Tutorial, Examples

### 1b. Example output blocks (`docs/examples/*.md`)

For each of the 14 example pages, add:

- Brief intro paragraph explaining what the example covers
- `??? example "Expected Output"` collapsible block after the code with actual output

Do **not** add `!!! tip` admonitions to example pages — the code speaks for itself.

Template:

```markdown
# Basic Operators

Arithmetic, comparison, and bitwise operations on Objects.
For API details, see [Arithmetic Operators](../object.md#arithmetic-operators).

```python
--8<-- "aaiclick/examples/basic_operators.py"
```                                          (closing fence)

??? example "Expected Output"

    ```
    Example 1: Creating objects from scalar values
    ...
    ```
```

### 1c. Admonitions in existing guide pages

**Principle**: Use admonitions only at genuine pitfall points — places where a user is
likely to hit a confusing error without the callout. Never for emphasis, decoration, or
restating what the surrounding prose already says. Target: **7 total** across all guide pages.

**`getting_started.md`** (2 admonitions):

1. After the Quick Example code block — warning about the `await` pitfall:

    ```markdown
    !!! warning "Always `await` operation results"
        `prices * tax_rate` returns a coroutine, not an Object.
        Forgetting `await` gives a confusing error downstream —
        not at the line where you forgot it.
    ```

2. After "All objects created inside `data_context()` are automatically cleaned up" sentence —
   replace that sentence with a collapsible detail:

    ```markdown
    ??? info "Automatic cleanup"
        All objects created inside `data_context()` are cleaned up on exit —
        no manual table management required. Don't store Objects for use after
        the context exits.
    ```

**`object.md`** (3 admonitions):

3. In the Scalar Broadcast section — replace the prose explanation:

    ```markdown
    !!! tip "Scalar broadcast"
        Python scalars work on either side of an operator:
        `obj * 2` and `2 * obj` both work. The scalar is auto-converted
        to a single-value Object via `_ensure_object()`.
    ```

4. In the String/Regex section, after the operator table — the RE2 note is already
   there as plain text but easy to miss:

    ```markdown
    !!! info "ClickHouse uses RE2 regex syntax"
        No lookaheads or lookbehinds. See
        [RE2 syntax](https://github.com/google/re2/wiki/Syntax).
    ```

5. In the `or_where()` / `or_having()` docs — the "requires a prior" rule is a common
   footgun:

    ```markdown
    !!! warning "`or_where()` requires a prior `where()`"
        Calling `or_where()` without a preceding `where()` raises `ValueError`.
        Same applies to `or_having()` on `GroupByQuery`.
    ```

**`data_context.md`** (2 admonitions):

6. In the Object Lifecycle and Staleness section — the staleness rule:

    ```markdown
    !!! warning "Objects become stale when their context exits"
        Using a stale Object raises `RuntimeError`. Create and consume Objects
        within the same `data_context()` block. Don't store them in module-level
        variables or pass them between contexts.
    ```

7. After the Deployment Modes paragraph — help users choose:

    ```markdown
    ??? info "Which deployment mode?"
        Start with the default (chdb + SQLite) — it needs zero setup.
        Switch to distributed (remote ClickHouse + PostgreSQL) when data
        exceeds local disk or you need multiple workers.
    ```

**Pages with zero admonitions** (and why):

| Page                | Reason                                                        |
|---------------------|---------------------------------------------------------------|
| `orchestration.md`  | Spec-style doc — admonitions would fight the dense reference format |
| `ai.md`             | Small page, mostly unimplemented — premature to decorate     |
| `oplog.md`          | Internal spec — admonition audience doesn't overlap           |
| `examples/*.md`     | Code-only pages — output blocks are sufficient                |

## Phase 2 — Tutorial (P1, large effort)

### 2a. Add section markers to existing example files

Annotate 7 example files with `# --8<-- [start:name]` / `# --8<-- [end:name]` markers.
This is non-breaking — existing full-file includes still work.

### 2b. Create tutorial pages

| File                               | Title                 | Snippets From                          | New Code Needed |
|------------------------------------|-----------------------|----------------------------------------|-----------------|
| `docs/tutorial/index.md`           | Tutorial overview     | —                                      | None             |
| `docs/tutorial/first_object.md`    | Your First Object     | `basic_operators.py:scalar_creation`, `basic_operators.py:list_creation` | None |
| `docs/tutorial/operations.md`      | Operations            | `basic_operators.py:arithmetic`, `basic_operators.py:comparison` | None |
| `docs/tutorial/aggregations.md`    | Aggregations          | `statistics.py:basic_stats`, `group_by.py:basic_groupby` | None |
| `docs/tutorial/dict_objects.md`    | Multi-Column Data     | `basic_operators.py:dict_creation`, `selectors.py:column_select` | None |
| `docs/tutorial/views.md`           | Views & Filters       | `views.py:where_clause`, `views.py:limit_offset` | None |
| `docs/tutorial/persistence.md`     | Persistence           | —                                      | Small (3–5 lines) |
| `docs/tutorial/orchestration.md`   | Orchestration         | `orchestration_basic.py:task_job_intro` | None |

**6 of 7 tutorial pages need zero new code** — they compose named snippets from existing examples.

Each tutorial page follows a consistent skeleton:

```markdown
# Page Title

What you'll learn: ...

## Section

Brief explanation.

```python
;--8<-- "aaiclick/examples/file.py:section_name"
```                                          (closing fence)

??? example "Expected Output"
    ```
    ...
    ```

## Next Steps

- [Next tutorial page](next.md) — what it covers
- [Related API reference](../api/data.md) — for full details
```

### 2c. Cross-page linking pass

Add to each guide page:

- "See Also" footer with links to related examples and API reference
- Inline links from guide sections to relevant example pages

Add to each example page:

- Header link back to relevant guide section

## Phase 3 — Polish (P2)

### 3a. Tabbed content

Add tabs to `getting_started.md` for local vs distributed install.
Add tabs to `data_context.md` for local vs distributed config.

### 3b. Comparison page (`docs/comparison.md`)

Feature matrix: aaiclick vs Pandas vs Spark vs Dask.

### 3c. Glossary (`docs/glossary.md`)

~20 terms: Object, View, DataContext, Snowflake ID, chdb, Scalar Broadcast, etc.

### 3d. Restructure `object.md`

- Keep API Quick Reference table at top
- Add "Common Patterns" section (5–6 most-used patterns with snippets)
- Wrap detailed operator sections in `??? details` collapsible blocks
- Move implementation references to separate contributor doc or remove

## Phase 4 — Maintenance & Deferred (P3)

### 4a. Changelog (`docs/changelog.md`)

Keep a Changelog format. Add to nav.

### 4b. Contributing guide (`docs/contributing.md`)

Extract from `CLAUDE.md`: dev setup, test conventions, code style, commit format.

### 4c. Troubleshooting / FAQ (`docs/troubleshooting.md`) — deferred

Wait until project has enough real user-reported issues to populate this meaningfully.
Premature FAQ pages with hypothetical problems add noise. Revisit after public release
or when GitHub issues show recurring patterns. Candidate sections when ready:

- "Object is stale" — cause, fix, pattern
- Forgetting `await` — symptoms, fix
- "No active data_context" — cause, fix
- chdb `url()` hanging — workaround (currently in `technical_debt.md`)
- Connection URL format errors
- ClickHouse type mismatches

# Updated Navigation

```yaml
nav:
  - Home: index.md
  - Getting Started: getting_started.md
  - Tutorial:
    - Overview: tutorial/index.md
    - Your First Object: tutorial/first_object.md
    - Operations: tutorial/operations.md
    - Aggregations: tutorial/aggregations.md
    - Multi-Column Data: tutorial/dict_objects.md
    - Views & Filters: tutorial/views.md
    - Persistence: tutorial/persistence.md
    - Orchestration: tutorial/orchestration.md
  - User Guide:
    - Object API: object.md
    - DataContext: data_context.md
    - Orchestration: orchestration.md
    - AI Layer: ai.md
    - Operation Log: oplog.md
  - Examples:
    - ... (unchanged)
  - API Reference:
    - ... (unchanged)
  - Glossary: glossary.md
  - Comparison: comparison.md
  - Changelog: changelog.md
  - Contributing: contributing.md
  - License: LICENSE.md
```
