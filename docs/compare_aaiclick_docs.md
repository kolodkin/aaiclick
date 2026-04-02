aaiclick Documentation: Comparison & Improvement Proposals
---

Comparison of aaiclick ReadTheDocs with FastAPI and SQLAlchemy documentation,
with concrete proposals for improvement.

# Executive Summary

aaiclick has a solid documentation foundation: MkDocs Material theme, auto-generated API
docs via mkdocstrings, snippet-included examples, and a clear nav structure. However,
compared to FastAPI and SQLAlchemy docs, there are significant gaps in **onboarding
experience**, **tutorial depth**, **discoverability**, and **progressive disclosure** that
would make the docs more effective for new and experienced users alike.

# Current State Assessment

## What aaiclick does well

| Strength                        | Details                                                      |
|---------------------------------|--------------------------------------------------------------|
| MkDocs Material theme           | Dark/light mode, code copy, search, tabs — good foundation   |
| Snippet-included examples       | Examples pulled from real `.py` files, so they stay in sync   |
| API Quick Reference table       | `object.md` has a comprehensive operator table with links     |
| Auto-generated API docs         | mkdocstrings pulls from docstrings — single source of truth   |
| Environment variable docs       | Clean table with defaults and descriptions                    |
| Navigation structure            | Logical grouping: Guide, Examples, API Reference              |

## Where aaiclick falls short (vs. FastAPI & SQLAlchemy)

| Gap                              | FastAPI / SQLAlchemy Pattern                                  | aaiclick Current State                                       |
|----------------------------------|---------------------------------------------------------------|--------------------------------------------------------------|
| **Home page / landing**          | FastAPI: hero section, badges, feature highlights, social proof | README.md with 5-line description and install command        |
| **Tutorial / guided walkthrough**| FastAPI: 30+ page progressive tutorial; SQLAlchemy: unified tutorial | No tutorial — jumps from "Quick Example" to reference docs  |
| **Example output**               | FastAPI: shows request/response inline; SQLAlchemy: shows SQL output | Examples are raw `.py` includes with no output shown         |
| **Admonitions / callouts**       | Both use tips, warnings, notes extensively                     | Extension configured but not used in any docs                |
| **Progressive disclosure**       | FastAPI: simple first, advanced later; SQLAlchemy: quickstart → deep dive | `object.md` is a 30KB wall of reference — no gradual intro  |
| **Cross-page linking**           | Both extensively cross-reference between pages                 | Minimal cross-references; mostly siloed pages                |
| **Error guidance**               | SQLAlchemy: dedicated error reference with root cause analysis | No error documentation or troubleshooting                    |
| **Tabbed content**               | FastAPI: tabs for Python 3.9+ vs 3.10+ syntax                 | Extension configured but not used                            |
| **Search-friendly headings**     | Both use descriptive, search-friendly section titles           | Some headings are terse (e.g., "Views", "Copy")              |
| **Changelog / What's New**       | SQLAlchemy: per-version migration guide + what's new           | No changelog or version history in docs                      |
| **Glossary**                     | SQLAlchemy: 100+ term glossary with cross-references           | No glossary — ClickHouse terms unexplained                   |

# Proposed Improvements

## 1. Landing Page Overhaul

**Inspiration**: FastAPI's landing page with feature highlights, badges, and quick pitch.

**Current**: `index.md` just includes `README.md` (5 lines + install).

**Proposed**: A dedicated landing page with:

- Tagline and one-paragraph pitch
- Key feature cards (Object API, Orchestration, AI Layer, Local-first)
- "Why aaiclick?" section comparing to alternatives (Pandas, Spark, Dask)
- Badges (PyPI version, CI status, license, Python versions)
- Code snippet showing the "wow factor" — a 10-line example that does something impressive
- Quick links to Getting Started, Tutorial, Examples

```markdown
--8<-- "README.md:overview"   <!-- named snippet for just the overview -->

## Key Features

<div class="grid cards" markdown>

-   :material-database: **ClickHouse-Powered**

    All computation runs inside ClickHouse. Python orchestrates — ClickHouse computes.

-   :material-language-python: **Familiar API**

    Pandas-like operators (`+`, `-`, `*`, `.mean()`, `.group_by()`) on distributed data.

-   :material-server: **Local-First, Scale-Out**

    Start with embedded chdb + SQLite. Scale to remote ClickHouse + PostgreSQL.

-   :material-robot: **AI-Ready**

    Built-in lineage tracing and debug agents powered by LLMs.

</div>
```

## 2. Add a Progressive Tutorial

**Inspiration**: FastAPI's step-by-step tutorial and SQLAlchemy's unified tutorial.

**Current**: No tutorial. Users jump from a 10-line Quick Example straight to reference docs.

**Proposed**: A multi-page tutorial that progressively builds concepts:

| Page                             | Concepts Introduced                                          |
|----------------------------------|--------------------------------------------------------------|
| `tutorial/01_first_object.md`    | Install, setup, `data_context()`, `create_object_from_value`, `.data()` |
| `tutorial/02_operations.md`      | Arithmetic, comparison, scalar broadcast, chaining           |
| `tutorial/03_aggregations.md`    | `.sum()`, `.mean()`, `.count()`, `.group_by()`               |
| `tutorial/04_dict_objects.md`    | Multi-column data, `obj["col"]`, `obj[["cols"]]`            |
| `tutorial/05_views.md`           | `.view()`, `.where()`, `.with_columns()`, `.explode()`       |
| `tutorial/06_persistence.md`     | Persistent objects, `open_object()`, `list_persistent_objects()` |
| `tutorial/07_orchestration.md`   | `@task`, `@job`, worker patterns                             |

Each page should:
- Start with what you'll learn
- Show complete, runnable code
- Show expected output inline
- End with "Next steps" linking forward
- Use admonitions for tips, warnings, and "info" boxes

## 3. Show Example Output Inline

**Inspiration**: FastAPI shows response bodies; SQLAlchemy shows generated SQL.

**Current**: Example pages just include raw `.py` files with no context or output.

**Proposed**: Add output blocks and explanatory text around examples:

```markdown
# Basic Operators

This example demonstrates arithmetic operations on Objects.

```python
--8<-- "aaiclick/examples/basic_operators.py"
```

??? example "Expected Output"

    ```
    Addition:        [15.0, 25.0, 35.0]
    Multiplication:  [50.0, 100.0, 150.0]
    Scalar Broadcast: [20.0, 40.0, 60.0]
    ```

!!! tip
    All operations create new Objects — the originals are never modified.
```

## 4. Use Admonitions Throughout

**Inspiration**: Both FastAPI and SQLAlchemy use tips/warnings/notes extensively.

**Current**: `admonition` and `pymdownx.details` extensions are configured but unused.

**Proposed**: Add admonitions throughout existing docs:

- `!!! tip` — Performance hints, best practices
- `!!! warning` — Common pitfalls, async gotchas
- `!!! info` — ClickHouse background, design decisions
- `!!! note` — Version requirements, optional features
- `??? example` — Collapsible extended examples

Examples for `object.md`:

```markdown
!!! warning "Awaiting Operations"
    All operations return awaitables. Always `await` the result:
    ```python
    result = await (prices * tax_rate)  # correct
    result = prices * tax_rate          # returns a coroutine, not an Object!
    ```

!!! tip "Scalar Broadcast"
    You can use plain Python numbers on either side of an operator:
    `obj * 2` and `2 * obj` both work.
```

## 5. Add Tabbed Content for Common Patterns

**Inspiration**: FastAPI uses tabs for Python version differences.

**Current**: `pymdownx.tabbed` extension configured but unused.

**Proposed**: Use tabs for local vs. distributed, sync vs. async patterns:

```markdown
=== "Local (chdb + SQLite)"

    ```bash
    pip install aaiclick
    python -m aaiclick setup
    ```

=== "Distributed (ClickHouse + PostgreSQL)"

    ```bash
    pip install "aaiclick[distributed]"
    export AAICLICK_CH_URL="clickhouse://user:pass@host:8123/db"
    export AAICLICK_SQL_URL="postgresql+asyncpg://user:pass@host:5432/db"
    ```
```

## 6. Add a Troubleshooting / FAQ Page

**Inspiration**: SQLAlchemy's error reference with root cause analysis.

**Current**: No troubleshooting docs. `technical_debt.md` exists but isn't in nav.

**Proposed**: `docs/troubleshooting.md` covering:

- "Object is stale" errors — what causes them, how to fix
- chdb hanging with `url()` function (already documented in `technical_debt.md`)
- Common async mistakes (forgetting `await`, using outside `data_context()`)
- Connection issues (wrong URL format, missing setup)
- ClickHouse type mismatches

## 7. Improve Cross-Page References

**Inspiration**: Both FastAPI and SQLAlchemy extensively link between pages.

**Current**: Minimal cross-referencing. Pages are mostly self-contained.

**Proposed**:
- Every example page should link to the relevant User Guide section
- User Guide sections should link to related examples
- API Reference should link back to User Guide explanations
- Add "See Also" sections at the bottom of key pages

## 8. Add a Glossary

**Inspiration**: SQLAlchemy's 100+ term glossary.

**Current**: ClickHouse-specific terms (chdb, Snowflake IDs, columnar tables) are unexplained.

**Proposed**: `docs/glossary.md` with terms like:

| Term              | Definition                                                          |
|-------------------|---------------------------------------------------------------------|
| Object            | Python wrapper for a ClickHouse table                               |
| View              | A read-only filtered/transformed projection of an Object            |
| DataContext        | Async context manager that manages Object lifecycle                |
| Snowflake ID      | Timestamp-encoded unique ID for distributed ordering               |
| chdb              | Embedded ClickHouse engine — no server needed                       |
| Scalar Broadcast  | Auto-converting Python numbers to single-value Objects             |

## 9. Add a "Why aaiclick?" / Comparison Page

**Inspiration**: FastAPI's comparison with other frameworks.

**Current**: README says "Pandas/SQLAlchemy-inspired" but doesn't elaborate.

**Proposed**: `docs/comparison.md` showing:

| Feature                | aaiclick          | Pandas             | Spark              | Dask               |
|------------------------|-------------------|--------------------|--------------------|--------------------|
| Compute Engine         | ClickHouse        | Python (NumPy)     | JVM                | Python (NumPy)     |
| Data Location          | Database           | In-memory          | Distributed memory | Distributed memory |
| Setup Complexity       | `pip install`      | `pip install`      | JVM + cluster      | `pip install`      |
| Scales Beyond Memory   | Yes (columnar DB)  | No                 | Yes                | Yes                |
| SQL Interop            | Native             | Limited            | SparkSQL           | Limited            |
| Orchestration Built-in | Yes (`@task/@job`) | No                 | No                 | No                 |

## 10. Improve `object.md` Structure

**Current**: 30KB single page mixing reference tables, implementation notes, and code examples.

**Proposed**:
- Keep the API Quick Reference table at the top (it's great)
- Move detailed sections into sub-pages or use collapsible details
- Add a "Common Patterns" section at the top with 5-6 most-used patterns
- Remove implementation references (e.g., "see `_ensure_object()`") from user-facing docs — those belong in contributor docs
- Add more inline examples showing input → output

## 11. Add Version/Changelog Documentation

**Inspiration**: SQLAlchemy's per-version "What's New" + migration guides.

**Current**: No version history in docs.

**Proposed**: `docs/changelog.md` linked from nav, following Keep a Changelog format.

## 12. Add a "Contributing" Page

**Current**: No contributor docs. `CLAUDE.md` has coding guidelines but isn't user-facing.

**Proposed**: Extract relevant parts of `CLAUDE.md` into `docs/contributing.md`:
- Development setup
- Test conventions
- Code style
- PR process

## 13. Fix Broken Navigation Links (CRITICAL BUG)

**Status**: Fixed in this branch (`mkdocs.yml` `site_url` updated).

`site_url` was set to `https://aaiclick.readthedocs.io` but ReadTheDocs serves under
`/en/latest/`. All internal links clicked from the homepage resolved to 404. Fixed by
changing `site_url` to `https://aaiclick.readthedocs.io/en/latest/`.

# Priority Ranking

| Priority | Improvement                        | Impact | Effort |
|----------|------------------------------------|--------|--------|
| P0       | Landing page overhaul              | High   | Low    |
| P0       | Add example output inline          | High   | Low    |
| P0       | Use admonitions throughout         | High   | Low    |
| P1       | Progressive tutorial (7 pages)     | High   | High   |
| P1       | Troubleshooting / FAQ page         | Medium | Medium |
| P1       | Cross-page references              | Medium | Low    |
| P2       | "Why aaiclick?" comparison page    | Medium | Medium |
| P2       | Add tabbed content                 | Medium | Low    |
| P2       | Glossary                           | Medium | Medium |
| P2       | Improve `object.md` structure      | Medium | Medium |
| P3       | Changelog documentation            | Low    | Low    |
| P3       | Contributing page                  | Low    | Low    |
