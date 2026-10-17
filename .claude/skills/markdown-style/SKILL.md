---
name: markdown-style
description: Project markdown rules on top of shortify — heading style, table formatting, admonitions, code ↔ doc links. TRIGGER when writing or editing `.md` files in subdirectories (`docs/`, `aaiclick/**`). Skip root-level `.md` files (`CLAUDE.md`, `README.md`, `CHANGELOG.md`).
---

# markdown-style

Project-specific rules on top of the `shortify` skill. Run `shortify` for the generic pass (redundancy, wordiness, heading depth, short examples); this skill adds only what is particular to aaiclick, and overrides shortify's "Reference Code" step where they differ.

Apply when writing or editing `.md` files in subdirectories. Skip root-level `.md` files — they have their own conventions.

**Quality reference**: [FastAPI docs](https://fastapi.tiangolo.com/) — progressive disclosure, concise admonitions, copy-paste-ready examples with output shown inline.

## Code ↔ doc links — module level only

Overrides shortify's "Reference Code, Don't Duplicate" step. Links name modules or packages, never functions, classes, private helpers or line numbers, and only where a module is the clear home of a section — not for small details.

- **Design docs** (`docs/designs/`): optionally one `**Implementation**:` line right under a section heading, naming one module or package path.
- **User guides** (`docs/user_guide/`): no `**Implementation**:` lines. Code paths appear only inside prose that needs them: an example file to open, a migration to read.
- **Code**: the module docstring names the doc section the module implements. No back-links on function or class docstrings, tests, or small helper modules.

```markdown
# BAD — symbols, private helpers, file lists
**Implementation**: `aaiclick/auth/models.py` — see `ApiToken`, `ScopeLevel`; `aaiclick/internal_api/api_tokens.py` — see `_mint_ceiling`; `src/views/Tokens.tsx`.

# GOOD — design doc, one module
# API Tokens

**Implementation**: `aaiclick/internal_api/api_tokens.py`
```

```python
# GOOD — module docstring points at the section it implements
"""Decorators for defining tasks and jobs.

See docs/designs/orchestration.md — "Decorators".
"""
```

Once code exists, the doc keeps intent and decisions and drops what the code already shows. Doc → doc pointers (`**Design**: docs/designs/auth.md`) are fine. No status icons: unimplemented work lives in `docs/designs/future.md`, not inline ⚠️ markers.

## Heading style — setext title, ATX sections

```markdown
# GOOD - setext title + ATX sections (one level deep)
Document Title
---

# Section One

## Subsection

# BAD - ATX title with deep nesting
# Document Title

## Section One

### Subsection
```

- Document title: setext underline with `---`
- Top-level sections: `#`, subsections: `##` (depth limit per shortify)

## Tables — align columns with padding

```markdown
# GOOD - aligned columns, padded with spaces
| Guard                                   | Scenario                                                  |
|-----------------------------------------|-----------------------------------------------------------|
| `sys.is_finalizing()`                   | Interpreter shutdown — skip to avoid thread safety issues |
| `_data_ctx_ref is None`                 | Object was never registered                               |

# BAD - minimal separators, hard to read
| Guard | Scenario |
|-------|----------|
| `sys.is_finalizing()` | Interpreter shutdown — skip to avoid thread safety issues |
| `_data_ctx_ref is None` | Object was never registered |
```

## Admonitions — only at genuine pitfall points

Use `!!! tip`, `!!! warning`, `??? info` only where a user would hit a confusing error without the callout. Never for emphasis, decoration, or restating what surrounding prose already says. Collapsible `???` for optional context.

```markdown
# GOOD — real pitfall, saves debugging time
!!! warning "`or_where()` requires a prior `where()`"
    Calling `or_where()` without a preceding `where()` raises `ValueError`.

# GOOD — optional context, reader can skip
??? info "Which deployment mode?"
    Start with the default (chdb + SQLite) — it needs zero setup.

# BAD — restating what the code already shows
!!! tip
    Use `await` to get the result of an operation.

# BAD — decorating a reference table
!!! info "ClickHouse uses RE2 regex syntax"
    No lookaheads or lookbehinds.
```
