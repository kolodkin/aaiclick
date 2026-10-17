---
name: markdown-style
description: Project markdown rules on top of shortify — heading style, table formatting, admonitions, code ↔ doc links. TRIGGER when writing or editing `.md` files in subdirectories (`docs/`, `aaiclick/**`). Skip root-level `.md` files (`CLAUDE.md`, `README.md`, `CHANGELOG.md`).
---

# markdown-style

Project-specific rules on top of the `shortify` skill. Run `shortify` for the generic pass (redundancy, wordiness, heading depth, short examples); this skill adds only what is particular to aaiclick, and overrides shortify's "Reference Code" step where they differ.

Apply when writing or editing `.md` files in subdirectories. Skip root-level `.md` files — they have their own conventions.

**Quality reference**: [FastAPI docs](https://fastapi.tiangolo.com/) — progressive disclosure, concise admonitions, copy-paste-ready examples with output shown inline.

## Code ↔ doc links — code points at docs

Overrides shortify's "Reference Code, Don't Duplicate" step. The default pointer lives in code: the implementing module's docstring names the doc section (or the class/function docstring, when one module serves several sections).

- **User guides** (`docs/user_guide/`): no `**Implementation**:` lines. Code paths appear only inside prose that needs them: an example file to open, a migration to read.
- **Design docs** (`docs/designs/`): an `**Implementation**:` line is optional. Keep one only when it orients the reader. It names one high-level module or package, or a meaningful public function or class, never private helpers (`_mint_ceiling`) or an exhaustive file list.
- Once code exists, the doc keeps intent and decisions and drops what the code already shows.

```markdown
# BAD — exhaustive, private helpers, goes stale
**Implementation**: `aaiclick/auth/models.py` — see `ApiToken`, `ScopeLevel`; `aaiclick/internal_api/api_tokens.py` — see `_mint_ceiling`; `src/views/Tokens.tsx`.

# GOOD — design doc, one high-level pointer (or none)
**Implementation**: `aaiclick/internal_api/api_tokens.py` — see `create_token`
```

```python
# GOOD — the module says which design section it implements
"""Decorators for defining tasks and jobs.

See docs/designs/orchestration.md — "Decorators".
"""
```

Doc → doc pointers (`**Design**: docs/designs/auth.md`) are fine. Never cite line numbers. No status icons: the code's back-link signals a feature is built, and unimplemented work lives in `docs/designs/future.md`, not inline ⚠️ markers.

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
