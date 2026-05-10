---
name: markdown-style
description: Project markdown style rules — heading style, table formatting, admonitions, implementation references. TRIGGER when writing or editing `.md` files in subdirectories (`docs/`, `aaiclick/**`). Skip root-level `.md` files (`CLAUDE.md`, `README.md`, `CHANGELOG.md`).
---

# markdown-style

Apply when writing or editing `.md` files in subdirectories. Skip root-level `.md` files — they have their own conventions.

**Quality reference**: [FastAPI docs](https://fastapi.tiangolo.com/) — progressive disclosure, concise admonitions, copy-paste-ready examples with output shown inline.

## Implementation references — name, not line number

Line numbers go stale. Refer to classes, methods, or functions by name.

```markdown
# BAD - line numbers become stale
**Implementation**: `aaiclick/orchestration/context.py:129-175`

# GOOD - method names are stable
**Implementation**: `aaiclick/orchestration/context.py` - see `OrchContext.apply()` method
```

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
- Top-level sections: `#`
- Subsections: `##`
- Avoid `###` and deeper — restructure instead.

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
