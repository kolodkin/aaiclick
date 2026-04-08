---
name: shortify
description: Review and shorten markdown docs — cut wordiness, reduce redundancy, reference code instead of duplicating. Use when user asks to trim, shorten, tighten, shortify, or condense documentation.
---

# shortify: Shorten Markdown Documentation

Review markdown files and make them concise by removing wordiness, redundancy,
and duplication with existing code.

## Invocation

```
/shortify [file-or-glob]
```

- `/shortify docs/object.md` — shortify a specific file
- `/shortify docs/*.md` — shortify all docs in a directory
- `/shortify` — no argument: ask which files to shortify

## Review Checklist

For each document, apply these cuts in order:

### 1. Kill Redundancy

- **Remove text that restates what code already shows.** If a class/function exists
  in the codebase, link to it (`see ClassName in path/to/file.py`) instead of
  re-explaining its internals.
- **Collapse duplicate examples.** One example per concept. Delete the rest.
- **Merge overlapping sections.** If two sections say the same thing differently,
  keep the clearer one.

### 2. Cut Wordiness

- Replace wordy phrases with direct ones:
  - "In order to" -> "To"
  - "It is important to note that" -> delete
  - "This allows you to" -> delete or rephrase
  - "As mentioned above/below" -> delete
- Remove filler sentences that add no information.
- Prefer bullet lists over prose paragraphs for reference material.
- Remove "obvious" admonitions that restate surrounding text.

### 3. Tighten Structure

- **Flatten heading depth.** Prefer `#` and `##`. Avoid `###` and deeper — restructure instead.
- **Delete empty/trivial sections.** If a section says "see above" or has one sentence, merge it.
- **Shorten introductions.** A doc title + one sentence is enough. No "welcome to" or "this document describes".

### 4. Reference Code, Don't Duplicate

- When implementation exists, replace inline code blocks with references:
  ```markdown
  # GOOD
  See `DataContext.query()` in `aaiclick/data/data_context.py`.

  # BAD
  ```python
  class DataContext:
      async def query(self, sql: str) -> Result:
          # 20 lines of implementation detail
  ```
  ```
- Keep short usage examples (3-5 lines). Delete long inline implementations.
- For API docs: show the call signature + one example, not the internals.

### 5. Preserve

- **Do NOT remove** content that has no other source (design decisions, rationale, gotchas).
- **Do NOT remove** usage examples that show non-obvious patterns.
- **Do NOT change** code that the doc references — only change the prose.
- **Keep** `!!! warning` admonitions for genuine pitfalls.

## Output

After trimming each file:
1. Show a short summary of what was cut and why.
2. Show line count before/after.
3. Stage and commit changes if the user approves.
