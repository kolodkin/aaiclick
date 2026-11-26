V-model: Object slice and marker check
---

# Intent

Docs sit on the right V-model layer, and each spec statement links to the test that proves it. Today no `req` marker exists in `docs/` or in any test, and nothing checks one.

Start with one slice, the Object API, to settle marker granularity before repeating it on every page.

# Goals

- `docs/user_guide/object.md` carries `req` markers; the tests in `aaiclick/data/object/` name them.
- A check fails when a marker and a test drift apart.
- Content on the wrong layer in `object.md` moves to its layer page.

Non-goals: tagging any other page, new user-guide pages for the UI, viewer or login (later slices), per-test tags.

Anti-goal: markers that restate headings without a test behind them.

# Approaches

| Approach                    | Trade-off                                                              |
|-----------------------------|------------------------------------------------------------------------|
| One slice, then roll out    | Picked. Granularity is tested on one page before 33 repeat it          |
| Full backfill with CI check | One huge diff; granularity mistakes are repeated across 224 test files |
| Docs first, markers later   | Fixes layering, leaves the spec to test link untouched                 |

# Design

## Marker granularity

One marker per `##` section (or `#` section with no subsections) of `object.md`, on the line under the heading:

```markdown
## Aggregation Operators
<!-- req: aggregation -->
```

The ID is the page path plus the marker: `user_guide/object/aggregation`.

## Test tags

One comment line per ID, directly under the module docstring. The files in `aaiclick/data/object/` already split by feature, so a tag covers the whole file:

```python
"""Parametrized tests for aggregation operations..."""

# req: user_guide/object/aggregation
```

A file may name several IDs. Sections documented in `ingest.md` (`insert`, `concat`, `join`) stay untagged until that page's slice.

## Tests without a statement

For each untagged test file in `aaiclick/data/object/`, pick one:

- the behavior is user-visible and undocumented: add the statement to `object.md`, then tag;
- it proves one module's logic: unit layer, no ID.

## Check

`aaiclick/test_vmodel.py`, one pytest module, runs in the existing test workflow. It scans `docs/**/*.md` for markers and `aaiclick/`, `src/`, `test_e2e/` for `# req:` and `// req:` tags, and fails when:

- a tag names an ID with no marker;
- a marker has no tag;
- a page defines the same marker twice.

Untagged pages have no markers, so the check ignores them and rollout stays page by page.

## Skill update

`.claude/skills/vmodel/SKILL.md` "Tagging": replace "No checker enforces markers here" with the check and its three rules.

# Left to the builder

- Marker names per section.
- Which `object.md` passages are integration-layer and move to `docs/designs/`.
- Rollout order after this slice; suggested: `ingest.md`, `object_context.md`, `orchestration.md`, then `docs/designs/`.
