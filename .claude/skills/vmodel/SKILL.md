---
name: vmodel
description: Use when a change alters behavior in this repo, before writing its doc, test or code; when adding or changing a test (pytest, vitest or `test_e2e/`); and when adding or editing a page under `docs/user_guide/`, `docs/architecture/` or `docs/designs/`.
---

# V-model

Extends the `devpowers:vmodel` skill: load it first for the method (markers,
the steps down and up the V, where a decision goes). This skill holds only
what is particular to aaiclick. When a `docs/superpowers/` plan splits the
work into tasks, each task follows both.

| Layer          | Doc of record                                                   | Test layer  | Proved in                                                                                                                  |
|----------------|-----------------------------------------------------------------|-------------|----------------------------------------------------------------------------------------------------------------------------|
| User           | `docs/user_guide/` (published)                                  | Acceptance  | `aaiclick/**/examples/` (checked output), `test_e2e/`, tests that use only `from aaiclick import ...`                       |
| Architecture   | `docs/architecture/`                                            | System      | `aaiclick/server/**`, `aaiclick/internal_api/**`, `aaiclick/test_cli.py`; `server/` and `internal_api/` run on both backends (`test-local`, `test-dist`), root tests on local only |
| Design         | `docs/designs/`                                                 | Integration | `aaiclick/**/test_*.py`, `src/**/*.test.ts(x)`                                                                             |
| Implementation | the module's docstring (Python) or leading comment (TypeScript) | Unit        | the test file beside the module, no ID                                                                                     |

Outside the V, with no IDs: `docs/contributor/` (testing, debugging, technical
debt), `docs/drafts/`, `docs/superpowers/`, Tutorial and Examples pages.

User and Architecture share tests: a public-API test that already walks an
architecture statement (a result on both backends, a table name) names both
IDs. No other pair shares.

The docs root is `docs/`: `user_guide/object/chained-where` is defined in
`docs/user_guide/object.md` as `<!-- req: chained-where -->`, and named by a
test as `# req: user_guide/object/chained-where` (`//` in TypeScript),
directly above the test function.

## Steps

- **Entry layer.**
  - User: a public signature, a CLI flag, an operator's result, a UI screen.
  - Architecture: table naming, a wire contract (REST, MCP, CLI to internal
    API), a role ceiling, what a backend supports.
  - Design: how tasks are claimed, how the oplog records, how the viewer
    queries an object.
- **Go up.**
  - the touched test files: `uv run pytest <path>` (chdb + SQLite, no
    infrastructure), `npm test` for `src/`;
  - `uv run pytest` for the whole backend suite;
  - `uv run pytest aaiclick/test_vmodel.py`, the checker;
  - `test_e2e/` runs only in GitHub Actions: push, then the `check-pr` skill.

## Tagging

`aaiclick/test_vmodel.py` scans layer pages for markers and test files for
tags, and fails when:

- a tag names an ID with no marker;
- a marker has no tag;
- a page defines the same slug twice, a slug is not kebab-case, or a
  `<!-- req` comment is malformed (inline, wrong case);
- a tag sits outside its layer's test roots: `aaiclick/` or `test_e2e/` for
  user and architecture, `aaiclick/` or `src/` for design. The roots are
  wider than "Proved in" (the user and architecture layers share tests);
  placing a test inside its "Proved in" location is the author's job.

A page with no markers is ignored. Tagging grows with changes: tag the
statement a change edits and the test that proves it; leave the rest
untagged.

## Writing a doc or a test

- Docs: the `markdown-style` skill. Unbuilt behavior goes in
  `docs/designs/future.md`, not on a layer page.
- Tests: the `python-testing-style` skill; fixtures and the chdb session
  constraint are in `docs/contributor/testing.md`.
