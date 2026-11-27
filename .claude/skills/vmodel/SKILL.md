---
name: vmodel
description: Use when a change alters behavior in this repo, before writing its doc, test or code; when adding or changing a test (pytest, vitest or `test_e2e/`); and when adding or editing a page under `docs/user_guide/` or `docs/designs/`.
---

# V-model

Extends the `devpowers:vmodel` skill: load it first for the method (markers,
the steps down and up the V, where a decision goes). This skill holds only
what is particular to aaiclick. When a `docs/superpowers/` plan splits the
work into tasks, each task follows both.

| Layer | Doc of record | A test covers | Proved in |
|---|---|---|---|
| System | `docs/user_guide/` | what a user can call, run or see: the Object API, the task API, the CLI, the UI | `aaiclick/**/test_*.py` through the public API; `test_e2e/` for the deployed package and the UI |
| Integration | `docs/designs/` | subsystems working together: orchestration, oplog, internal API, server, backends | `aaiclick/**/test_*.py`, `src/**/*.test.ts(x)` |
| Unit | the module's docstring (Python) or leading comment (TypeScript) | one module's logic | the test file beside the module, no ID |

There is no user layer: a new user need enters at system. No layers share
tests.

The docs root is `docs/`: `user_guide/object/concat` is defined in
`docs/user_guide/object.md` as `<!-- req: concat -->`, and named by a test as
`# req: user_guide/object/concat` (`//` in TypeScript).

## Steps

- **Entry layer.** A public signature, a CLI flag, an operator's result or a
  UI screen enters at system. A change to how tasks are scheduled, how the
  oplog records, or an internal API route enters at integration.
- **Go up.**
  - the touched test files: `uv run pytest <path>` (chdb + SQLite, no
    infrastructure), `npm test` for `src/`;
  - `uv run pytest` for the whole backend suite;
  - `test_e2e/` runs only in GitHub Actions: push, then the `check-pr` skill.

## Tagging

No checker enforces markers here, and most pages and tests predate them. Tag
what a change touches: the statement it edits and the test that proves it.
Leave the rest untagged.

## Writing a doc or a test

- Docs: the `markdown-style` skill. Unbuilt behavior goes in
  `docs/designs/future.md`, not on a layer page.
- Tests: the `python-testing-style` skill; fixtures and the chdb session
  constraint are in `docs/contributor/testing.md`.
