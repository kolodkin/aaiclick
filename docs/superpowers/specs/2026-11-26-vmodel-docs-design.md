V-model docs: layout, markers and check
---

# Intent

Docs sit on the right V-model layer, and each spec statement links to the test that proves it, while the published site (mkdocs on Read the Docs) stays easy for users to read.

Today no `req` marker exists in `docs/` or in any test, nothing checks one, the User Guide nav is a flat list of 14 pages, and the UI and login are documented only in the unpublished `docs/designs/`.

# Goals

- The User Guide nav is grouped by surface; groups mirror the test packages.
- The two largest pages are split so one page matches one cluster of test files.
- User-visible behavior of the UI and login has a published page.
- Tagged statements and tests are kept in step by a check in the test suite.

Non-goals: tagging Tutorial or Examples (they teach; their scripts prove them), renaming `docs/designs/`, per-test tags, splitting reference from narrative inside a page.

Anti-goals: a published page URL that stops resolving; markers visible to readers; markers that restate headings with no test behind them.

# Constraints

- `mkdocs.yml` excludes `designs/` and `drafts/`. Published pages are the system layer, `docs/designs/` is the integration layer. This stays.
- CI runs `uv run mkdocs build --strict`, so a broken internal link or anchor fails the build.
- No new docs dependency.
- `markdown-style` and `python-testing-style` skills apply.

# Approaches

| Approach                    | Trade-off                                                                   |
|-----------------------------|-----------------------------------------------------------------------------|
| Regroup, split, add, tag    | Picked. Each phase is one reviewable PR and improves the site on its own    |
| Tag in place, no moves      | No URL risk, but 750-line pages and the missing UI pages stay               |
| Full backfill in one change | One huge diff; a granularity mistake is repeated across 224 test files (205 pytest, 19 vitest)      |
| Reorganize docs by layer    | Users would browse by V-model layer instead of by what they are trying to do |

# Design

## Nav groups

Regroup the `User Guide` entry in `mkdocs.yml`; no file moves in this step.

| Group         | Pages                                                                        |
|---------------|------------------------------------------------------------------------------|
| Data          | Object API, Ingest, ObjectContext                                            |
| Orchestration | Orchestration, Task API (+ Python, Java), Task Interface, Lineage            |
| Operations    | Deployment, Container Images, Sandbox, DNS Rebinding, Example Projects       |
| Web UI        | UI, Access (added in phase 4)                                                |

## Page splits

A split page becomes a folder whose `index.md` keeps the old URL: `user_guide/object.md` and `user_guide/object/index.md` both publish at `user_guide/object/`. Page URLs survive; anchors into moved sections do not. In-repo links are fixed in the same change (about 40 sites; `--strict` finds them, and the quoted-heading pointers in module docstrings are found by grep).

| Folder                      | Pages                                                                                                 |
|-----------------------------|-------------------------------------------------------------------------------------------------------|
| `user_guide/object/`        | `index` (Overview, API Quick Reference), `operators`, `lazy_results`, `data_retrieval`, `export`, `blob_storage`, `views` (with `copy()`) |
| `user_guide/orchestration/` | `index` (Quick Start), `tasks_and_jobs`, `running_jobs`, `runners`, `parallel_operators`, `managing_jobs` (with Task Logs) |

The API Quick Reference table stays on `index` and links into the new pages, so it is the map. Sections that only point at a design page ("Internal Design", "Operation Provenance") shrink to a link on `index`.

## New system pages

| Page                    | Lifted from                                                                   |
|-------------------------|-------------------------------------------------------------------------------|
| `user_guide/ui.md`      | `designs/ui.md` (Layout, Navigation, Modes), `designs/viewer.md` (Scope Model) |
| `user_guide/access.md`  | `designs/auth.md`: roles, API tokens, MFA, password reset, admin bootstrap    |

Only what a user sees or does moves. The design page keeps the mechanics and links to the new page instead of repeating it.

## Markers

One marker per `##` section (or `#` section with no subsections), on the line under the heading. HTML comments do not render.

```markdown
## Chained WHERE Clauses
<!-- req: where -->
```

The ID is the page path plus the marker: `user_guide/object/views/where`. An `index.md` drops out of the path: a marker on `user_guide/object/index.md` is `user_guide/object/<marker>`.

A section no test proves gets no marker. Tagging is not a reason to write tests; an unmarked section is the visible list of what is unproven.

This spec is the record for the marker format. The base `devpowers:vmodel` skill the project skill extends is not installed, so nothing here depends on it.

## Test tags

One comment line per ID, directly under the module docstring. A tag covers the whole file; a file may name several IDs.

```python
"""Tests for chained where() / or_where()."""

# req: user_guide/object/views/where
```

A test file names IDs from one layer only ("no layers share tests"). A `docs/designs/` marker is tagged by an integration test (orchestration, oplog, internal API, server, `src/`), never by adding a second ID to a file already tagged for a user-guide page.

For a test file with no matching statement, pick one: the behavior is user-visible and undocumented, so add the statement and tag; or it proves one module's logic, so it is unit layer with no ID.

## Check

`aaiclick/test_vmodel.py`, one pytest module in the existing test workflow. It scans `docs/**/*.md` for markers and `aaiclick/`, `src/`, `test_e2e/` for `# req:` and `// req:` tags, and fails when:

- a tag names an ID with no marker;
- a marker has no tag;
- a page defines the same marker twice.

A page with no markers is ignored, so rollout is page by page. The scan functions take a root path and are tested against `tmp_path` trees, one case per rule.

Top-level `aaiclick/test_*.py` files run in CI only through the "Misc local" path list in `.github/workflows/_test-reusable.yaml`, so the module is added there. Tests ship in the wheel, so it skips when `docs/` is absent.

`.claude/skills/vmodel/SKILL.md` "Tagging" is updated to describe the check.

# Phases

Each phase is one PR and leaves the site and the check green.

| Phase | Content                                                              |
|-------|----------------------------------------------------------------------|
| 1     | Nav groups, the check, the skill update                              |
| 2     | Split `object.md`; tag its pages and `aaiclick/data/object/` tests   |
| 3     | Split `orchestration.md`; tag its pages and tests                    |
| 4     | `ui.md` and `access.md`; tag with `test_e2e/` tests                  |
| 5     | Remaining User Guide pages, then `docs/designs/`, one page per PR    |

# Left to the builder

- Marker names, and the exact section-to-page cut inside each split.
- Which passages in a user page are integration-layer and move to `docs/designs/`.
- Order within phase 5.
