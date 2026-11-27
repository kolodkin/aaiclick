# V-model Foundation (Phases 0–1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task (the project's CLAUDE.md requires inline execution). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the defects the layer review found, and lay the four-layer V-model foundation: contributor folder, project skill, marker check, grouped nav.

**Architecture:** Docs and CI config edits, plus one pytest module (`aaiclick/test_vmodel.py`) that scans layer pages for `req` markers and test files for `req` tags and reports mismatches. A second small test (`aaiclick/test_ci_paths.py`) keeps every test file inside a CI path list. Phases 2–5 of the spec get their own plans after this lands.

**Tech Stack:** Python 3.11+, pytest, mkdocs-material 1.6, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-11-26-vmodel-docs-design.md`

## Global Constraints

- Develop on branch `ccr-696e1f2e-0ne5sk`; push there only. No PR unless the user asks.
- `mkdocs.yml` publishes `user_guide/`, Tutorial and Examples only; `designs/`, `drafts/`, `architecture/`, `contributor/` stay excluded.
- `uv run mkdocs build --strict` must pass after every task that touches `docs/` or `mkdocs.yml`.
- No new dependency.
- All imports at the top of the file; NamedTuple over plain tuples; no `Any` (CLAUDE.md).
- Docs follow the `markdown-style` skill (setext title, ATX sections, aligned tables).
- Marker: `<!-- req: <slug> -->` on its own line above the statement; slug is kebab-case `[a-z0-9]+(-[a-z0-9]+)*`.
- ID: page path under `docs/` without `.md`, then `/<slug>`, e.g. `user_guide/object/chained-where`.
- Tag: `# req: <id>[, <id>...]` (Python) or `// req: ...` (TypeScript), directly above the test function.
- Commit trailer on every commit:
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` and
  `Claude-Session: https://claude.ai/code/session_01LcC1uAdwzdrETUDd5YMXn9`.

## Review Focus

1. A marker with a malformed slug (`<!-- req: Chained_Where -->`) must be reported, not silently ignored. → Task 5 case `bad-slug`.
2. A tag list with extra spaces or a trailing comma (`# req: a/b/c ,  a/b/d,`) parses to two clean IDs. → Task 5 case `messy-tag-list`.
3. A marker on a non-layer page (`docs/superpowers/`, `docs/drafts/`, `docs/contributor/`) is ignored, so specs and plans can show marker syntax. → Task 5 case `non-layer-page`.
4. Test-like files under any `node_modules/` are never scanned. → Task 5 case `node-modules`.
5. The same slug on two different pages is two IDs, not a duplicate. → Task 5 case `same-slug-two-pages`.

---

### Task 1: Every test file runs in CI

About 180 tests (`aaiclick/sandbox/`, `aaiclick/viewer/`, eight root `test_*.py`) are in no `test-paths` list. Fix the list and add a guard so it cannot happen again.

**Files:**
- Modify: `.github/workflows/_test-reusable.yaml:122-126` (the `Misc local` entry)
- Create: `aaiclick/test_ci_paths.py`

**Interfaces:**
- Produces: `Misc local` test-paths covers `aaiclick/test_*.py`, so later root test modules (Task 5's `aaiclick/test_vmodel.py`) run in CI with no further edit.

- [ ] **Step 1: Write the failing test** in `aaiclick/test_ci_paths.py`

```python
def test_every_test_file_is_in_a_ci_group():
    uncovered = [p for p in all_test_files() if not any(covers(entry, p) for entry in ci_test_paths())]
    assert uncovered == []
```

`ci_test_paths() -> list[str]`: every whitespace-separated entry of each `test-paths: "..."` line in `.github/workflows/_test-reusable.yaml`, read with a regex (no YAML parser; anchors such as `*data-dist-paths` contribute nothing and are skipped). `all_test_files() -> list[str]`: `aaiclick/**/test_*.py` as repo-relative POSIX paths. `covers(entry: str, path: str) -> bool`: `fnmatch(path, entry)` for a glob, `path.startswith(entry)` for a directory (ends in `/`), equality otherwise. Skip the module with `pytest.mark.skipif` when the workflow file is absent (tests ship in the wheel).

- [ ] **Step 2: Run it, expect FAIL listing the 12 uncovered files**

Run: `uv run pytest aaiclick/test_ci_paths.py -v`
Expected: FAIL; the diff names `aaiclick/sandbox/test_parse.py`, `aaiclick/viewer/test_scope.py`, `aaiclick/test_locks.py` and nine others.

- [ ] **Step 3: Widen `Misc local`**

Set its `test-paths` to `"test_e2e/compose/test_env_init.py aaiclick/test_*.py aaiclick/sandbox/ aaiclick/viewer/"` (the run-pytest action expands the glob unquoted in bash). Update its comment to: backend-independent tests with no group of their own: the compose `env.init.sh`, root modules, the sandbox and the viewer.

- [ ] **Step 4: Run the guard and the newly covered tests on the Misc install**

Run: `uv sync && uv run pytest aaiclick/test_ci_paths.py aaiclick/test_*.py aaiclick/sandbox/ aaiclick/viewer/ -q`
Expected: all pass (180 + the guard). If a file fails only for a missing extra, set the `Misc local` `extras` to that extra (e.g. `--extra server`) and rerun with `uv sync --extra <name>`.

- [ ] **Step 5: Commit** — `git add .github/workflows/_test-reusable.yaml aaiclick/test_ci_paths.py` with message `Run every test file in CI and guard the path lists`.

---

### Task 2: Fix doc defects and make mkdocs catch broken anchors

**Files:**
- Modify: `mkdocs.yml` (add `validation`)
- Modify: `docs/user_guide/object.md:740`
- Modify: `aaiclick/data/object/object.py:969`
- Modify: `docs/designs/task_log_retirement.md` (the "Capture per runner" paragraph after the table)
- Modify: `docs/designs/deployment.md:29-31`
- Modify: `docs/designs/auth.md` ("Roles" section)
- Modify: `docs/user_guide/orchestration.md:117-118`
- Modify: `docs/user_guide/ingest.md:251-255`, `docs/designs/future.md`

- [ ] **Step 1: Turn on anchor validation** — add to `mkdocs.yml`:

```yaml
validation:
  anchors: warn
```

- [ ] **Step 2: Build, expect FAIL on at least `object_context.md#schema-storage`**

Run: `uv run mkdocs build --strict 2>&1 | grep -i anchor`
Expected: a warning naming `user_guide/object.md` → `object_context.md#schema-storage`; note every other anchor it reports.

- [ ] **Step 3: Fix each reported anchor.** For `object.md:740`, point the link at `https://github.com/kolodkin/aaiclick/blob/main/docs/designs/object_context.md#schema-storage` (designs are not on the site; other user pages link them the same way). Fix any other reported anchor to the heading it was meant for.

- [ ] **Step 4: Fix the remaining defects, one edit each**
  - `object.py:969`: `"Row Order" in docs/user_guide/object.md` → `"Row Order" in docs/user_guide/ingest.md`.
  - `task_log_retirement.md`: replace "tagged with their source stream (stdout → `INFO`, stderr → `WARNING`; …)" with "tagged with their source stream in the `stream` column (`stdout` / `stderr`; the docker/kubectl wrappers keep the split for non-TTY runs)". This matches `_TASK_LOG_COLS` in `aaiclick/orchestration/logging.py`.
  - `designs/deployment.md`: the Role grants `pods` create/get/list/watch/delete, `pods/log` get, `pods/exec` create/get and `secrets` create/delete, matching `deploy/helm/aaiclick/templates/rbac.yaml`.
  - `designs/auth.md` Roles: add one sentence under the table: every role may submit to the sandbox from a signed-in session (`require_session` in `aaiclick/server/routers/sandbox.py`); see `docs/user_guide/sandbox.md`.
  - `user_guide/orchestration.md:117-118`: "require an admin JWT" → "require admin scope, from a session or an API token".
  - `ingest.md`: delete "Distributed considerations"; add its two paragraphs to `future.md` under `# Deferred` as `## Sharded Joins`, keeping their content.

- [ ] **Step 5: Verify**

Run: `uv run mkdocs build --strict && uv run pytest aaiclick/data/object/test_concat_parametrized.py -q`
Expected: build succeeds with no anchor warnings; tests pass (docstring-only change).

- [ ] **Step 6: Commit** — the files above, message `Fix doc defects found by the layer review; validate anchors`.

---

### Task 3: Move contributor docs out of the V

**Files:**
- Move: `docs/designs/testing.md`, `docs/designs/debugging.md`, `docs/designs/technical_debt.md` → `docs/contributor/`
- Modify: `mkdocs.yml` (`exclude_docs` adds `contributor/`)
- Modify: every reference found in Step 2 (at least `CLAUDE.md:15`, `pyproject.toml:149,176,181`, `aaiclick/orchestration/orch_context.py:436`, `aaiclick/orchestration/execution/test_remote_result.py:7`, `aaiclick/orchestration/execution/test_kubernetes_worker.py:5`, `aaiclick/data/object_context/chdb_client.py:261`, `aaiclick/testing.py:207,490`, `.claude/skills/vmodel/SKILL.md:48`)

- [ ] **Step 1: Move the files** — `git mv` each of the three into `docs/contributor/`; add `contributor/` to `exclude_docs`.

- [ ] **Step 2: Rewrite references**

Run: `grep -rnE "designs/(testing|debugging|technical_debt)" --exclude-dir=node_modules --exclude-dir=.git --exclude-dir='*.egg-info' --exclude-dir=superpowers .`
Replace `designs/` with `contributor/` in each hit, keeping quoted section titles as they are. Also fix relative links inside the three moved pages (e.g. `[x](orchestration.md)` → `[x](../designs/orchestration.md)`).

- [ ] **Step 3: Verify**

Run: the grep from Step 2 (expect no output), then `uv run mkdocs build --strict`.
Expected: no hits; build passes.

- [ ] **Step 4: Commit** — message `Move testing, debugging and technical debt docs to docs/contributor/`.

---

### Task 4: Rewrite the project vmodel skill for four layers

**Files:**
- Modify: `.claude/skills/vmodel/SKILL.md`

**Interfaces:**
- Produces: the layer table and `LAYER_TEST_ROOTS` values Task 5 enforces.

- [ ] **Step 1: Replace the layer table and its rules** with:

| Layer          | Doc of record                      | Test layer  | Proved in                                                                                              |
|----------------|------------------------------------|-------------|--------------------------------------------------------------------------------------------------------|
| User           | `docs/user_guide/` (published)     | Acceptance  | `aaiclick/**/examples/` (checked output), `test_e2e/`, tests that use only `from aaiclick import ...`   |
| Architecture   | `docs/architecture/`               | System      | `aaiclick/server/**`, `aaiclick/internal_api/**`, `aaiclick/test_cli.py`; both backends via `test-local` and `test-dist` |
| Design         | `docs/designs/`                    | Integration | `aaiclick/**/test_*.py`, `src/**/*.test.ts(x)`                                                         |
| Implementation | module docstring / leading comment | Unit        | the test file beside the module, no ID                                                                 |

Keep the rest of the file's shape (extends `devpowers:vmodel`, only project-specific content). State:
- outside the V: `docs/contributor/`, `docs/drafts/`, `docs/superpowers/`;
- shared pair: User and Architecture — a public-API test that already walks an architecture statement names both IDs; no other pair shares;
- docs root `docs/`; ID example `user_guide/object/chained-where`; tag directly above the test function;
- entry-layer examples: public signature, CLI flag, UI screen → user; table naming, wire contract, role ceiling → architecture; task claiming, oplog recording → design;
- go-up commands: unchanged, plus "then `uv run pytest aaiclick/test_vmodel.py`";
- Tagging: replace "No checker enforces markers here" with the four check rules from Task 5 and "a page with no markers is ignored; tagging grows with changes";
- Writing: fixtures and the chdb session constraint are in `docs/contributor/testing.md`.

- [ ] **Step 2: Verify** — `grep -n "no user layer\|No checker" .claude/skills/vmodel/SKILL.md` returns nothing.

- [ ] **Step 3: Commit** — message `vmodel skill: four layers per devpowers:vmodel`.

---

### Task 5: Marker check

**Files:**
- Create: `aaiclick/test_vmodel.py`

**Interfaces:**
- Consumes: Task 1 (root `test_*.py` already in the `Misc local` CI group), Task 4 (layer roots).
- Produces, all in `aaiclick/test_vmodel.py`:
  - `class Marker(NamedTuple): id: str; path: Path; line: int`
  - `class Tag(NamedTuple): id: str; path: Path; line: int`
  - `LAYER_PAGES = ("user_guide", "architecture", "designs")`
  - `LAYER_TEST_ROOTS: dict[str, tuple[str, ...]] = {"user_guide": ("aaiclick/", "test_e2e/"), "architecture": ("aaiclick/", "test_e2e/"), "designs": ("aaiclick/", "src/")}`
  - `find_markers(docs_root: Path) -> tuple[list[Marker], list[str]]` — markers, plus problems for malformed slugs
  - `find_tags(repo_root: Path) -> list[Tag]`
  - `check(repo_root: Path) -> list[str]` — one human-readable problem per line, empty when consistent

Scanning rules:
- Markers: `docs/<layer>/**/*.md` for each `LAYER_PAGES` entry; a line matching `^<!--\s*req:\s*(.+?)\s*-->\s*$`; a slug failing `^[a-z0-9]+(-[a-z0-9]+)*$` is a `bad slug` problem.
- Tags: `aaiclick/**/test_*.py`, `aaiclick/**/examples/*.py`, `test_e2e/**/*.py`, `src/**/*.test.ts`, `src/**/*.test.tsx`, skipping any path with a `node_modules` part; a line matching `^\s*(?:#|//)\s*req:\s*(.+)$`, split on `,`, stripped, empties dropped.
- Problems: `unknown id` (tag names no marker), `untagged` (marker with no tag), `duplicate` (same ID twice), `wrong layer` (tag path outside `LAYER_TEST_ROOTS[id.split("/")[0]]`).

Fixture strings in this file put the tag text inside a quoted literal (`"# req: ..."`), so a line starts with `"` and the tag regex never matches the module itself.

- [ ] **Step 1: Write the failing tests**

```python
CASES = {
    "clean":               ({"docs/user_guide/p.md": "<!-- req: a -->\n", "aaiclick/test_x.py": "# req: user_guide/p/a\n"}, []),
    "unknown-id":          ({"aaiclick/test_x.py": "# req: user_guide/p/a\n"}, ["unknown id user_guide/p/a"]),
    "untagged":            ({"docs/user_guide/p.md": "<!-- req: a -->\n"}, ["untagged user_guide/p/a"]),
    "duplicate":           ({"docs/user_guide/p.md": "<!-- req: a -->\n<!-- req: a -->\n", "aaiclick/test_x.py": "# req: user_guide/p/a\n"}, ["duplicate user_guide/p/a"]),
    "wrong-layer":         ({"docs/user_guide/p.md": "<!-- req: a -->\n", "src/x.test.ts": "// req: user_guide/p/a\n"}, ["wrong layer user_guide/p/a"]),
    "bad-slug":            ({"docs/user_guide/p.md": "<!-- req: Bad_Slug -->\n"}, ["bad slug"]),
    "messy-tag-list":      ({"docs/designs/d.md": "<!-- req: a -->\n<!-- req: b -->\n", "aaiclick/test_x.py": "# req: designs/d/a ,  designs/d/b,\n"}, []),
    "non-layer-page":      ({"docs/superpowers/plans/x.md": "<!-- req: a -->\n"}, []),
    "node-modules":        ({"src/node_modules/m/x.test.ts": "// req: user_guide/p/a\n"}, []),
    "same-slug-two-pages": ({"docs/user_guide/p.md": "<!-- req: a -->\n", "docs/user_guide/q.md": "<!-- req: a -->\n", "aaiclick/test_x.py": "# req: user_guide/p/a, user_guide/q/a\n"}, []),
}

@pytest.mark.parametrize(("files", "expected"), CASES.values(), ids=CASES.keys())
def test_check(tmp_path, files, expected):
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(text)
    problems = check(tmp_path)
    assert len(problems) == len(expected)
    for want in expected:
        assert any(want in p for p in problems)

@pytest.mark.skipif(not (REPO_ROOT / "docs").is_dir(), reason="docs/ not shipped in the wheel")
def test_repo_markers_and_tags_agree():
    assert check(REPO_ROOT) == []
```

`REPO_ROOT = Path(__file__).resolve().parents[1]`.

- [ ] **Step 2: Run, expect FAIL** — `uv run pytest aaiclick/test_vmodel.py -v` → NameError on `check`.

- [ ] **Step 3: Implement** `Marker`, `Tag`, `LAYER_PAGES`, `LAYER_TEST_ROOTS`, `find_markers`, `find_tags`, `check` above the tests, using `Path.rglob`, `re` and `collections.Counter`. Add a module docstring: what the check enforces, and `See .claude/skills/vmodel/SKILL.md — "Tagging".`

- [ ] **Step 4: Run, expect PASS** — `uv run pytest aaiclick/test_vmodel.py aaiclick/test_ci_paths.py -v`; all 11 cases plus the guard pass (the repo has no markers yet, so the repo test passes vacuously).

- [ ] **Step 5: Lint** — `uv run ruff check aaiclick/test_vmodel.py aaiclick/test_ci_paths.py && uv run ruff format --check aaiclick/test_vmodel.py aaiclick/test_ci_paths.py`.

- [ ] **Step 6: Commit** — message `Add vmodel marker check`.

---

### Task 6: Group the User Guide nav

**Files:**
- Modify: `mkdocs.yml` (`nav` → `User Guide`)

- [ ] **Step 1: Regroup** — no file moves, same page titles:
  - `Data`: Object API, Ingest, ObjectContext
  - `Orchestration`: Orchestration, Task API, Task API (Python), Task API (Java), Task Interface, Lineage
  - `Operations`: Deployment, Container Images, Sandbox, DNS Rebinding, Example Projects

- [ ] **Step 2: Verify** — `uv run mkdocs build --strict`; every page from the old list appears exactly once (`grep -c "user_guide/" mkdocs.yml` equals 14).

- [ ] **Step 3: Commit** — message `Group User Guide nav by surface`.

---

### Task 7: Push and check CI

- [ ] **Step 1:** `git push -u origin ccr-696e1f2e-0ne5sk` (retry network errors up to 4 times with backoff).
- [ ] **Step 2:** Run the full local suite once: `uv run pytest -q -n auto`; expected: no failures.
- [ ] **Step 3:** Ask the user whether to open a PR; if one exists, run the `check-pr` skill and fix any failing workflow.
