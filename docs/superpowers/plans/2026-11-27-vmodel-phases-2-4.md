# V-model Phases 2–4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task (CLAUDE.md requires inline execution). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the four-layer docs: split the two large user pages, move user content out of `designs/`, add Web UI and Access pages, create `docs/architecture/`, narrow `docs/designs/`, link the new and moved pages, and turn the data examples into acceptance tests.

**Architecture:** Mostly docs moves done with a section-moving script (by heading), each followed by a marker/tag pass that the checker (`aaiclick/test_vmodel.py`) verifies. One new test module checks the examples' `# →` output comments.

**Tech Stack:** mkdocs-material, pytest, Python stdlib (`ast`, `inspect`, `contextlib`).

**Spec:** `docs/superpowers/specs/2026-11-26-vmodel-docs-design.md` (phases 2–4; phase 5 is the tagging these tasks do as they land).

## Global Constraints

- Branch `ccr-696e1f2e-0ne5sk`; push there only. PR #574 stays open.
- After every task: `uv run pytest aaiclick/test_vmodel.py aaiclick/test_ci_paths.py -q` and `uv run mkdocs build --strict` pass.
- IDs are the page path under `docs/` without `.md`, then `/<slug>` (base skill rule; `index.md` keeps `index` in the path). A moved statement keeps its slug; its ID prefix changes and every tag naming it is rewritten in the same commit.
- A new or moved page gets markers in the change that adds it (base skill "Tagging grows with changes"); a statement with no proving test gets no marker.
- Docstring pointers cite `docs/<file>.md — "Section"`; every moved or renamed section updates them in the same commit (`grep -rn 'docs/<file>.md'`).
- No product-code changes, except where a task says so.
- `markdown-style`: setext title, ATX sections one level deep, aligned tables. Design pages have no user how-to; user pages have no implementation detail.
- Commit trailer: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01LcC1uAdwzdrETUDd5YMXn9`.

## Review Focus

1. A deep link into a split page (`object.md#views`, `orchestration.md#runner-modes`) from another page, the README or `index.md` must resolve after the split — `mkdocs --strict` with anchor validation catches in-site links; grep catches README and code.
2. A tag naming an old ID after a move must not survive — the checker's `unknown id` rule catches it.
3. A docstring pointer to a moved section must not dangle — grep for the old `file.md — "Heading"` pair after every move.
4. A non-deterministic example output (ids, timings, temp table names) must not make the examples test flaky — `...` in a `# →` comment is a wildcard.
5. `docs/designs/` must still describe only shipped code — unbuilt lineage replay moves to `future.md`.

---

### Task 1: Section-moving and ID-rename helpers (scratch, not committed)

**Files:**
- Create: `<scratchpad>/vtools.py` (not in the repo)

**Interfaces:**
- Produces:
  - `move_sections(src: Path, dst: Path, headings: list[str], level: int = 1) -> None` — cut each `#`-level section (heading line to the next heading of the same or higher level, outside code fences) from `src` and append to `dst` in the given order.
  - `rename_ids(repo: Path, mapping: dict[str, str]) -> int` — rewrite `# req:` / `// req:` tags in `aaiclick/**`, `src/**`, `test_e2e/**` per `mapping`; returns the count changed.
  - `ids_on(page: Path, docs: Path) -> dict[str, str]` — slug → full ID for a page.

- [ ] **Step 1:** Write the three functions; check `move_sections` on a temp copy of `object.md` (moving `# Views` keeps both files' line totals equal to the original).

### Task 2: Split `docs/user_guide/object.md`

**Files:**
- Move: `docs/user_guide/object.md` → `docs/user_guide/object/index.md`
- Create: `docs/user_guide/object/{operators,lazy_results,data_retrieval,export,blob_storage,views}.md`
- Modify: `mkdocs.yml` nav; every in-repo link to `object.md…`; tags naming `user_guide/object/<slug>`; docstrings citing `docs/user_guide/object.md`

| New page             | Sections (from current `object.md`)                    |
|----------------------|--------------------------------------------------------|
| `index.md`           | Overview, API Quick Reference, Operation Provenance    |
| `operators.md`       | Operator Support (all its `##`)                        |
| `lazy_results.md`    | Lazy Operator Results (all its `##`)                   |
| `data_retrieval.md`  | Data Retrieval                                         |
| `export.md`          | Export                                                 |
| `blob_storage.md`    | Blob Storage                                           |
| `views.md`           | Views, The copy() Method                               |

- [ ] **Step 1:** `git mv` to `object/index.md`; `move_sections` into the six pages; give each a setext title and a one-line intro.
- [ ] **Step 2:** Build the old→new ID mapping (`user_guide/object/<slug>` → `user_guide/object/<page>/<slug>`, index stays `user_guide/object/index/<slug>`), run `rename_ids`.
- [ ] **Step 3:** Fix links: Quick Reference rows point at `operators.md#…` etc.; other pages' `object.md#x` links; `README.md`, tutorial and examples pages; docstrings.
- [ ] **Step 4:** Nav: `Data → Object API` becomes a section with the seven pages.
- [ ] **Step 5: Verify** — checker passes; `mkdocs build --strict` passes; `grep -rn "user_guide/object\.md" --exclude-dir=.git --exclude-dir=superpowers .` is empty; touched tests pass.
- [ ] **Step 6: Commit** — `Split the Object API user guide into a folder of pages`.

### Task 3: Split `docs/user_guide/orchestration.md`

Same steps as Task 2.

| New page                 | Sections (from current `orchestration.md`)                  |
|--------------------------|-------------------------------------------------------------|
| `index.md`               | Quick Start, Internal Design                                |
| `tasks_and_jobs.md`      | Defining Tasks and Jobs                                     |
| `running_jobs.md`        | Running Jobs                                                |
| `runners.md`             | Runners                                                     |
| `parallel_operators.md`  | Parallel Operators                                          |
| `managing_jobs.md`       | Managing Jobs, Task Logs                                    |

- [ ] Steps 1–6 as Task 2; commit `Split the orchestration user guide into a folder of pages`.

### Task 4: Move user content out of `designs/`, deduplicate

**Files:** `docs/designs/{orchestration,oplog,java-sdk,auth,api_server,deployment,blob_storage,lineage}.md`, `docs/contributor/debugging.md`, the user pages named below.

- [ ] **Step 1: Move in** (then mark and tag what a test proves):
  - `designs/orchestration.md` "CLI" (`execution-worker`, `background`, `migrate`) → `user_guide/orchestration/managing_jobs.md` "Workers and maintenance".
  - `designs/orchestration.md` `AAICLICK_TASK_LOGS` table → `user_guide/orchestration/managing_jobs.md` "Task Logs".
  - `designs/oplog.md` "Environment Variables" → `user_guide/lineage.md`.
  - `designs/java-sdk.md` Dockerfile + `-parameters` how-to → `user_guide/task_api_java.md`.
  - `AAICLICK_DEBUG` (from `contributor/debugging.md`) → `user_guide/deployment.md` configuration; `debugging.md` keeps only the contributor how-to.
- [ ] **Step 2: Deduplicate** — keep one copy, the others become one-line pointers:
  - preservation modes: keep `orchestration/running_jobs.md`; `object_context.md` section and `designs/oplog.md`, `designs/lineage.md` copies → pointer; retarget `user_guide/object_context/preservation-modes` tags to the kept ID (merge into the existing one if it states the same promise).
  - seed admin + JWT secret: keep `user_guide/deployment.md`; `designs/auth.md`, `designs/api_server.md`, `designs/deployment.md` → pointer.
  - `local start`: keep `user_guide/deployment.md`; `designs/api_server.md` "Running the server" keeps only process roles.
  - credentials / located objects: keep `user_guide/object/blob_storage.md`; `designs/blob_storage.md` keeps mechanics only.
- [ ] **Step 3: Verify, commit** — `Move user-facing content from designs into the user guide`.

### Task 5: Web UI and Access pages

**Files:**
- Create: `docs/user_guide/ui.md` (from `designs/ui.md` Layout, Navigation, Modes; `designs/frontend.md` LiveStatus badge; `designs/viewer.md` user lines), `docs/user_guide/access.md` (from `designs/auth.md` Configuration, Roles, API Tokens, MFA, Password Reset, CLI & Admin Bootstrap, SPA prompts; MCP tool tiers)
- Modify: `mkdocs.yml` (nav group `Web UI`: UI, Access), `docs/index.md` and other user pages linking `designs/ui.md`, the source design pages (moved text becomes a pointer)

- [ ] **Step 1:** Write both pages from the sources, user voice only; add the views `designs/ui.md` omits (registered jobs, run/register forms, sandbox, login) from `src/views/`.
- [ ] **Step 2:** Mark and tag: `test_e2e/web/*`, `aaiclick/server/**` auth/role tests, `aaiclick/auth/**`, `aaiclick/test_cli.py` token tests.
- [ ] **Step 3: Verify, commit** — `Add Web UI and Access user guide pages`.

### Task 6: `docs/architecture/`

**Files:**
- Create: `docs/architecture/{topology,data_model,contracts,security,backends}.md`; `mkdocs.yml` `exclude_docs` adds `architecture/`.
- Modify: source design pages (sections move out), docstrings citing moved sections.

| Page            | Moved in from                                                                                          | Written new                         |
|-----------------|--------------------------------------------------------------------------------------------------------|-------------------------------------|
| `topology.md`   | `designs/orchestration.md` Topology, Deployment Modes, Worker runner, Execution layers; `designs/api_server.md` Architecture, process roles | —                                   |
| `data_model.md` | `designs/orchestration.md` DB Models; `designs/oplog.md` Storage, Table Lifecycle & Cleanup; `designs/object_context.md` Schema Storage | table naming and scopes; row order and `aai_id` |
| `contracts.md`  | `designs/api_server.md` View Model Catalogue, Internal API Contract, REST Surface, MCP Surface; `designs/task_interface.md` Vocabulary | —                                   |
| `security.md`   | `designs/auth.md` Scope, Data Model, Roles (ceiling), Principal Resolution & RBAC; `designs/blob_storage.md` Credentials (secrets never in SQL); image-source trust; `user_guide/dns_rebinding.md` "Why distributed needs no guard" | —                                   |
| `backends.md`   | scattered chdb-vs-server facts (export local paths, `.bz2`, `connection=`, stats fields, chdb one-session-per-process from `contributor/testing.md`) | the capability table               |

- [ ] **Step 1:** Move sections; write the "new" parts from code (`aaiclick/data/scope.py`, `aaiclick/data/object/ingest.py`, `aaiclick/data/models.py`).
- [ ] **Step 2:** Mark and tag with system tests (`aaiclick/server/**`, `aaiclick/internal_api/**`, `aaiclick/test_cli.py`, both-backend data tests); user tests that already walk a statement add the architecture ID (shared pair).
- [ ] **Step 3: Verify, commit** — `Create docs/architecture from system sections of designs`.

### Task 7: Narrow `docs/designs/`, link it

**Files:** remaining `docs/designs/*.md`, `docs/designs/future.md`, `docs/contributor/`.

- [ ] **Step 1:**
  - delete duplicates left in `designs/orchestration.md` (Basic Example, Decorators except the retry-reset line, Shell entry type, Configuration);
  - merge `task_log_retirement.md` run registration and flushing into `designs/orchestration.md` "Task Logs"; delete the page;
  - `designs/lineage.md`: `ReplayHandle`, `request_full_replay`, drift comparison → `future.md` "Lineage — Tier 2 Full Replay";
  - release/build/testing sections → `docs/contributor/` (`release.md` from `designs/deployment.md` Release pipeline + `java-sdk.md` Publishing; testing sections of `frontend.md`, `java-sdk.md`, `kubernetes_runner.md`, `blob_storage.md`, `viewer.md` → `contributor/testing.md`).
- [ ] **Step 2:** Mark and tag every remaining design page's statements with integration tests (`aaiclick/**/test_*.py`, `src/**/*.test.ts(x)`).
- [ ] **Step 3: Verify, commit** — `Narrow docs/designs to integration content and link it`.

### Task 8: Examples check their `# →` output

**Files:**
- Create: `aaiclick/data/examples/test_examples.py`
- Modify: example files whose `# →` comment is wrong or non-deterministic (comment only)

**Interfaces:**
- Produces: `expected_outputs(path: Path) -> dict[int, str]` (source line → expected text, from `# →` comments on `print(` lines); `run_and_capture(amain) -> list[tuple[int, str]]` (line of each `print` call in the example module, printed text); `check_example(module) -> list[str]` (mismatches).
- Rule: a printed line matches when it ends with the expected text; `...` in the expected text matches any run of characters.

- [ ] **Step 1: Failing test** — `test_check_example_reports_a_wrong_comment(tmp_path)`: a tiny example module printing `2` with `# → 3` yields one mismatch; with `# → 2` none; with `# → ...` none.
- [ ] **Step 2:** Implement; then one test function per example file (`test_basic_operators_example`, …, 15), each `assert check_example(mod) == []`, each tagged with the `user_guide/object/...` IDs the example demonstrates.
- [ ] **Step 3:** Run; fix wrong comments in the examples (comment text only), use `...` for ids, table names and timings.
- [ ] **Step 4: Verify, commit** — `Check the data examples' output comments`.

### Task 9: Close out

- [ ] Update `CLAUDE.md` "Testing" line to name the four layers; update `.claude/skills/vmodel/SKILL.md` if any path changed.
- [ ] Delete `docs/superpowers/plans/*vmodel*` and `docs/superpowers/specs/2026-11-26-vmodel-docs-design.md`; fix references.
- [ ] Full suite, `pre-commit run --from-ref origin/main --to-ref HEAD`, push, `check-pr`.
