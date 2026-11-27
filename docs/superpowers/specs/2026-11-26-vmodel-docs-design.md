V-model docs: four layers, markers and check
---

# Intent

Docs follow the four V-model layers of `devpowers:vmodel` (1.20.1), and each statement links to the test that proves it, while the published site (mkdocs on Read the Docs) stays a user site.

Today:

- the project `vmodel` skill defines three layers and puts the user guide at "System";
- `docs/designs/` mixes architecture, design, user how-to, process and unbuilt work;
- no `req` marker exists in `docs/` or in any test, and nothing checks one;
- the web UI and login have no published page.

# Goals

- `docs/` follows the base skill's recommended layout; each page holds one layer.
- The project `vmodel` skill lists four layers and extends the base skill without contradicting it.
- Existing defects found by the review are fixed first.
- Example scripts check their `# →` output, so the Object API has acceptance tests.
- A check in the test suite keeps markers and tags in step.

Non-goals:

- tagging every page and test up front: tagging grows with changes, per the base skill;
- publishing `architecture/` or `designs/`;
- tagging the Tutorial or Examples pages.

Anti-goals:

- a published page URL that stops resolving;
- markers visible to readers;
- a marker with no test behind it;
- a statement duplicated across layers.

# Constraints

- `mkdocs.yml` publishes `user_guide/`, Tutorial and Examples only; `designs/`, `drafts/` and the new `architecture/` and `contributor/` stay excluded.
- CI runs `uv run mkdocs build --strict`; broken links and anchors fail it.
- About 110 code files cite design pages by quoted section title (`markdown-style`); every move updates them in the same PR.
- No new docs dependency.

# Approaches

| Approach                         | Trade-off                                                                    |
|----------------------------------|------------------------------------------------------------------------------|
| Base layout, phased, tag on touch | Picked. Matches `devpowers:vmodel`; each phase is one or a few PRs           |
| Keep three layers                | No folder moves, but architecture stays buried in `designs/`, against the base skill |
| Full backfill of markers         | One huge diff; against "tagging grows with changes"                          |

# Layers

| Layer          | Doc of record                       | Test layer  | Proved in                                                                                           |
|----------------|-------------------------------------|-------------|-----------------------------------------------------------------------------------------------------|
| User           | `docs/user_guide/` (published)      | Acceptance  | `aaiclick/**/examples/` (checked output), `test_e2e/`, tests through `from aaiclick import ...` only |
| Architecture   | `docs/architecture/`                | System      | `aaiclick/server/**` (HTTP, MCP), `aaiclick/test_cli.py`, `aaiclick/internal_api/**`; runs on both backends in `test-local` and `test-dist` |
| Design         | `docs/designs/`                     | Integration | `aaiclick/**/test_*.py`, `src/**/*.test.ts(x)`                                                      |
| Implementation | module docstring / leading comment  | Unit        | the test file beside the module, no ID                                                              |

Outside the V: `docs/contributor/` (testing, debugging, technical debt, release), `docs/drafts/`, `docs/superpowers/`.

The pair User and Architecture shares tests: a public-API test that already walks an architecture statement (e.g. a result on both backends) names both IDs. No other pair shares.

# Page map

## architecture/ (new)

| Page            | Content                                                                 | Taken from                                                                       |
|-----------------|-------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| `topology.md`   | Process roles, deployment modes, worker runner, execution layers        | `designs/orchestration.md` (Topology, Deployment Modes, Worker runner, Execution layers), `designs/api_server.md` (Architecture, Running the server: roles) |
| `data_model.md` | SQL models, internal CH databases, `operation_log`, `table_registry`, table naming and scopes, row order and `aai_id`, schema storage | `designs/orchestration.md` (DB Models), `designs/oplog.md` (Storage, Cleanup), `designs/object_context.md` (Schema Storage); naming and row order written new |
| `contracts.md`  | REST surface and versioning, internal API verb table, MCP surface, task interface wire contract, view models | `designs/api_server.md`, `designs/task_interface.md` (Vocabulary)                |
| `security.md`   | Auth scope, roles, token ladder, session revocation, secrets never in SQL or logs, image trust, sandbox, DNS guard | `designs/auth.md` (Scope, Data Model, Roles), `designs/blob_storage.md` (Credentials), `designs/orchestration.md` (Image source trust), `user_guide/dns_rebinding.md` (Why distributed) |
| `backends.md`   | What chdb + SQLite and ClickHouse + Postgres each support               | Scattered over about five pages; written new                                     |

## designs/ (kept, narrowed)

Each page keeps only how components work together. `orchestration.md` keeps claiming, liveness, job management, registered jobs, cron, operators and the distributed object lifecycle (~255 lines). It also absorbs `task_log_retirement.md`, after the stdout/stderr capture contradiction is settled against the code. `kubernetes_runner.md`, `oplog.md` (Recording), `blob_storage.md` (Export), `viewer.md`, `frontend.md` (data layer, live updates) and the internals of `auth.md` stay. `java-sdk.md` stays without its Testing and Publishing sections.

Unbuilt behavior (`ReplayHandle`, `request_full_replay`, drift comparison in `lineage.md`) moves to `future.md`.

## contributor/ (new)

`testing.md`, `debugging.md`, `technical_debt.md`, and the release, build and testing sections cut from `designs/deployment.md`, `frontend.md`, `java-sdk.md`, `kubernetes_runner.md` and `blob_storage.md`.

## user_guide/

- **Split** `object.md` and `orchestration.md` into folders whose `index.md` keeps the old URL.
- **Add** `ui.md` (~230 lines from `designs/ui.md` and `frontend.md` badges) and `access.md` (~210 lines from `designs/auth.md`). Also describe the views `ui.md` omits today: registered jobs, run and register forms, sandbox, login.
- **Move in** content that is user-level but sits only in `designs/`:
  - the `execution-worker`, `background` and `migrate` CLI commands;
  - the `AAICLICK_TASK_LOGS` and oplog env vars;
  - the Java image contract;
  - `AAICLICK_DEBUG`.
- **Deduplicate** content repeated across pages:
  - preservation modes (three copies; keep one, in orchestration);
  - credentials and located objects (user guide against `blob_storage.md`);
  - seed admin and JWT secret (four copies);
  - `local start` (three copies).
- **Nav** groups by surface: Data, Orchestration, Operations, Web UI.

`index.md` and the user pages link to `user_guide/ui.md` instead of the repo-only `designs/ui.md`.

# Markers and tags

Per the base skill:

- An ID is the page path plus a kebab-case slug: `user_guide/object/views/chained-where`. One ID per behavior a reader would call a separate promise.
- The marker sits on the line above the statement: `<!-- req: chained-where -->`.
- A test names full IDs in a comment directly above the test function: `# req: ...` (Python), `// req: ...` (TypeScript). Per-test tags fit files that mix levels (20–30% today) without splitting them.
- Design decisions carry no ID; they go in a `## Why` section on the page they explain, or in that layer's `decisions.md`.

# Check

`aaiclick/test_vmodel.py` scans `docs/**/*.md` for markers and `aaiclick/`, `src/`, `test_e2e/` for tags. It fails when:

- a tag names an ID with no marker;
- a marker has no tag;
- a page defines the same slug twice;
- a tag sits outside its layer's "Proved in" location.

A page with no markers is ignored. The module is added to the "Misc local" path list in `_test-reusable.yaml` and skips when `docs/` is absent (tests ship in the wheel). Each rule has one `tmp_path` test.

# Acceptance from examples

`aaiclick/data/examples/*.py` already carry `# →` comments on result prints. A test runs each example and compares printed results with the comments. Lines whose output is not deterministic are marked to skip. This turns 16 examples that assert nothing into acceptance tests for the Object API.

# Phases

| Phase | Content                                                                                                     |
|-------|-------------------------------------------------------------------------------------------------------------|
| 0     | Fix defects (below)                                                                                         |
| 1     | Rewrite the project `vmodel` skill to the table above; add the check; regroup nav; create `contributor/`    |
| 2     | Create `architecture/`: move sections out of `designs/`, write `backends.md` and the naming and row-order contracts |
| 3     | User guide: split, add `ui.md` and `access.md`, move user content in, deduplicate                           |
| 4     | Checked `# →` output for the data examples                                                                  |
| 5     | Tag as changes land; optionally tag the statements proved by the example tests from phase 4                 |

Phases 2 and 3 are several PRs each, one page or split per PR.

## Phase 0 defects

- About 60 tests run in no CI workflow: `aaiclick/sandbox/`, `aaiclick/viewer/` and eight root files (`test_async_wait`, `test_backend`, `test_cli_renderers`, `test_cli_viewer`, `test_cli_wait`, `test_datetime_utils`, `test_ghcr`, `test_locks`) are missing from the path lists in `_test-reusable.yaml`.
- `user_guide/object.md` links `object_context.md#schema-storage`, which does not exist.
- `aaiclick/data/object/object.py` points Row Order at `object.md`; it lives in `ingest.md`.
- Task log capture: `task_log_retirement.md` and `designs/orchestration.md` disagree; settle against the code.
- `designs/deployment.md` lists chart RBAC without `pods/log`, `pods/exec` and `secrets`, which `container_images.md` requires.
- The `designs/auth.md` Roles table omits the sandbox.
- `user_guide/orchestration.md` says mutating endpoints need "an admin JWT"; it is admin scope, from a session or a token.
- `user_guide/ingest.md` "Distributed considerations" is future work; move to `future.md`.

# Left to the builder

- Slugs, and the exact section cut for each split and move.
- Order within phases 2, 3 and 5.
- How an example marks a non-deterministic output line.
