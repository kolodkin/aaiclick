Testing Architecture
---

How the test suite composes fixtures, isolates backend state, and works
around chdb's single-session-per-process constraint.

# Fixtures

Shared fixtures live in `aaiclick/testing.py`; `aaiclick/conftest.py`
imports them so pytest registers them at the root (a nested
`pytest_plugins` is ignored under `--pyargs`). Subpackage conftests hold
only subpackage-local fixtures:

| Conftest                                        | Content                                  |
|-------------------------------------------------|------------------------------------------|
| `aaiclick/data/conftest.py`                     | `ctx` (function-scoped `object_context`) |
| `aaiclick/orchestration/conftest.py`            | `fast_poll`                              |
| `aaiclick/orchestration/background/conftest.py` | `bg_db` (temp SQLite engine)             |

## Shared fixtures (from `aaiclick/testing.py`)

**Session-scoped, autouse** — fire implicitly, never declared. A conftest
wraps the `worker_databases()` context manager in its own autouse session
fixture, which composes:

| Context manager       | Purpose                                                               |
|-----------------------|-----------------------------------------------------------------------|
| `worker_ch_database`  | `<root>/chdb_data` for chdb / per-xdist-worker database for real CH   |
| `worker_sql_database` | `<root>/local.db` for SQLite / per-xdist-worker database for Postgres |

**Module-scoped** — one entry per test module, explicit via `orch_ctx*`:

| Fixture                 | Enters                                                           |
|-------------------------|------------------------------------------------------------------|
| `orch_module_ctx`       | `orch_context()` (with chdb)                                     |
| `orch_module_ctx_no_ch` | `orch_context()` + per-module chdb tempdir for the child         |
| `orch_shared_ctx`       | `orch_module_ctx`, reset once — for shared module setup          |
| `orch_shared_ctx_no_ch` | `orch_module_ctx_no_ch`, SQL reset once — shared mp-worker setup |

**Function-scoped** — declare as a test parameter:

| Fixture          | Depends on               | Resets                          |
|------------------|--------------------------|---------------------------------|
| `ctx`            | fresh `object_context()` | CH tables                       |
| `orch_ctx`       | `orch_module_ctx`        | CH tables + SQL rows            |
| `orch_ctx_no_ch` | `orch_module_ctx_no_ch`  | SQL rows only (no CH in parent) |

# chdb Session Constraint

chdb embeds one ClickHouse server (Poco `Application` singleton +
native `ThreadPool`) as per-process state. `Session.cleanup()` followed
by another `Session(path)` races lingering ThreadPool workers and
intermittently trips a glibc pthread_mutex assertion inside
`_chdb.abi3.so`, `abort()`ing the process. Upstream:
[chdb-io/chdb#229](https://github.com/chdb-io/chdb/issues/229),
[#197](https://github.com/chdb-io/chdb/issues/197) — no fix as of 4.1.6.
**Exactly one Session per process lifetime is supported.**

# Workaround Layers

Two independent mitigations compose to get a stable suite:

1. **Nested `orch_context()` reuses the outer ClickHouse client.** Inner
   `orch_context` / inline `object_context()` calls reuse the process's
   cached chdb session (`aaiclick/data/object_context/chdb_client.py`)
   instead of opening one. Only the outermost `orch_context` installs a lazy
   client, which opens chdb on its first query.

2. **Module-scoped orch fixtures.** `orch_ctx` / `orch_ctx_no_ch` sit on
   top of `orch_module_ctx*`, not per-test `orch_context()`. Requires
   `asyncio_default_fixture_loop_scope = "module"` (+ test loop scope)
   in `pyproject.toml` so the async fixtures persist across the module's
   tests.

Production follows the same constraint: `orch_context` never closes the
Session mid-process. It lives for the whole process and is closed exactly
once at exit (`chdb_client.py`), because a bare process exit lets engine
threads race C++ static destructors — an intermittent shutdown SIGSEGV
after a green run.

# mp-worker Module Split

Multiprocessing-worker tests (`test_mp_worker.py`, `test_retry_mp.py`,
`test_worker_mp.py`) need the **parent** process to hold no chdb file
lock. They use `orch_ctx_no_ch` in dedicated modules, and
`orch_module_ctx_no_ch` swaps `AAICLICK_CH_URL` to a per-module tempdir
so the spawned child opens a fresh chdb file. A throwaway subprocess
migrates that tempdir first — with chdb + Postgres the child's
`init_oplog_tables` only checks for pending migrations, never applies them.

!!! tip "Don't mix `orch_ctx` and `orch_ctx_no_ch` in one module"
    The two module-scoped fixtures require conflicting chdb setups.
    Put mp-worker tests in a separate `_mp.py` file.

!!! tip "Don't mix `orch_ctx` and `orch_shared_ctx` in one module"
    `orch_ctx` resets before every test and wipes the shared state.
    Put shared-setup tests in their own module (e.g.
    `test_execution_shared_runs.py`).

# Per-process Isolation

`worker_databases()` pins `AAICLICK_LOCAL_ROOT` to a temp directory for
each pytest process, with or without xdist. File-backed backends (SQLite,
chdb) live under it in `local start`'s layout, so a plain `pytest` never
opens `~/.aaiclick`, which a running `local start` holds the chdb lock on;
server-backed backends get a per-xdist-worker database. The per-test reset
(`drop_all_ch_tables` + `reset_sql_tables`) scopes to `currentDatabase()`
and its `_internal` sibling, so it's safe even on shared distributed
servers.

`drop_all_ch_tables` truncates the migration-created tables while their
DDL and applied versions match the snapshot taken after the last full
rebuild. Any drift (e.g. `oplog/test_migrate.py` dropping them) triggers a
full drop + `ch_upgrade` — needed in distributed mode, where
`init_oplog_tables` never creates them.

# Foreign Keys on SQLite

SQLite leaves `PRAGMA foreign_keys` off per connection, so without help the
local backend never checks the FKs Postgres enforces. `create_sql_engine()`
in `aaiclick/orchestration/sql_context.py` turns the pragma on via a
`connect` listener; ruff bans direct `create_async_engine` calls so every
engine gets it. `reset_sql_tables` therefore deletes children before parents.

# Isolation and Seeding

Tests in one pytest process share that process's databases, so a test sees
whatever the previous one left unless it starts from a known state. Two rules
follow; the `python-testing-style` skill holds the details and examples.

- **Isolated**: every test resets or declares a resetting fixture. A test
  that starts a worker or the server lifespan must: a worker claims and runs
  any pending task it finds.
- **Seed first**: build the state once in a module fixture on
  `orch_shared_ctx`, several task runs started together, then only assert.

# Adding New Tests

1. Put the test next to the module it tests (`test_<module>.py`).
2. Declare fixtures by parameter name (`orch_ctx`, `ctx`, `fast_poll`).
   Autouse worker-isolation fixtures fire implicitly.
3. mp-worker tests go in a dedicated `_mp.py` module.
   Seed-first modules use `orch_shared_ctx` and never mix it with `orch_ctx`.
4. Helpers reusable across subpackages → `aaiclick/testing.py`.
   Otherwise → a local helper module next to the tests.
5. Flows shared by the runner e2e suites → `test_e2e/runner_flow.py`;
   the job waiter → `test_e2e/job_wait.py`.
6. Test modules under `test_e2e/` need unique basenames: with no
   `__init__.py`, the default `prepend` import mode can't collect two of the
   same name. `importlib` mode would, but breaks the bare `from job_wait import …`.

# Deployment tests

- `test_e2e/compose/test_env_init.py` runs `env.init.sh` as a user would
  (PR CI, "Misc local" group).
- `deploy/helm/check.sh` lints the chart and asserts on renders: generated
  Secret, `existingSecret`, external URLs, image tags, the no-database
  failure, a stable checksum. The PR `chart` job runs it; helm templates are
  not YAML before rendering, so they get no Python tests.
- `test_e2e/compose/` drives the reference stack in CI: server health, an
  execution worker registered, and a job round trip via `docker compose exec`
  into the server.

# Frontend tests

**Implementation**: `test_e2e/web/`

| Layer                | Tool                | Where                                                                     |
|----------------------|---------------------|---------------------------------------------------------------------------|
| Static type check    | `tsc --noEmit`      | `npm run check` — CI gate for every frontend task                         |
| Formatting           | Prettier            | `npm run format:check` — CI gate; `npm run format` rewrites               |
| Unit tests           | vitest              | `npm test` — kernel tests, `src/prompt.test.ts`, `src/lib/viewer.test.ts` |
| End-to-end (browser) | Playwright (Python) | `test_e2e/web/`, pytest                                                   |

**One test body, both modes.** Tests create jobs in-process through the
orchestration API (the same pattern as `seed.py`), never through REST: no
credentials are needed under distributed-mode auth, and whichever worker the
mode runs executes the job. The live-update tests therefore run against both
transports — `LocalTransport` locally, Postgres `LISTEN`/`NOTIFY` in the `UI e2e
dist` job — and the distributed run is the only place the full chain
`pg_notify` → SSE frame → browser DOM is exercised end to end.

`shot("name")` saves a numbered full-page PNG to `test-results/shots/` (reset
per run, gitignored).

**Why Playwright Python**: an e2e test exercises the browser, FastAPI,
orchestrator and DB together. `test_e2e/web/` shares the pytest harness with
`test_e2e/docker/` rather than running a parallel Node runner.

!!! warning "E2E suites don't run in default `pytest`"
    `test_e2e/<suite>/` is excluded from the default `pytest` testpaths and
    only runs when the path is passed explicitly or in a dedicated CI workflow.
    The `UI e2e dist` job in `.github/workflows/_test-reusable.yaml` runs
    `test_e2e/web/` on every PR against the distributed (Postgres + ClickHouse)
    backend.

# Java SDK tests

The shim's only I/O is two files, so the suite is infra-free: each test gets
a temp dir, writes `in.json`, runs the shim and reads `out.json`.

The docker-runner e2e covers what the unit suite cannot: a `jvm` task between
two Python tasks, each in its own container — the real `docker create` /
`docker cp` handoff of `in.json` into a JVM image, the host resolving an
upstream Python result into it, `out.json` read back, and a Python consumer
of the jvm return value. The fixture image (`test_e2e/fixtures/jvm_task/`)
builds the SDK from the checkout, so the shim under test is the commit's.

# Kubernetes end-to-end test

`test_e2e/kubernetes/` mirrors `test_e2e/docker/`: the same `sample_job` fixture
and git-daemon publishing, driven through the `register-job` → `run-job` CLI,
with the reconciler running in-process. The reusable workflow
`_kubernetes-e2e-reusable.yaml` follows `_docker-e2e-reusable.yaml` with a
**k3s** cluster (the production distribution, so NetworkPolicy is enforced)
plus the same Postgres / ClickHouse services. A `kubernetes_e2e` marker gates the suite;
collection skips it unless `kubectl cluster-info` succeeds.

Three tests cover the concurrent worker: a fan-out whose peak number of live
Pods, sampled while it runs, is above one; a worker stopped mid-run whose module
and shell Pods the next worker adopts (one attempt each, shell log intact across
the restart); and a job no node can fit that fails on the not-started deadline
while another job completes beside it.

!!! note "k3s networking recipe"
    - **Services**: Postgres, ClickHouse, pypiserver and the registry run as
      pods from `test_e2e/kubernetes/chart`. k3s runs on the runner itself, so
      each NodePort answers on `localhost`; `--service-node-port-range` is
      widened so every nodePort equals the standard port.
    - **Registry**: the image tag `localhost:5000/aaiclick-job:<sha>[-<dockerfile>]`
      is pushed by a host-network buildkitd over loopback and pulled by the
      node's containerd over plain HTTP via a `/etc/rancher/k3s/registries.yaml`
      mirror.
    - **Pod → DBs**: one DSN serves pods (Service name via CoreDNS) and the
      runner (`/etc/hosts` alias → `127.0.0.1` → NodePort).
    - **Build → test pypi**: `host.docker.internal` + `--add-host=…:host-gateway`,
      same as the docker e2e.
    - **Local dev**: k3s is Linux-only; on macOS run the same cluster with
      [k3d](https://k3d.io).

# Blob storage tests

`aaiclick/data/object/test_blob.py` runs against an in-process moto server on
both backends; a remote ClickHouse reaches it through
`AAICLICK_TEST_FILESERVER_HOST`. On a remote server the module probes
`system.settings` and skips unless the profile opts in, so it runs in the
dedicated `Blob storage dist` CI job — which starts ClickHouse with the
`users.d` file mounted and fails on a skip — and skips in the jobs that use
stock service containers. `claudeai/run.sh` uses the same `users.d` file and
starts a real MinIO for manual checks.

Its `store` fixture runs the shared export / open tests against S3 and Azure.
Azure uses Azurite, Microsoft's Azure Storage emulator: started with `npx`
locally (chdb, connection string in the env), or the container the
`Blob storage dist` job runs, whose ClickHouse mounts `docker/clickhouse/test/`
to define and grant the `azurite_test` collection.

# Viewer tests

- chdb: `query_object` over persistent and job objects — fields, `where`,
  ordering, paging, CSV, the `where` guard, unknown objects and jobs;
  dashboard round-trips; `run_dashboard` / `run_queries` column orientation.
- Routers: routes, 404 / 422 problems, 401 without a token in distributed
  mode.
- Vitest (`npm test`): the kernel's own tests, prompt parsing, the adapter.
- Playwright (seeded before the server starts): `@data` lists, orders and
  pages the seeded object, `@dashboard` renders the saved dashboard and
  re-runs it through `setParams`.
