Testing Architecture
---

How the test suite composes fixtures, isolates backend state, and works
around chdb's single-session-per-process constraint.

---

# Fixtures

All shared fixtures live in `aaiclick/testing.py` and register globally
via `pytest_plugins = ["aaiclick.testing"]` in `aaiclick/conftest.py`.
Subpackage conftests only hold subpackage-local fixtures.

| Conftest                                         | Content                                          |
|--------------------------------------------------|--------------------------------------------------|
| `aaiclick/data/conftest.py`                      | `ctx` (function-scoped `data_context`)           |
| `aaiclick/orchestration/conftest.py`             | `_tmp_log_dir` autouse, `fast_poll`              |
| `aaiclick/oplog/conftest.py`                     | empty                                            |
| `aaiclick/ai/conftest.py`                        | `live_llm` skip + warning-filter marker logic    |
| `aaiclick/orchestration/background/conftest.py`  | `bg_db` (background-worker SQLite engine)        |

## Shared fixtures (from `aaiclick/testing.py`)

**Session-scoped, autouse** — fire implicitly, never declared:

| Fixture              | Purpose                                                                 |
|----------------------|-------------------------------------------------------------------------|
| `ch_worker_setup`    | Per-xdist-worker chdb tempdir / CH database                             |
| `sql_worker_setup`   | Per-xdist-worker SQLite file / Postgres database                        |
| `pin_chdb_session`   | No-ops `close_session` for the pytest run (see [chdb Session](#chdb-session-constraint)) |

**Module-scoped** — one entry per test module, explicit via `orch_ctx*`:

| Fixture                | Enters                                                                |
|------------------------|-----------------------------------------------------------------------|
| `orch_module_ctx`      | `orch_context()` (with chdb)                                          |
| `orch_module_ctx_no_ch`| `orch_context(with_ch=False)` + per-module chdb tempdir for the child |

**Function-scoped** — declare as a test parameter:

| Fixture           | Depends on               | Resets                               |
|-------------------|--------------------------|--------------------------------------|
| `ctx`             | fresh `data_context()`   | CH tables                            |
| `orch_ctx`        | `orch_module_ctx`        | CH tables + SQL rows                 |
| `orch_ctx_no_ch`  | `orch_module_ctx_no_ch`  | SQL rows only (no CH in parent)      |

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

Three independent mitigations compose to get a stable suite:

1. **Nested `orch_context()` reuses the outer `ch_client`.** Inner
   `orch_context` / inline `data_context()` calls don't create their own
   chdb session; only the outermost owns teardown. See the
   `owns_ch_client` branch in `aaiclick/orchestration/orch_context.py`.

2. **Module-scoped orch fixtures.** `orch_ctx` / `orch_ctx_no_ch` sit on
   top of `orch_module_ctx*`, not per-test `orch_context()`. Teardown
   drops from ~500 cycles/run to ~20. Requires
   `asyncio_default_fixture_loop_scope = "module"` (+ test loop scope)
   in `pyproject.toml` so the async fixtures persist across the module's
   tests.

3. **`pin_chdb_session`** no-ops `close_session` for the pytest run.
   Module boundaries never call the real close; the Session lives once
   per process.

A production-side alternative — making `close_session()` opt-in instead
of unconditional on every `orch_context` exit — is tracked in
`docs/future.md`. Landing it lets us delete `pin_chdb_session`.

# mp-Worker Module Split

Multiprocessing-worker tests (`test_mp_worker.py`, `test_retry_mp.py`,
`test_worker_mp.py`) need the **parent** process to hold no chdb file
lock. They use `orch_ctx_no_ch` in dedicated modules, and
`orch_module_ctx_no_ch` swaps `AAICLICK_CH_URL` to a per-module tempdir
so the spawned child opens a fresh chdb file.

!!! tip "Don't mix `orch_ctx` and `orch_ctx_no_ch` in one module"
    The two module-scoped fixtures require conflicting chdb setups.
    Put mp-worker tests in a separate `_mp.py` file.

# Per-xdist-Worker Isolation

`ch_worker_setup` / `sql_worker_setup` give each xdist worker its own
CH path + SQL database. The per-test reset
(`drop_all_ch_tables` + `reset_sql_tables`) scopes to
`database = currentDatabase()`, so it's safe even on shared distributed
servers.

# Adding New Tests

1. Put the test next to the module it tests
   (`aaiclick/data/test_context.py` tests
   `aaiclick/data/data_context.py`).
2. Declare fixtures by parameter name (`orch_ctx`, `ctx`, `fast_poll`).
   Autouse worker-isolation fixtures fire implicitly.
3. mp-worker tests go in a dedicated `_mp.py` module.
4. Helpers reusable across subpackages → `aaiclick/testing.py`.
   Otherwise → a local helper module next to the tests.
