Technical Debt
---

# chdb Session Not Safe to Close + Reopen In-Process

- **`_pin_chdb_session` fixture** (`aaiclick/conftest.py`)
  - **Issue**: chdb embeds the entire ClickHouse server (Poco `Application` singleton, logger, settings manager, a native `ThreadPool`) as per-process state. Calling `chdb.session.Session.cleanup()` and then instantiating another `Session(path)` in the same process races lingering ThreadPool workers against reinitialization. Intermittently trips a glibc assertion `___pthread_mutex_lock: Assertion 'mutex->__data.__owner == 0' failed` inside `_chdb.abi3.so`, abort()ing the process. Reproduces ~25–40 % of full test-suite runs; gdb confirms the fault is entirely in chdb ThreadPool threads, zero aaiclick frames on the crashing stack.
  - **Root cause**: chdb's documented constraint — one active session per process. `aaiclick/orchestration/orch_context.py` calls `close_session()` on every context exit (needed so a subprocess worker can acquire the chdb file lock); under pytest, ~500+ orch_context cycles amplify the teardown race.
  - **Workaround**: `_pin_chdb_session` (session-scoped, autouse) replaces `close_session` with a no-op in both `aaiclick.data.data_context.chdb_client` and the `aaiclick.orchestration.orch_context` import binding for the duration of the pytest run. The chdb `Session` becomes a true per-process singleton, matching what chdb supports. Subprocess-worker tests (which legitimately need the close) aren't affected — they own their own child process, unaffected by the parent's patched binding.
  - **Debt**: Remove the fixture once chdb ships a `Session` implementation that tolerates repeated teardown+reopen in-process. No upstream fix across chdb 4.0 → 4.1.6 (see [chdb-io/chdb#229](https://github.com/chdb-io/chdb/issues/229), [#197](https://github.com/chdb-io/chdb/issues/197)). A complementary production-side improvement — making `close_session()` opt-in (only when about to spawn a chdb-owning subprocess) instead of unconditional on every `orch_context` exit — is tracked separately in `docs/future.md`.

# chdb `url()` Table Function

- **`ChdbClient._rewrite_external_urls()`** (`aaiclick/data/data_context/chdb_client.py`)
  - **Issue**: chdb's embedded ClickHouse hangs or misinterprets external HTTP/HTTPS URLs passed to the `url()` table function. The embedded HTTP client either blocks the process with no timeout (≤4.1.2) or treats URLs as named collections (26.1.0).
  - **Workaround**: `ChdbClient.command()` and `.query()` intercept any `url('https://...', 'fmt')` in SQL via regex, download the file to a `NamedTemporaryFile` via `asyncio.to_thread(urllib.request.urlretrieve)`, and rewrite the expression to `file('/tmp/x', 'fmt')` before execution. All URLs (including localhost) are rewritten consistently. `NamedTemporaryFile` is used (not `TemporaryFile`) because chdb needs a filesystem path string.
  - **Debt**: Confirmed broken in chdb 4.1.2 and 26.1.0; no upstream fix. Remove this workaround once chdb's `url()` works reliably. Track at [chdb-io/chdb](https://github.com/chdb-io/chdb).

# clickhouse-connect Async FutureWarning

- **`filterwarnings` in `pyproject.toml`** (`[tool.pytest.ini_options]`)
  - **Issue**: `clickhouse-connect>=0.15` emits a `FutureWarning` about the async client being a thread-pool wrapper, recommending the `[async]` prerelease.
  - **Workaround**: `warnings.catch_warnings()` around `get_async_client()` calls in `clickhouse_client.py` and `background_worker.py`.
  - **Debt**: Remove the filter once `clickhouse-connect` 1.0 ships the native async client as default.

# GitHub Actions

- **`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true`** (`.github/workflows/test.yaml`)
  - **Issue**: `dorny/test-reporter@v2` targets Node.js 20, which GitHub Actions deprecates from June 2, 2026.
  - **Debt**: No v3 of the action exists yet. Remove the env var and pin to the new version once `dorny/test-reporter` releases Node.js 24 support.

# LiteLLM Unawaited Coroutine on Ollama Path

- **`pytest_collection_modifyitems()`** (`aaiclick/ai/conftest.py`)
  - **Issue**: LiteLLM 1.82.4 leaves `async_success_handler` coroutines unawaited on the Ollama code path despite the upstream fix in v1.76.1 (PR #14050). This produces `RuntimeWarning` and `PytestUnraisableExceptionWarning` that fail tests under `filterwarnings = ["error"]`.
  - **Workaround**: `pytest_collection_modifyitems()` attaches per-test `filterwarnings` marks to `live_llm`-marked tests, suppressing only those two warnings. Scoped to live LLM tests so it doesn't mask warnings elsewhere.
  - **Debt**: Remove the suppressions once LiteLLM fixes the Ollama-specific code path. Track at [BerriAI/litellm](https://github.com/BerriAI/litellm).
