Lazy ClickHouse Client and Scoped Task Info
---

Three changes from the "decouple Objects from task runs" evaluation. Bare scripts using Objects with no context are out of scope: `object_context()` (today `data_context()`, renamed below) stays the owner for standalone code.

# Goals

- `orch_context()` opens ClickHouse only when something queries it. The `with_ch` flag goes away.
- The executing task's `TaskInfo` is set for the task's lifetime and restored afterwards, never left behind by a bare `ContextVar.set()`.
- `data_context` becomes `object_context` everywhere.

# Lazy ClickHouse client

**Implementation**: `aaiclick/data/data_context/ch_client.py`

## Problem

`orch_context(with_ch=False)` exists so SQL-only callers (server, CLI, background worker, mp-worker parent) don't take the chdb file lock. Callers must pick the flag up front: 65 call sites, plus a second FastAPI dependency (`orch_scope_with_ch`) just to flip it.

## Design

`orch_context()` always sets a `LazyChClient` in `_ch_client_var`. It satisfies `ChClient` and creates the real client on the first awaited call.

```python
class LazyChClient:
    """ChClient that creates the real client on first use."""

    def __init__(self) -> None:
        self._client: ChClient | None = None
        self._lock = asyncio.Lock()

    async def _get(self) -> ChClient:
        async with self._lock:
            if self._client is None:
                self._client = await create_ch_client()
            return self._client

    async def query(self, query, settings=None, parameters=None):
        return await (await self._get()).query(query, settings, parameters)

    # command / insert / raw_query / insert_arrow / raw_stream delegate the same way

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
```

- **One object, set once at entry.** Child asyncio tasks inherit the same proxy, so the first query in any of them fills it for all. This avoids the lazy-`ContextVar.set()` trap, where a value set inside a getter is invisible to the caller and to sibling tasks.
- **`get_ch_client()` stays sync.** It returns the proxy and does no I/O, so its ~30 callers don't change.
- **`raw_stream`** is outside the `ChClient` protocol but used by `export_query_to_file` on remote ClickHouse, so the proxy delegates it too.
- **Nesting is unchanged.** If an outer context (`orch_context` or `data_context`) already set a client, the inner `orch_context` reuses it and does not close it.
- **Closing** works as today: the context that created the proxy awaits `close()` on exit. On chdb, `ChdbClient.close()` stays a no-op, because the Session is a per-process singleton closed by `atexit`.
- **Failure moves to first use.** A held chdb lock or an unreachable server now raises on the first query, not on `orch_context()` entry. The context still unwinds normally.

## Removing `with_ch`

| Site                                           | Change                                    |
|------------------------------------------------|-------------------------------------------|
| `orch_context(with_ch=...)` (65 calls)         | `orch_context()`                          |
| `server/deps.py` `orch_scope_with_ch`          | Deleted; routers use `orch_scope`         |
| `__main__.py` `_run_internal_api(with_ch=...)` | Parameter dropped                         |
| `testing.py` `orch_module_ctx_no_ch`           | Keeps the chdb tempdir, drops the flag    |

Docs that mention `with_ch` are updated too: `api_server.md`, `viewer.md`, `testing.md`, `task_log_retirement.md`, and the `server/mcp.py` docstring.

## Trade-off: the mp-worker parent

Today `with_ch=False` makes accidental ClickHouse use in the mp-worker parent fail loudly (`get_ch_client()` raises). With the lazy client, the same mistake silently takes the chdb lock and the task children fail to open chdb.

Decision: accept it. The mp-worker test modules (`orch_module_ctx_no_ch`) run real children against chdb and fail if the parent ever takes the lock. No runtime guard is added.

# Scoped task info

**Implementation**: `aaiclick/orchestration/execution/runner.py`

## Problem

`execute_task` calls `set_current_task_info(...)` (`runner.py`), a bare `ContextVar.set()` that is never reset. Nothing currently breaks: the mp child exits after one task, and the in-process worker runs each task in its own asyncio task. But the value outlives the task in whatever context ran it, which `CLAUDE.md` forbids.

## Design

- `execute_task` wraps its body in the existing `current_task_info(TaskInfo(...))` context manager, which resets on exit.
- `set_current_task_info` is deleted. Its two test callers (`test_image_injection.py`) switch to `with current_task_info(TaskInfo(...)):`, and it is dropped from `execution/__init__.py`.
- `task_scope` is unchanged. Setting `TaskInfo` there would make the synthetic `task_scope` in the `orch_ctx` fixture look like an executing task, which breaks tests asserting that no task is active (`test_finish_shell_result_restores_task_context`).

# Rename `data_context` → `object_context`

A breaking rename with no alias: the context exists to scope Object lifecycles, and the new name says so. It is mechanical (`git mv` plus a whole-word replace) and lands as its own commit so the diff stays reviewable.

| Old                                     | New                                       |
|-----------------------------------------|-------------------------------------------|
| `data_context()`                        | `object_context()`                        |
| package `aaiclick/data/data_context/`   | `aaiclick/data/object_context/`           |
| module `data_context/data_context.py`   | `object_context/object_context.py`        |
| `docs/user_guide/data_context.md`       | `docs/user_guide/object_context.md`       |
| `mkdocs.yml` nav "DataContext"          | "ObjectContext"                           |
| "DataContext" in prose (16 hits)        | "ObjectContext"                           |

About 290 whole-word hits across 121 files: imports, call sites, examples, docstrings, error messages (`get_ch_client`, `scope.py`), and the root `README.md`. Section links quoting `docs/user_guide/data_context.md — "..."` are updated with the file.

# Tests

- **`test_orch_context.py`:** replace the `with_ch` nesting tests with "nested `orch_context` reuses the outer client". `test_orch_context_unwinds_when_ch_client_creation_fails` becomes: the first query raises and the context still resets its vars and disposes the engine.
- **New:** `orch_context()` with no ClickHouse query never calls `create_ch_client`, and concurrent first queries create exactly one client.
- **New:** after `execute_task` returns, `get_current_task_info()` raises.
- **Existing mp-worker modules:** they guard the parent-lock trade-off.

# Out of scope

- Objects with no context (lazy SQL engine, default local lifecycle, `aaiclick.aclose()`).
- Per-task ownership of connections. Connections stay owned by `orch_context()` / `data_context()`.
