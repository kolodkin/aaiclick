# Abstract Lifecycle Handler Plan

## Problem

Currently, `DataContext` handles object lifecycle (table creation tracking, reference counting, cleanup) directly via `TableWorker` — a local background thread that drops ClickHouse tables when refcount reaches 0. This works for single-process local runs, but in distributed orchestration (where tasks run across multiple workers), table lifecycle must be coordinated through PostgreSQL so that:

- Tables are not dropped while other workers still reference them
- Table cleanup happens when all distributed references are released
- The orchestration backend tracks which tables belong to which jobs/tasks

## Design

### Abstract Base Class: `LifecycleHandler`

Extract the lifecycle interface from `DataContext` into an abstract base class with two concrete implementations:

```python
class LifecycleHandler(ABC):
    """Abstract interface for Object table lifecycle management."""

    @abstractmethod
    async def start(self) -> None:
        """Initialize the handler."""

    @abstractmethod
    async def stop(self) -> None:
        """Shutdown and cleanup."""

    @abstractmethod
    def incref(self, table_name: str) -> None:
        """Increment reference count for a table."""

    @abstractmethod
    def decref(self, table_name: str) -> None:
        """Decrement reference count for a table."""
```

### Implementation 1: `LocalLifecycleHandler` (current behavior)

Wraps the existing `TableWorker` logic — background thread with queue-based refcounting and automatic `DROP TABLE` when refcount reaches 0. This is the default when no lifecycle handler is injected.

**Owned by**: `DataContext` (created and destroyed per context)

```python
class LocalLifecycleHandler(LifecycleHandler):
    """Local lifecycle via background TableWorker thread."""

    def __init__(self, creds: ClickHouseCreds):
        self._worker = TableWorker(creds)

    async def start(self) -> None:
        self._worker.start()

    async def stop(self) -> None:
        self._worker.stop()

    def incref(self, table_name: str) -> None:
        self._worker.incref(table_name)

    def decref(self, table_name: str) -> None:
        self._worker.decref(table_name)
```

### Implementation 2: `PgLifecycleHandler` (distributed)

**Owned by**: `OrchContext` (global, shared across all DataContexts in the worker process)

Runs as an `asyncio.Task` in OrchContext's event loop. Uses OrchContext's PG session directly — no separate thread, no separate engine. This is the first "background worker" in OrchContext, establishing a pattern for future cleanup tasks.

Why an asyncio task (not a separate thread):
- OrchContext already has a running event loop
- The handler can use `get_orch_context_session()` directly (same loop)
- No cross-loop engine issues, no thread management overhead

How sync `incref`/`decref` bridge to async:
- `queue.Queue` (thread-safe) receives sync calls from `Object.__del__` / `Object._register`
- `asyncio.Task` uses `loop.run_in_executor(None, queue.get)` to drain without blocking the event loop

Tables are NOT dropped — refcounts are tracked in PostgreSQL. Cleanup is deferred to a coordinated job-level process.

```python
class PgLifecycleHandler(LifecycleHandler):
    """Distributed lifecycle via PostgreSQL reference tracking.

    Runs as an asyncio.Task in OrchContext's event loop.
    Uses thread-safe queue.Queue for sync incref/decref calls.
    """

    def __init__(self):
        self._queue: queue.Queue[PgLifecycleMessage] = queue.Queue()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._process_loop())

    async def stop(self) -> None:
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.SHUTDOWN, ""))
        await self._task  # wait for drain + cleanup

    def incref(self, table_name: str) -> None:
        # Thread-safe, non-blocking
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.INCREF, table_name))

    def decref(self, table_name: str) -> None:
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.DECREF, table_name))

    async def _process_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            # Bridge sync queue to async — blocks in executor, not event loop
            msg = await loop.run_in_executor(None, self._queue.get)
            if msg.op == PgLifecycleOp.SHUTDOWN:
                break
            # Uses OrchContext's engine directly (same event loop)
            async with get_orch_context_session() as session:
                if msg.op == PgLifecycleOp.INCREF:
                    # UPSERT refcount row
                    ...
                elif msg.op == PgLifecycleOp.DECREF:
                    # Decrement refcount (do NOT drop table)
                    ...
```

### Ownership Model

```
OrchContext (long-lived, per worker process)
├── AsyncEngine (PostgreSQL)
├── PgLifecycleHandler (asyncio.Task)     ← owns, starts, stops
│   └── queue.Queue (thread-safe for sync incref/decref)
└── [future background workers: heartbeat, metrics, ...]

DataContext (short-lived, per task)
├── ClickHouse AsyncClient
└── self._lifecycle → injected reference to either:
    ├── orch_ctx.lifecycle (PgLifecycleHandler)  ← shared, NOT owned
    └── LocalLifecycleHandler                    ← owned, created locally
```

### OrchContext Changes

OrchContext creates and owns `PgLifecycleHandler`, exposing it via a `lifecycle` property:

```python
class OrchContext:
    def __init__(self):
        self._token = None
        self._engine: AsyncEngine | None = None
        self._lifecycle: PgLifecycleHandler | None = None

    @property
    def lifecycle(self) -> PgLifecycleHandler:
        return self._lifecycle

    async def __aenter__(self):
        # ... existing engine setup ...
        self._lifecycle = PgLifecycleHandler()
        await self._lifecycle.start()
        # ... set ContextVar ...
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Stop background workers first (flush pending writes)
        if self._lifecycle:
            await self._lifecycle.stop()
        # ... existing engine dispose, ContextVar reset ...
```

### DataContext Changes — Explicit Injection

DataContext accepts an optional `lifecycle` parameter. When provided, it uses the injected handler (shared, not owned). When `None`, it creates its own `LocalLifecycleHandler` (owned).

```python
class DataContext:
    def __init__(
        self,
        creds: ClickHouseCreds | None = None,
        engine: EngineType | None = None,
        lifecycle: LifecycleHandler | None = None,  # NEW: injected from OrchContext
    ):
        self._creds = creds or get_ch_creds()
        self._injected_lifecycle = lifecycle
        self._lifecycle: LifecycleHandler | None = None
        self._owns_lifecycle: bool = False
        # ... existing init ...

    async def __aenter__(self):
        # ... existing client init ...

        if self._injected_lifecycle is not None:
            # Use injected handler (from OrchContext) — shared, not owned
            self._lifecycle = self._injected_lifecycle
            self._owns_lifecycle = False
        else:
            # Create local handler — owned by this DataContext
            self._lifecycle = LocalLifecycleHandler(self._creds)
            await self._lifecycle.start()
            self._owns_lifecycle = True

        # ... set ContextVar ...
        return self

    async def __aexit__(self, ...):
        # ... mark objects stale ...

        # Only stop lifecycle if we own it (local mode)
        if self._owns_lifecycle and self._lifecycle:
            await self._lifecycle.stop()
        self._lifecycle = None

        # ... reset ContextVar ...
```

### Integration with Orchestration

In `execute_task()`, pass `orch_ctx.lifecycle` into `DataContext`:

```python
async def execute_task(task: Task) -> Any:
    func = import_callback(task.entrypoint)
    kwargs = deserialize_task_params(task.kwargs)

    orch_ctx = get_orch_context()

    with capture_task_output(task.id):
        # Inject PG lifecycle handler from OrchContext
        async with DataContext(lifecycle=orch_ctx.lifecycle):
            if asyncio.iscoroutinefunction(func):
                result = await func(**kwargs)
            else:
                result = func(**kwargs)

    return result
```

## Files to Change

| File | Change |
|------|--------|
| `aaiclick/data/lifecycle.py` | **NEW** — `LifecycleHandler` ABC, `LocalLifecycleHandler` |
| `aaiclick/data/data_context.py` | Add `lifecycle` param, use `self._lifecycle` instead of `self._worker` directly |
| `aaiclick/data/__init__.py` | Export `LifecycleHandler`, `LocalLifecycleHandler` |
| `aaiclick/orchestration/pg_lifecycle.py` | **NEW** — `PgLifecycleHandler` (lives in orchestration, not data) |
| `aaiclick/orchestration/context.py` | Create and manage `PgLifecycleHandler` as background worker |
| `aaiclick/orchestration/execution.py` | Pass `orch_ctx.lifecycle` into `DataContext(lifecycle=...)` |
| `aaiclick/data/test_lifecycle.py` | **NEW** — Tests for `LocalLifecycleHandler` |

## Implementation Phases

### Phase 1: Extract Abstract Class & Local Implementation
1. Create `aaiclick/data/lifecycle.py` with `LifecycleHandler` ABC and `LocalLifecycleHandler`
2. Refactor `DataContext` to use `self._lifecycle: LifecycleHandler` instead of `self._worker: TableWorker`
3. Add `lifecycle` parameter to `DataContext.__init__` (default `None`)
4. When `lifecycle is None`, create and own `LocalLifecycleHandler` (preserves all current behavior)
5. When `lifecycle` is provided, use it as shared reference (`_owns_lifecycle = False`)
6. Update `DataContext.incref()`, `DataContext.decref()`, `__aenter__`, `__aexit__` to delegate to `self._lifecycle`

### Phase 2: PgLifecycleHandler as OrchContext Background Worker
1. Create `aaiclick/orchestration/pg_lifecycle.py` with `PgLifecycleHandler`
2. `PgLifecycleHandler` runs as `asyncio.Task` in OrchContext's event loop
3. Uses `queue.Queue` + `run_in_executor` to bridge sync callers to async processing
4. Uses `get_orch_context_session()` for PG writes (same event loop, shared engine)
5. Design the PostgreSQL table for tracking CH table refcounts
6. Update `OrchContext.__aenter__` to create and start `PgLifecycleHandler`
7. Update `OrchContext.__aexit__` to stop it (flush pending writes)
8. Expose via `OrchContext.lifecycle` property

### Phase 3: Orchestration Integration
1. Update `execute_task()` to pass `orch_ctx.lifecycle` into `DataContext(lifecycle=...)`
2. Add job-level cleanup: when a job completes, drop all CH tables with refcount 0

## Design Decisions

- **Why inject `lifecycle` handler, not `orch_engine` or auto-detect?** Explicit dependency injection. DataContext doesn't need to know about OrchContext at all. The caller decides which lifecycle strategy to use. No hidden ContextVar lookups.
- **Why OrchContext-owned, not DataContext-owned?** PgLifecycleHandler tracks tables across multiple task executions. DataContext is per-task (short-lived), but table references span tasks within a job. OrchContext lives for the entire worker process — correct ownership boundary.
- **Why asyncio.Task, not a separate thread?** OrchContext already has an event loop. The handler can use `get_orch_context_session()` directly. No cross-loop engine issues, no thread management overhead.
- **Why `queue.Queue` (not `asyncio.Queue`)?** `incref`/`decref` are called from sync code (`Object.__del__`). `queue.Queue.put()` is thread-safe and non-blocking. The asyncio task uses `run_in_executor` to bridge the blocking `get()`.
- **Why `PgLifecycleHandler` lives in `orchestration/`, not `data/`?** It depends on OrchContext sessions and PG models. `LocalLifecycleHandler` lives in `data/` because it only depends on ClickHouse. Each handler lives with its dependencies.
- **Why `_owns_lifecycle` flag?** DataContext must know whether to call `stop()` on exit. Shared handlers (from OrchContext) are stopped by OrchContext. Local handlers are stopped by the DataContext that created them.
- **PG handler does NOT drop ClickHouse tables** — it only tracks refcounts in PostgreSQL. Table cleanup is a separate coordinated process at job completion, ensuring no worker drops a table another worker still needs.
- **Background worker pattern** — PgLifecycleHandler is the first OrchContext background worker. Future workers (heartbeat, metrics) follow the same protocol: `start()` creates `asyncio.Task`, `stop()` signals shutdown and awaits completion.
