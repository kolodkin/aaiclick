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
        """Initialize the handler (called on DataContext.__aenter__)."""

    @abstractmethod
    async def stop(self) -> None:
        """Shutdown and cleanup all remaining tables (called on DataContext.__aexit__)."""

    @abstractmethod
    def incref(self, table_name: str) -> None:
        """Increment reference count for a table."""

    @abstractmethod
    def decref(self, table_name: str) -> None:
        """Decrement reference count for a table."""
```

### Implementation 1: `LocalLifecycleHandler` (current behavior)

Wraps the existing `TableWorker` logic — background thread with queue-based refcounting and automatic `DROP TABLE` when refcount reaches 0. This is the default when no orchestration context is provided.

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

Uses PostgreSQL (via the orchestration engine) to track table references across distributed workers. Has its own **background worker thread** (similar to `TableWorker`) that runs independently of the `OrchContext` event loop. This is necessary because:

- `incref`/`decref` are called synchronously from `Object.__del__` and `Object._register`
- PG writes are async — a background thread with its own event loop bridges the sync/async boundary
- The worker must be independent of `OrchContext` so it doesn't interfere with the orchestration loop or get blocked by it

Tables are NOT dropped locally — instead, refcounts are managed in a PostgreSQL table, and cleanup is deferred to a coordinated process (e.g., job completion cleanup that drops all tables with refcount 0).

```python
class PgLifecycleHandler(LifecycleHandler):
    """Distributed lifecycle via PostgreSQL reference tracking.

    Uses a background worker thread with its own asyncio event loop
    to process incref/decref operations against PostgreSQL.
    Completely independent of the OrchContext event loop.
    """

    def __init__(self, orch_engine: AsyncEngine):
        self._engine = orch_engine
        self._queue: queue.Queue[PgLifecycleMessage] = queue.Queue()
        self._thread: threading.Thread | None = None

    async def start(self) -> None:
        # Start background thread with its own event loop for PG operations
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    async def stop(self) -> None:
        # Signal shutdown and wait for all pending PG writes to flush
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.SHUTDOWN, ""))
        self._thread.join()

    def incref(self, table_name: str) -> None:
        # Non-blocking: enqueue PG refcount increment
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.INCREF, table_name))

    def decref(self, table_name: str) -> None:
        # Non-blocking: enqueue PG refcount decrement
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.DECREF, table_name))

    def _run(self) -> None:
        """Background thread: own event loop, processes queue, writes to PG."""
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._process_queue())
        finally:
            loop.close()

    async def _process_queue(self) -> None:
        """Process incref/decref messages and write to PostgreSQL."""
        # Create own async PG engine (independent of OrchContext)
        engine = create_async_engine(...)
        try:
            while True:
                msg = self._queue.get()
                if msg.op == PgLifecycleOp.SHUTDOWN:
                    break
                elif msg.op == PgLifecycleOp.INCREF:
                    async with AsyncSession(engine) as session:
                        # UPSERT refcount row
                        ...
                elif msg.op == PgLifecycleOp.DECREF:
                    async with AsyncSession(engine) as session:
                        # Decrement refcount (do NOT drop table)
                        ...
        finally:
            await engine.dispose()
```

**Key architectural difference from `LocalLifecycleHandler`:**
- `LocalLifecycleHandler` background thread uses a sync ClickHouse client and drops tables directly
- `PgLifecycleHandler` background thread runs its own `asyncio` event loop with its own async PG engine, completely decoupled from the main event loop and `OrchContext`

### DataContext Constructor Change

Add an optional `orch_engine` parameter (from `OrchContext.engine`) to `DataContext`. When provided, use `PgLifecycleHandler`; when `None` (default), use `LocalLifecycleHandler`.

```python
class DataContext:
    def __init__(
        self,
        creds: ClickHouseCreds | None = None,
        engine: EngineType | None = None,
        orch_engine: AsyncEngine | None = None,  # NEW
    ):
        self._orch_engine = orch_engine
        # ... existing init ...

    async def __aenter__(self):
        # ... existing client init ...

        # Choose lifecycle handler based on orch_engine
        if self._orch_engine is not None:
            self._lifecycle = PgLifecycleHandler(self._orch_engine)
        else:
            self._lifecycle = LocalLifecycleHandler(self._creds)

        await self._lifecycle.start()
        # ... rest of existing __aenter__ ...
```

### Integration with Orchestration

In `execute_task()` (orchestration/execution.py), the `DataContext` is created with access to the `OrchContext.engine`:

```python
async def execute_task(task: Task) -> Any:
    func = import_callback(task.entrypoint)
    kwargs = deserialize_task_params(task.kwargs)

    orch_ctx = get_orch_context()

    with capture_task_output(task.id):
        # Pass orch engine to DataContext for distributed lifecycle
        async with DataContext(orch_engine=orch_ctx.engine):
            if asyncio.iscoroutinefunction(func):
                result = await func(**kwargs)
            else:
                result = func(**kwargs)

    return result
```

## Files to Change

| File | Change |
|------|--------|
| `aaiclick/data/lifecycle.py` | **NEW** — `LifecycleHandler` ABC, `LocalLifecycleHandler`, `PgLifecycleHandler` |
| `aaiclick/data/data_context.py` | Add `orch_engine` param, use `self._lifecycle` instead of `self._worker` directly |
| `aaiclick/data/__init__.py` | Export `LifecycleHandler`, `LocalLifecycleHandler`, `PgLifecycleHandler` |
| `aaiclick/orchestration/execution.py` | Pass `orch_ctx.engine` into `DataContext(orch_engine=...)` |
| `aaiclick/data/test_lifecycle.py` | **NEW** — Tests for both lifecycle handler implementations |

## Implementation Phases

### Phase 1: Extract Abstract Class & Local Implementation
1. Create `aaiclick/data/lifecycle.py` with `LifecycleHandler` ABC and `LocalLifecycleHandler`
2. Refactor `DataContext` to use `self._lifecycle: LifecycleHandler` instead of `self._worker: TableWorker`
3. Add `orch_engine` parameter to `DataContext.__init__` (default `None`)
4. When `orch_engine is None`, instantiate `LocalLifecycleHandler` (preserves all current behavior)
5. Update `DataContext.incref()`, `DataContext.decref()`, `__aenter__`, `__aexit__` to delegate to `self._lifecycle`

### Phase 2: PostgreSQL Lifecycle Handler
1. Implement `PgLifecycleHandler` in `lifecycle.py` with its own background worker thread
2. Background worker runs its own `asyncio` event loop with its own `AsyncEngine` (independent of `OrchContext`)
3. Design the PostgreSQL table for tracking CH table refcounts (could be a new `object_refs` table or extend existing orchestration models)
4. When `orch_engine is not None` in `DataContext`, instantiate `PgLifecycleHandler`
5. `incref`/`decref` enqueue messages to the background thread (non-blocking, same pattern as `TableWorker`)
6. `stop()` signals shutdown and joins the background thread, ensuring all pending PG writes flush

### Phase 3: Orchestration Integration
1. Update `execute_task()` to pass `orch_ctx.engine` into `DataContext`
2. Add job-level cleanup: when a job completes, drop all tables with refcount 0

## Design Decisions

- **Why `orch_engine` and not `orch_context`?** The DataContext only needs the SQLAlchemy engine to manage PG lifecycle. Passing the full `OrchContext` would create a tighter coupling. The engine is the minimal dependency.
- **Why a separate background thread for `PgLifecycleHandler`?** The `incref`/`decref` calls are synchronous (called from `Object.__del__` and `Object._register`), but PG writes are async. A background thread with its own event loop bridges this gap without interfering with the main event loop or `OrchContext`'s operations. This mirrors the same pattern used by `TableWorker` in `LocalLifecycleHandler`.
- **Why not reuse `OrchContext`'s engine in the background thread?** The background thread runs its own `asyncio` event loop. SQLAlchemy `AsyncEngine` is bound to the event loop that created it. The PG handler must create its own engine in its own loop to avoid cross-loop issues.
- **`incref`/`decref` stay synchronous on DataContext** — both handler implementations use queue-based non-blocking dispatch internally. The `DataContext` interface doesn't change.
- **PG handler does NOT drop ClickHouse tables** — it only tracks refcounts in PostgreSQL. Table cleanup is a separate coordinated process at job completion, ensuring no worker drops a table another worker still needs.
