# Abstract Lifecycle Handler Plan

## Problem

Currently, `DataContext` handles object lifecycle (table creation tracking, reference counting, cleanup) directly via `TableWorker` — a local background thread that drops ClickHouse tables when refcount reaches 0. This works for single-process local runs, but in distributed orchestration (where tasks run across multiple workers), table lifecycle must be coordinated through PostgreSQL so that:

- Tables are not dropped while other workers still reference them
- Table cleanup happens when all distributed references are released
- The orchestration backend tracks which tables belong to which jobs/tasks

## Design

### Two Independent Concerns

1. **Refcount tracking** (incref/decref) — happens inline via `LifecycleHandler`, used by DataContext
2. **Table cleanup** (DROP TABLE) — separate background worker that polls PG, completely independent

### Abstract Base Class: `LifecycleHandler`

Extract the lifecycle interface from `DataContext` into an abstract base class:

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

**Focused on incref/decref only.** Writes refcount changes to PostgreSQL. Does NOT drop tables — that's the background cleanup worker's job.

**Owned by**: Whoever starts it (background services, worker startup). Passed into DataContext via injection.

How sync `incref`/`decref` bridge to async:
- `queue.Queue` (thread-safe) receives sync calls from `Object.__del__` / `Object._register`
- `asyncio.Task` uses `loop.run_in_executor(None, queue.get)` to drain without blocking the event loop
- Own PG engine — fully decoupled from OrchContext

```python
class PgLifecycleHandler(LifecycleHandler):
    """Distributed lifecycle via PostgreSQL reference tracking.

    Only handles incref/decref. Table cleanup is a separate background worker.
    Owns its own PG engine — independent of OrchContext.
    """

    def __init__(self):
        self._queue: queue.Queue[PgLifecycleMessage] = queue.Queue()
        self._task: asyncio.Task | None = None
        self._engine: AsyncEngine | None = None

    async def start(self) -> None:
        self._engine = create_async_engine(get_pg_url())
        self._task = asyncio.create_task(self._process_loop())

    async def stop(self) -> None:
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.SHUTDOWN, ""))
        await self._task  # wait for drain
        if self._engine:
            await self._engine.dispose()
            self._engine = None

    def incref(self, table_name: str) -> None:
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.INCREF, table_name))

    def decref(self, table_name: str) -> None:
        self._queue.put(PgLifecycleMessage(PgLifecycleOp.DECREF, table_name))

    async def _process_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            msg = await loop.run_in_executor(None, self._queue.get)
            if msg.op == PgLifecycleOp.SHUTDOWN:
                break
            # Own engine, own sessions — no OrchContext dependency
            async with AsyncSession(self._engine) as session:
                if msg.op == PgLifecycleOp.INCREF:
                    # UPSERT refcount row
                    ...
                elif msg.op == PgLifecycleOp.DECREF:
                    # Decrement refcount (do NOT drop table)
                    ...
```

### Background Cleanup Worker (completely separate)

A separate background service that polls the PG refcount table every N seconds and drops CH tables where refcount <= 0. Totally independent of DataContext and OrchContext.

Started/stopped via `aaiclick background start/stop` CLI.

```python
class PgCleanupWorker:
    """Background worker that drops CH tables with refcount <= 0.

    Polls PostgreSQL every N seconds. Completely independent of
    DataContext and OrchContext. Has own PG engine and CH client.
    """

    def __init__(self, poll_interval: float = 10.0):
        self._poll_interval = poll_interval
        self._task: asyncio.Task | None = None
        self._engine: AsyncEngine | None = None
        self._shutdown: asyncio.Event | None = None

    async def start(self) -> None:
        self._engine = create_async_engine(get_pg_url())
        self._shutdown = asyncio.Event()
        self._task = asyncio.create_task(self._cleanup_loop())

    async def stop(self) -> None:
        self._shutdown.set()
        await self._task
        if self._engine:
            await self._engine.dispose()

    async def _cleanup_loop(self) -> None:
        while not self._shutdown.is_set():
            # SELECT table_name FROM refcounts WHERE refcount <= 0
            # For each: DROP TABLE IF EXISTS in ClickHouse
            # Then: DELETE FROM refcounts WHERE table_name = ...
            await self._do_cleanup()

            # Wait for poll_interval or shutdown signal
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(),
                    timeout=self._poll_interval,
                )
            except asyncio.TimeoutError:
                pass  # Normal: timeout means keep polling
```

### Architecture

```
DataContext (per task, short-lived)
├── ClickHouse AsyncClient
└── self._lifecycle: LifecycleHandler (incref/decref only)
    ├── LocalLifecycleHandler   (local mode: incref/decref + DROP TABLE)
    └── PgLifecycleHandler      (distributed: incref/decref → PG writes only)

PgCleanupWorker (completely independent, long-lived)
├── Own AsyncEngine (PostgreSQL)
├── Own ClickHouse client
└── Polls every N seconds:
    SELECT tables WHERE refcount <= 0 → DROP TABLE → DELETE row

OrchContext (unchanged — no lifecycle concerns)
├── AsyncEngine (PostgreSQL)
└── Task/Job/Worker management only
```

### DataContext Changes — Explicit Injection

DataContext accepts an optional `lifecycle` parameter. When provided, it uses the injected handler (shared, not owned). When `None`, it creates its own `LocalLifecycleHandler` (owned).

```python
class DataContext:
    def __init__(
        self,
        creds: ClickHouseCreds | None = None,
        engine: EngineType | None = None,
        lifecycle: LifecycleHandler | None = None,  # NEW
    ):
        self._creds = creds or get_ch_creds()
        self._injected_lifecycle = lifecycle
        self._lifecycle: LifecycleHandler | None = None
        self._owns_lifecycle: bool = False

    async def __aenter__(self):
        # ... existing client init ...

        if self._injected_lifecycle is not None:
            self._lifecycle = self._injected_lifecycle
            self._owns_lifecycle = False
        else:
            self._lifecycle = LocalLifecycleHandler(self._creds)
            await self._lifecycle.start()
            self._owns_lifecycle = True

        # ... set ContextVar ...
        return self

    async def __aexit__(self, ...):
        # ... mark objects stale ...

        if self._owns_lifecycle and self._lifecycle:
            await self._lifecycle.stop()
        self._lifecycle = None

        # ... reset ContextVar ...
```

### Worker Startup Integration

Worker startup creates PgLifecycleHandler and PgCleanupWorker, passes lifecycle into execute_task:

```python
# In __main__.py — worker start
async def start_worker():
    pg_lifecycle = PgLifecycleHandler()
    pg_cleanup = PgCleanupWorker()
    await pg_lifecycle.start()
    await pg_cleanup.start()
    try:
        async with OrchContext():
            await worker_main_loop(
                max_tasks=args.max_tasks,
                lifecycle=pg_lifecycle,
            )
    finally:
        await pg_lifecycle.stop()
        await pg_cleanup.stop()
```

In `execute_task()`:

```python
async def execute_task(task: Task, lifecycle: LifecycleHandler | None = None) -> Any:
    func = import_callback(task.entrypoint)
    kwargs = deserialize_task_params(task.kwargs)

    with capture_task_output(task.id):
        async with DataContext(lifecycle=lifecycle):
            if asyncio.iscoroutinefunction(func):
                result = await func(**kwargs)
            else:
                result = func(**kwargs)

    return result
```

### Standalone Background CLI

For running cleanup independently (e.g., dedicated cleanup process):

```bash
aaiclick background start   # runs PgCleanupWorker standalone
```

```python
# In __main__.py
async def start_background():
    cleanup = PgCleanupWorker()
    await cleanup.start()
    # Wait for SIGTERM/SIGINT
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)
    await shutdown.wait()
    await cleanup.stop()
```

## Files to Change

| File | Change |
|------|--------|
| `aaiclick/data/lifecycle.py` | **NEW** — `LifecycleHandler` ABC, `LocalLifecycleHandler` |
| `aaiclick/data/data_context.py` | Add `lifecycle` param, use `self._lifecycle` instead of `self._worker` directly |
| `aaiclick/data/__init__.py` | Export `LifecycleHandler`, `LocalLifecycleHandler` |
| `aaiclick/orchestration/pg_lifecycle.py` | **NEW** — `PgLifecycleHandler` (incref/decref to PG only) |
| `aaiclick/orchestration/pg_cleanup.py` | **NEW** — `PgCleanupWorker` (polls PG, drops CH tables) |
| `aaiclick/orchestration/execution.py` | Pass `lifecycle` into `DataContext(lifecycle=...)` |
| `aaiclick/orchestration/worker.py` | Accept and forward `lifecycle` param |
| `aaiclick/__main__.py` | Wire up PgLifecycleHandler + PgCleanupWorker in worker start; add `background start` CLI |
| `aaiclick/data/test_lifecycle.py` | **NEW** — Tests for `LocalLifecycleHandler` |

## Implementation Phases

### Phase 1: Extract Abstract Class & Local Implementation
1. Create `aaiclick/data/lifecycle.py` with `LifecycleHandler` ABC and `LocalLifecycleHandler`
2. Refactor `DataContext` to use `self._lifecycle: LifecycleHandler` instead of `self._worker: TableWorker`
3. Add `lifecycle` parameter to `DataContext.__init__` (default `None`)
4. When `lifecycle is None`, create and own `LocalLifecycleHandler` (preserves all current behavior)
5. When `lifecycle` is provided, use it as shared reference (`_owns_lifecycle = False`)
6. Update `DataContext.incref()`, `DataContext.decref()`, `__aenter__`, `__aexit__` to delegate to `self._lifecycle`

### Phase 2: PgLifecycleHandler (incref/decref only)
1. Create `aaiclick/orchestration/pg_lifecycle.py` with `PgLifecycleHandler`
2. Own PG engine — independent of OrchContext
3. Uses `queue.Queue` + `run_in_executor` to bridge sync callers to async processing
4. Design the PostgreSQL table for tracking CH table refcounts
5. UPSERT for incref, decrement for decref. No cleanup.

### Phase 3: PgCleanupWorker (background table dropper)
1. Create `aaiclick/orchestration/pg_cleanup.py` with `PgCleanupWorker`
2. Own PG engine + own CH client
3. Polls every N seconds: SELECT WHERE refcount <= 0 → DROP TABLE → DELETE row
4. Clean shutdown via asyncio.Event

### Phase 4: Integration
1. Update `execute_task()` to accept and pass `lifecycle` param
2. Update `worker_main_loop()` to accept and forward `lifecycle` param
3. Wire up in `__main__.py` worker start: create PgLifecycleHandler + PgCleanupWorker
4. Add `aaiclick background start` CLI command for standalone cleanup process

## Design Decisions

- **Why two separate components (PgLifecycleHandler vs PgCleanupWorker)?** Separation of concerns. Refcount tracking must be fast and inline (called from Object.__del__). Cleanup is slow, periodic, and independent. They don't need to know about each other.
- **Why PgLifecycleHandler has own PG engine?** Fully decoupled from OrchContext. Can be started/stopped independently. No dependency on OrchContext lifecycle.
- **Why PgCleanupWorker has own CH client?** It needs to DROP tables in ClickHouse. Completely independent — can even run in a separate process.
- **Why inject `lifecycle` handler into DataContext?** Explicit dependency injection. DataContext doesn't know about OrchContext, PG, or background services. The caller decides which lifecycle strategy to use.
- **Why `_owns_lifecycle` flag?** DataContext must know whether to call `stop()` on exit. Shared handlers (PgLifecycleHandler) are stopped by whoever started them. Local handlers are stopped by the DataContext that created them.
- **Why `queue.Queue` (not `asyncio.Queue`)?** `incref`/`decref` are called from sync code (`Object.__del__`). `queue.Queue.put()` is thread-safe and non-blocking. The asyncio task uses `run_in_executor` to bridge the blocking `get()`.
- **Why `PgLifecycleHandler` lives in `orchestration/`, not `data/`?** It depends on PG models and SQLAlchemy. `LocalLifecycleHandler` lives in `data/` because it only depends on ClickHouse. Each handler lives with its dependencies.
- **PgLifecycleHandler does NOT drop ClickHouse tables** — it only tracks refcounts in PostgreSQL. PgCleanupWorker handles the actual cleanup, ensuring no worker drops a table another worker still needs.
- **OrchContext is unchanged** — it manages orchestration DB (tasks, jobs, workers). Lifecycle is not its concern.

## Failure Handling & Dangling Tables

### Problem

Tables can become "dangling" — existing in ClickHouse with no live reference tracking them. This happens when the refcount system fails to record a table's existence before a crash.

### Failure Scenarios

#### Scenario 1: Worker crash after CREATE TABLE, before incref reaches PG

```
Timeline:
1. create_object() → CREATE TABLE t_abc in ClickHouse     ✓
2. obj._register(ctx) → ctx.incref("t_abc")               ✓
3. PgLifecycleHandler queues INCREF message                ✓
4. *** Worker crashes (SIGKILL, OOM) ***
5. Asyncio task never writes incref to PG                  ✗
```

**Result**: Table `t_abc` exists in ClickHouse. No `table_refcounts` row in PG. PgCleanupWorker doesn't know about it. **Permanently orphaned.**

This is the most dangerous scenario — the table is invisible to the cleanup system.

#### Scenario 2: Worker crash with pending decrefs

```
Timeline:
1. Object goes out of scope → decref queued
2. *** Worker crashes ***
3. Decref never written to PG
```

**Result**: Refcount in PG is higher than reality. Table won't be cleaned up until refcount eventually reaches 0 through other decrefs or sweeping. **Delayed cleanup**, not data loss.

#### Scenario 3: PgCleanupWorker crash

**Result**: Tables with refcount <= 0 accumulate in PG. Not dangerous — cleanup resumes when worker restarts. **Self-healing.**

#### Scenario 4: Refcount skew from partial flush

If a worker crashes mid-batch (some increfs written, others not), refcounts become inaccurate. Decrefs from other workers may drive refcount to 0 prematurely.

**Result**: Table dropped while another worker still references it. Worker gets "table not found" error. **Data loss for in-flight operations.**

### Solution: Table Sweeper

A periodic sweeper that catches all orphaned tables regardless of cause.

#### How It Works

All aaiclick tables follow the naming convention `t{snowflake_id}`. The snowflake ID encodes a creation timestamp. The sweeper:

1. Lists all `t*` tables in ClickHouse
2. Extracts the creation timestamp from each table's snowflake ID
3. For tables older than a threshold (e.g., 1 hour):
   - Checks if a `table_refcounts` row exists in PG
   - If no row exists → table is orphaned → DROP it
4. For tables with a refcount row where refcount <= 0:
   - Already handled by PgCleanupWorker (redundant but harmless)

#### Why Age Threshold?

A table that was just created may not have its incref flushed to PG yet. The age threshold (e.g., 1 hour) ensures we never drop a table that's actively being created. Since snowflake IDs encode timestamps, we can determine table age without any metadata lookup.

```python
class TableSweeper:
    """Periodic sweeper for orphaned ClickHouse tables.

    Catches tables that the refcount system missed (e.g., worker crashes).
    Uses snowflake ID timestamps to determine table age.
    """

    def __init__(self, max_age_seconds: float = 3600.0, poll_interval: float = 300.0):
        self._max_age = max_age_seconds
        self._poll_interval = poll_interval

    async def _sweep(self) -> int:
        """One sweep pass. Returns number of tables dropped."""
        # 1. List all t* tables in ClickHouse
        ch_tables = await self._ch_client.query(
            "SELECT name FROM system.tables "
            "WHERE database = currentDatabase() AND name LIKE 't%'"
        )

        # 2. Filter to tables older than threshold (via snowflake timestamp)
        now = time.time()
        old_tables = []
        for (name,) in ch_tables.result_rows:
            try:
                snowflake_id = int(name[1:])  # strip 't' prefix
                created_ms = extract_timestamp(snowflake_id)
                age_seconds = now - (created_ms / 1000.0)
                if age_seconds > self._max_age:
                    old_tables.append(name)
            except (ValueError, IndexError):
                continue  # Not a snowflake-named table

        if not old_tables:
            return 0

        # 3. Check which have refcount rows in PG
        async with AsyncSession(self._engine) as session:
            result = await session.execute(
                text("SELECT table_name FROM table_refcounts "
                     "WHERE table_name = ANY(:names)"),
                {"names": old_tables},
            )
            tracked = {row[0] for row in result.fetchall()}

        # 4. Drop orphaned tables (old + no refcount row)
        orphaned = [t for t in old_tables if t not in tracked]
        for table_name in orphaned:
            await self._ch_client.command(f"DROP TABLE IF EXISTS {table_name}")

        return len(orphaned)
```

#### Integration

The sweeper runs alongside PgCleanupWorker. It's a separate concern:
- **PgCleanupWorker**: Drops tables that the refcount system knows about (refcount <= 0)
- **TableSweeper**: Catches tables the refcount system missed (no PG row at all)

Both can run in the `aaiclick background start` process:

```python
async def start_background():
    cleanup = PgCleanupWorker(poll_interval=10.0)
    sweeper = TableSweeper(max_age_seconds=3600.0, poll_interval=300.0)
    await cleanup.start()
    await sweeper.start()
    # wait for shutdown...
    await sweeper.stop()
    await cleanup.stop()
```

#### Sweeper vs Other Approaches

| Approach | Prevents orphans? | Adds latency? | Catches all cases? |
|----------|-------------------|---------------|-------------------|
| **Sweeper** (chosen) | No (detects after) | No | Yes — any orphan older than threshold |
| Write-ahead incref | Yes (prevents) | Yes — sync PG write per table creation | No — doesn't catch external table drops |
| Job-scoped registry | Partially | No | No — doesn't catch tables outside jobs |

The sweeper is the simplest, most resilient approach. It doesn't prevent orphans — it detects and cleans them up after the fact. Since the age threshold is configurable (default 1 hour), the worst case is temporary storage waste, never data loss from premature deletion.

#### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_age_seconds` | 3600 (1 hour) | Only sweep tables older than this |
| `poll_interval` | 300 (5 min) | How often to run a sweep pass |

The sweeper is intentionally slow (5-minute interval) because it queries `system.tables` which can be expensive on large ClickHouse clusters. The PgCleanupWorker handles the fast path (10-second interval for known refcount=0 tables).
