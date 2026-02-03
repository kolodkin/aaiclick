# Object Lifecycle Tracking

## Overview

This document specifies the design for automatic table lifecycle management in DataContext. Tables are tracked via reference counting and dropped when no Objects reference them.

## Problem Statement

Currently, all tables are dropped when DataContext exits. This is wasteful for long-running contexts where intermediate results are no longer needed. We need:

1. Track table references (Object/View creation increments, deletion decrements)
2. Drop tables when refcount reaches 0
3. Thread-safe (Objects can be deleted from any thread via `__del__`)
4. Clean shutdown (wait for pending operations, drop remaining tables)

## Design

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      DataContext                             │
│                                                              │
│  __aenter__()  ──► Start TableWorker thread                 │
│  __aexit__()   ──► Stop TableWorker, wait, cleanup          │
│                                                              │
│  ┌──────────┐    Queue     ┌─────────────────────────────┐  │
│  │  Object  │──(INCREF)───►│                             │  │
│  │  __init__│              │     TableWorker (thread)    │  │
│  └──────────┘              │                             │  │
│                            │  refcounts: {table: count}  │  │
│  ┌──────────┐              │                             │  │
│  │  Object  │──(DECREF)───►│  - INCREF: count++          │  │
│  │  __del__ │              │  - DECREF: count--          │  │
│  └──────────┘              │     if count==0: DROP TABLE │  │
│                            │  - SHUTDOWN: cleanup all    │  │
│  ┌──────────┐              │                             │  │
│  │   View   │──(INCREF)───►│                             │  │
│  │  __init__│              └─────────────────────────────┘  │
│  └──────────┘                        │                      │
│                                      │ sync HTTP            │
│                                      ▼                      │
│                               ClickHouse                    │
└─────────────────────────────────────────────────────────────┘
```

### Components

#### 1. ClickHouseCreds

Dataclass bundling ClickHouse connection parameters.

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class ClickHouseCreds:
    """ClickHouse connection credentials."""
    host: str = "localhost"
    port: int = 8123
    user: str = "default"
    password: str = ""
    database: str = "default"
```

#### 2. TableMessage

Message passed to worker via queue.

```python
from dataclasses import dataclass
from enum import Enum, auto

class TableOp(Enum):
    INCREF = auto()
    DECREF = auto()
    SHUTDOWN = auto()

@dataclass
class TableMessage:
    op: TableOp
    table_name: str = ""
```

#### 3. TableWorker

Background thread managing table lifecycle.

```python
import queue
import threading
from clickhouse_connect import get_client

class TableWorker:
    """Background worker that manages table lifecycle via refcounting."""

    def __init__(self, creds: ClickHouseCreds):
        """Initialize worker with ClickHouse credentials."""
        self._creds = creds
        self._ch_client = None  # Created in thread
        self._queue: queue.Queue[TableMessage] = queue.Queue()
        self._refcounts: dict[str, int] = {}
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        """Start the worker thread."""
        self._thread.start()

    def stop(self) -> None:
        """Stop worker and wait for completion. Blocks until done."""
        self._queue.put(TableMessage(TableOp.SHUTDOWN))
        self._thread.join()

    def incref(self, table_name: str) -> None:
        """Increment reference count for table. Non-blocking."""
        self._queue.put(TableMessage(TableOp.INCREF, table_name))

    def decref(self, table_name: str) -> None:
        """Decrement reference count for table. Non-blocking."""
        self._queue.put(TableMessage(TableOp.DECREF, table_name))

    def _run(self) -> None:
        """Worker loop - runs in background thread."""
        # Create sync client in worker thread
        self._ch_client = get_client(
            host=self._creds.host,
            port=self._creds.port,
            username=self._creds.user,
            password=self._creds.password,
            database=self._creds.database,
        )

        try:
            while True:
                msg = self._queue.get()

                if msg.op == TableOp.SHUTDOWN:
                    self._cleanup_all()
                    break

                elif msg.op == TableOp.INCREF:
                    self._refcounts[msg.table_name] = (
                        self._refcounts.get(msg.table_name, 0) + 1
                    )

                elif msg.op == TableOp.DECREF:
                    if msg.table_name in self._refcounts:
                        self._refcounts[msg.table_name] -= 1
                        if self._refcounts[msg.table_name] <= 0:
                            self._drop_table(msg.table_name)
                            del self._refcounts[msg.table_name]
        finally:
            if self._ch_client:
                self._ch_client.close()

    def _drop_table(self, table_name: str) -> None:
        """Drop single table. Best effort."""
        try:
            self._ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass  # Best effort - table may already be gone

    def _cleanup_all(self) -> None:
        """Drop all remaining tables on shutdown."""
        for table_name in list(self._refcounts.keys()):
            self._drop_table(table_name)
        self._refcounts.clear()
```

#### 4. DataContext Integration

```python
class DataContext:
    def __init__(self, creds: ClickHouseCreds | None = None, engine: EngineType | None = None):
        self._creds = creds or ClickHouseCreds()
        self._ch_client: AsyncClient | None = None
        self._worker: TableWorker | None = None
        self._token = None
        self._engine: EngineType = engine if engine is not None else ENGINE_DEFAULT

    def incref(self, table_name: str) -> None:
        """Increment reference count for table. Thread-safe, non-blocking."""
        if self._worker is not None:
            self._worker.incref(table_name)

    def decref(self, table_name: str) -> None:
        """Decrement reference count for table. Thread-safe, non-blocking."""
        if self._worker is not None:
            self._worker.decref(table_name)

    async def __aenter__(self):
        """Enter context: start client and worker."""
        if self._ch_client is None:
            self._ch_client = await get_ch_client(self._creds)

        # Start background worker
        self._worker = TableWorker(self._creds)
        self._worker.start()

        self._token = _current_context.set(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit context: stop worker and cleanup."""
        # Stop worker (blocks until all tables dropped)
        if self._worker:
            self._worker.stop()
            self._worker = None

        # Reset context var
        _current_context.reset(self._token)

        return False
```

#### 5. Object Integration

```python
import sys
import weakref

class Object:
    def __init__(self, table: str | None = None):
        self._table_name = table if table is not None else f"t{get_snowflake_id()}"
        self._stale = False
        self._context_ref: weakref.ref[DataContext] | None = None

    def _register(self, context: DataContext) -> None:
        """Register this object with context."""
        self._context_ref = weakref.ref(context)
        context.incref(self._table_name)

    def __del__(self):
        """Decrement refcount on deletion."""
        # Guard 1: Interpreter shutdown
        if sys.is_finalizing():
            return

        # Guard 2: Never registered
        if self._context_ref is None:
            return

        # Guard 3: Context gone
        context = self._context_ref()
        if context is None:
            return

        # Decref (handles worker=None internally)
        context.decref(self._table_name)
```

#### 6. View Integration

Views reference the same table as their source, so they also incref/decref.

```python
class View(Object):
    def __init__(
        self,
        source: Object,
        where: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order_by: str | None = None,
    ):
        # Don't call super().__init__ - we share source's table
        self._source = source
        self._where = where
        self._limit = limit
        self._offset = offset
        self._order_by = order_by
        self._context_ref: weakref.ref[DataContext] | None = None

        # Incref same table as source
        if source._context_ref is not None:
            context = source._context_ref()
            if context is not None:
                self._context_ref = weakref.ref(context)
                context.incref(source.table)

    def __del__(self):
        """Decrement refcount on deletion."""
        if sys.is_finalizing():
            return

        if self._context_ref is None:
            return

        context = self._context_ref()
        if context is None:
            return

        # Decref the source's table
        context.decref(self._source.table)
```

### Registration Flow

```python
async def create_object(schema: Schema, engine: EngineType | None = None) -> Object:
    """Create object and register with worker."""
    ctx = get_data_context()
    obj = Object()

    # ... create table SQL ...

    # Register with worker (incref)
    obj._register(ctx)

    return obj
```

### Sequence Diagrams

#### Normal Operation

```
Timeline
════════

1. Context enters
   └─► Worker thread starts

2. obj1 = create_object(...)
   └─► obj1._register(ctx)
   └─► ctx.incref("t123")
   └─► worker: refcounts["t123"] = 1

3. obj2 = obj1.view(where="value > 0")
   └─► ctx.incref("t123")
   └─► worker: refcounts["t123"] = 2

4. del obj2  (or goes out of scope)
   └─► obj2.__del__()
   └─► ctx.decref("t123")
   └─► worker: refcounts["t123"] = 1

5. del obj1
   └─► obj1.__del__()
   └─► ctx.decref("t123")
   └─► worker: refcounts["t123"] = 0
   └─► worker: DROP TABLE t123
   └─► worker: del refcounts["t123"]

6. Context exits
   └─► worker.stop()
   └─► worker: cleanup remaining (none left)
   └─► thread.join() returns
```

#### Early Context Exit

```
Timeline
════════

1. Context enters, worker starts

2. obj1 = create_object(...)
   └─► ctx.incref("t123")
   └─► refcounts["t123"] = 1

3. obj2 = create_object(...)
   └─► ctx.incref("t456")
   └─► refcounts["t456"] = 1

4. Context exits (obj1, obj2 still exist in Python)
   └─► worker.stop()
   └─► worker: DROP TABLE t123
   └─► worker: DROP TABLE t456
   └─► thread.join() returns
   └─► context._worker = None

5. Later: del obj1
   └─► obj1.__del__()
   └─► ctx.decref("t123")
   └─► _worker is None → no-op
```

### Edge Cases

| Scenario | Handling |
|----------|----------|
| `__del__` during interpreter shutdown | `sys.is_finalizing()` check returns early |
| `__del__` after context GC'd | `weakref` returns `None`, return early |
| `__del__` after context exited | `ctx.decref()` is no-op when `_worker is None` |
| Multiple Objects same table | Each increfs/decrefs independently |
| View of View | View refs source's table (chains resolved) |
| Exception during context exit | Worker still stops, tables still dropped |

### Thread Safety

| Operation | Thread Safety |
|-----------|---------------|
| `queue.put()` | Thread-safe (Python queue) |
| `refcounts` dict | Single-threaded (only worker accesses) |
| `_context_ref` read | Atomic (Python GIL) |
| `DROP TABLE` | ClickHouse handles concurrency |

### Performance Considerations

1. **Queue overhead**: `queue.put()` is O(1), negligible overhead
2. **Worker latency**: Tables dropped asynchronously (non-blocking for Python code)
3. **Batch drops**: Could batch multiple drops, but single drops are fast enough
4. **Sync client**: Worker uses sync client (simpler, runs in own thread)

## Refactor Note

This implementation requires refactoring existing code:

1. **Extract ClickHouseCreds**: Currently connection params are scattered (env vars read in multiple places). Consolidate into `ClickHouseCreds` dataclass.

2. **Update get_ch_client()**: Accept `ClickHouseCreds` parameter instead of reading env vars directly.

3. **DataContext signature change**: Add optional `creds` parameter to `__init__`.

## Implementation Plan

### Phase 0: Refactor Prerequisites

- [ ] Create `ClickHouseCreds` dataclass in `aaiclick/data/models.py`
- [ ] Add `get_creds_from_env()` helper function
- [ ] Update `get_ch_client()` to accept `ClickHouseCreds`
- [ ] Update `DataContext.__init__` to accept optional `creds` parameter

### Phase 1: Core Infrastructure

- [ ] Create `TableMessage` and `TableOp` in new file `aaiclick/data/table_worker.py`
- [ ] Implement `TableWorker` class
- [ ] Add unit tests for worker (mock ClickHouse client)

### Phase 2: DataContext Integration

- [ ] Remove `_objects` weakref dict from `DataContext`
- [ ] Remove `_register_object()` method
- [ ] Add `_worker` attribute
- [ ] Update `__aenter__` to start worker
- [ ] Update `__aexit__` to stop worker
- [ ] Remove `_delete_object()` and `delete()` methods (no longer needed)

### Phase 3: Object Integration

- [ ] Add `_context_ref` attribute to `Object`
- [ ] Add `_register()` method to `Object`
- [ ] Implement `Object.__del__()` with guards
- [ ] Update `create_object()` to call `obj._register(ctx)`

### Phase 4: View Integration

- [ ] Update `View.__init__()` to incref source's table
- [ ] Implement `View.__del__()` with guards
- [ ] Handle View-of-View case (use ultimate source table)

### Phase 5: Testing

- [ ] Test basic incref/decref flow
- [ ] Test multiple objects same table
- [ ] Test View lifecycle
- [ ] Test context exit with pending objects
- [ ] Test `__del__` after context exit
- [ ] Test concurrent object deletion

## Future: PostgreSQL-Based Registry

For distributed orchestration, the refcount tracking will move to PostgreSQL:

- `TableWorker` will write to PostgreSQL instead of in-memory dict
- Background job in orchestration service will process drops
- Same queue-based interface for Objects/Views

This is out of scope for the initial implementation.
