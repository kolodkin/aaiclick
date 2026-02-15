# Object Lifecycle Tracking - Implementation Plan

**Design Document**: [object_lifecycle.md](object_lifecycle.md)

## Refactor Note

This implementation requires refactoring existing code:

1. **Extract ClickHouseCreds**: Currently connection params are scattered (env vars read in multiple places). Consolidate into `ClickHouseCreds` dataclass.

2. **Update get_ch_client()**: Accept `ClickHouseCreds` parameter instead of reading env vars directly.

3. **DataContext signature change**: Add optional `creds` parameter to `__init__`.

## Phases

### Phase 0: Refactor Prerequisites ✅

- [x] Create `ClickHouseCreds` dataclass in `aaiclick/data/models.py`
- [x] Add `get_ch_creds()` helper function in `aaiclick/data/env.py`
- [x] Update `get_ch_client()` to accept `ClickHouseCreds`
- [x] Update `DataContext.__init__` to accept optional `creds` parameter

### Phase 1: Core Infrastructure ✅

- [x] Create `TableMessage` and `TableOp` in `aaiclick/data/table_worker.py`
- [x] Implement `TableWorker` class
- [x] Add unit tests for worker (mock ClickHouse client)

### Phase 2: DataContext Integration ✅

- [x] Remove `_objects` weakref dict from `DataContext`
- [x] Remove `_register_object()` method
- [x] Add `_worker` attribute and `incref()`/`decref()` methods
- [x] Update `__aenter__` to start worker
- [x] Update `__aexit__` to stop worker
- [x] Remove `_delete_object()` and `delete()` methods

### Phase 3: Object Integration ✅

- [x] Add `_data_ctx_ref` attribute to `Object`
- [x] Add `_register()` method to `Object`
- [x] Implement `Object.__del__()` with guards
- [x] Update `create_object()` to call `obj._register(ctx)`

### Phase 4: View Integration ✅

- [x] Update `View.__init__()` to incref source's table
- [x] Implement `View.__del__()` with guards
- [x] Handle View-of-View case (uses source's table via `self._source.table`)

### Phase 5: Testing ✅

- [x] Test `__del__` guard: unregistered object
- [x] Test `__del__` guard: interpreter shutdown (mocked `sys.is_finalizing`)
- [x] Test `__del__` guard: after context exit
- [x] Test View `__del__` guard: interpreter shutdown
- [x] Test View `__del__` guard: after context exit

## Implementation Files

| Component | File |
|-----------|------|
| `ClickHouseCreds` | `aaiclick/data/models.py` |
| `get_ch_creds()` | `aaiclick/data/env.py` |
| `TableWorker`, `TableOp`, `TableMessage` | `aaiclick/data/table_worker.py` |
| `DataContext.incref()`, `decref()` | `aaiclick/data/data_context.py` |
| `Object._register()`, `__del__()` | `aaiclick/data/object.py` |
| `View.__init__()`, `__del__()` | `aaiclick/data/object.py` |

## Future: PostgreSQL-Based Registry

For distributed orchestration, the refcount tracking will move to PostgreSQL:

- `TableWorker` will write to PostgreSQL instead of in-memory dict
- Background job in orchestration service will process drops
- Same queue-based interface for Objects/Views

This is out of scope for the initial implementation.
