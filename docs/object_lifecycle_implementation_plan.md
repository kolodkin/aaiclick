# Object Lifecycle Tracking - Implementation Plan

**Design Document**: [object_lifecycle.md](object_lifecycle.md)

## Refactor Note

This implementation requires refactoring existing code:

1. **Extract ClickHouseCreds**: Currently connection params are scattered (env vars read in multiple places). Consolidate into `ClickHouseCreds` dataclass.

2. **Update get_ch_client()**: Accept `ClickHouseCreds` parameter instead of reading env vars directly.

3. **DataContext signature change**: Add optional `creds` parameter to `__init__`.

## Phases

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
