Insert Advisory Lock for Concurrent Workers
---

Serialize concurrent inserts into the same shared ClickHouse table so
`generateSnowflakeID()` produces a contiguous, non-interleaved ID range per
insert. Distributed mode only — local mode (chdb + SQLite) is single-process
and needs no lock.

Tracked in `docs/future.md` (High Priority).

---

# Problem

`p_<name>` and (future) `j_<job_id>_<name>` tables permit append-on-existing
semantics: two workers calling `create_object_from_value(..., name="foo")`
or `insert_objects_db(...)` against the same destination both rely on
ClickHouse's `DEFAULT generateSnowflakeID()` to assign per-row IDs.

ClickHouse guarantees the IDs are unique, but it does not guarantee that one
worker's INSERT consumes a contiguous sequence range. Concurrent INSERTs
interleave at the row level, so worker A's rows end up with IDs
`[100, 102, 104, ...]` and worker B's with `[101, 103, 105, ...]`. The
`aai_id` column then no longer marks an insert-batch boundary, and any
downstream consumer that relies on per-batch contiguity breaks.

Operator-produced temp tables (`t_<snowflake_id>`) are immune — each gets a
unique name at creation, so there is no shared destination to race on. Only
named, append-able tables exhibit the problem.

---

# Design

A single PostgreSQL session-level advisory lock per shared table, held for
the duration of one CH INSERT. The lock key is a 64-bit `advisory_id`
minted once per table at registration time and stored in the existing SQL
`table_context_refs` row.

## Lock key: `advisory_id` on `table_context_refs`

`table_context_refs` already exists in the orchestration SQL schema — see
`TableContextRef` in `aaiclick/orchestration/lifecycle/db_lifecycle.py` —
and is the single choke point through which every tracked CH table passes
on its way into the system (via `OrchLifecycleHandler.incref`). Add one
column:

```python
class TableContextRef(SQLModel, table=True):
    __tablename__: ClassVar[str] = "table_context_refs"

    table_name:  str = Field(sa_column=Column(String,     primary_key=True))
    context_id:  int = Field(sa_column=Column(BigInteger, primary_key=True))
    advisory_id: int = Field(sa_column=Column(BigInteger, nullable=False))
```

Snowflake IDs are 63-bit unsigned and fit directly into `pg_advisory_lock(bigint)`
with no hashing or truncation, so collision risk between distinct table names
is zero by construction.

## Invariant: same `table_name` ⇒ same `advisory_id`

The PK on `table_context_refs` is composite `(table_name, context_id)`, so
the same `table_name` may appear in multiple rows. All such rows MUST carry
the same `advisory_id` value. Otherwise two workers in different contexts
would lock on different keys for the same physical CH table and the lock
would not actually serialize them.

This invariant is not expressible as a portable SQL constraint. It is
enforced in `OrchLifecycleHandler.handle_incref`:

```python
async def handle_incref(table_name: str, context_id: int) -> None:
    existing = await session.scalar(
        select(TableContextRef.advisory_id)
        .where(TableContextRef.table_name == table_name)
        .limit(1)
    )
    advisory_id = existing if existing is not None else get_snowflake_id()

    await session.execute(
        insert(TableContextRef)
        .values(
            table_name=table_name,
            context_id=context_id,
            advisory_id=advisory_id,
        )
        .on_conflict_do_nothing()
    )
```

The mint is a proposal; the existing row's value is the truth. Concurrent
first-inserts race in the SELECT-then-INSERT window; the loser's candidate
id is silently discarded. To eliminate even that small window, wrap the
mint path in a transient `pg_advisory_xact_lock(hashtext(table_name))` —
held only for the SQL transaction that creates the registry row, not for
the subsequent CH INSERT.

## Lock lifetime: session-level, scoped to one CH INSERT

The work being serialized is a CH statement, unrelated to any PG transaction.
Use `pg_advisory_lock` (session-level) rather than `pg_advisory_xact_lock`
(transaction-level) so the lock lifetime is detached from PG txn boundaries:

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def table_insert_lock(advisory_id: int):
    if not is_distributed():
        yield
        return
    async with sql_engine.connect() as conn:
        await conn.exec_driver_sql(
            "SELECT pg_advisory_lock(:k)", {"k": advisory_id}
        )
        try:
            yield
        finally:
            await conn.exec_driver_sql(
                "SELECT pg_advisory_unlock(:k)", {"k": advisory_id}
            )
```

PG releases session-level advisory locks automatically on backend
disconnect, so a worker crash mid-INSERT cannot strand the lock.

## Distributed-only branch

`aaiclick/backend.py` adds `is_distributed()` returning `is_postgres()`.
`table_insert_lock` short-circuits to a no-op `yield` when not distributed.
Local mode (chdb + SQLite) is single-process by chdb constraint — no
cross-process concurrency exists to serialize.

## Which call sites acquire the lock

| Operation                        | Destination                       | Lock |
|----------------------------------|-----------------------------------|------|
| `insert_objects_db`              | append rows to existing shared table | yes  |
| `concat_objects_db`              | merge sources into existing shared table | yes  |
| `create_object_from_value(name=)` (append path) | `p_<name>` already exists | yes  |
| `copy_db`                        | create + populate new destination | no — user-owned flow |
| Operator materializations        | `t_<snowflake_id>`                | no — unique per call |
| Reads, pure DDL                  | n/a                                | no   |

`copy_db` deliberately stays lock-free. Two concurrent copies into the same
named destination are a pipeline-design bug, not a framework concern. This
is documented as a small admonition on `copy()` in `docs/object.md`.

## Call sequence

For an `insert_objects_db(dest, src)` in distributed mode:

1. `advisory_id = await load_advisory_id(dest.table)` — single SELECT against
   `table_context_refs`, cached in-process by `(sql_url, table_name)`.
2. `async with table_insert_lock(advisory_id):` — blocks only workers
   targeting this same `advisory_id`; different tables never contend.
3. `INSERT INTO {dest.table} SELECT ... FROM {src.table}` — `generateSnowflakeID()`
   fires per row, producing a contiguous range.
4. Lock released on context exit. PG also auto-releases on disconnect.

---

# Guarantees

- Per-table: rows from one INSERT share a contiguous `aai_id` range, with no
  interleaving from concurrent workers writing to that same table.
- Cross-table: zero contention. Two workers writing to different `p_*`
  tables never block each other.
- Failure-safe: PG auto-releases session-level locks when the worker's
  backend disconnects.

---

# Non-goals

- IDs are not contiguous across distinct tables. CH's `generateSnowflakeID`
  counter ticks globally; the lock only promises per-table monotonicity.
- `copy_db` is not protected. Concurrent `copy()` to the same named
  destination remains a pipeline-design error.
- Existing inserts that have already produced interleaved IDs are not
  re-ordered. The lock prevents future interleaving only.

---

# Migration

Alembic migration `add_advisory_id_to_table_context_refs` (head:
`b7c8d9e0f1a2`):

1. Add `advisory_id BIGINT NULL` to `table_context_refs` via
   `op.batch_alter_table` for SQLite compatibility.
2. Backfill: one `get_snowflake_id()` per distinct `table_name`, applied to
   all rows sharing that name. The migration calls into `aaiclick.snowflake_id`
   so legacy tables and new tables share the same minting path.
3. `ALTER COLUMN advisory_id SET NOT NULL` once every row has a value.

The backfill is a no-op on fresh installs (zero rows) and does not touch CH
in that case. On installs with existing rows, it requires a working CH
connection — same dependency as any other Snowflake-minting code path.

---

# Files touched

| File                                                  | Change                                                     |
|-------------------------------------------------------|------------------------------------------------------------|
| `aaiclick/orchestration/lifecycle/db_lifecycle.py`    | Add `advisory_id` column to `TableContextRef`              |
| `aaiclick/orchestration/migrations/versions/<new>.py` | Alembic migration: add column + backfill                   |
| `aaiclick/orchestration/orch_context.py`              | `OrchLifecycleHandler.handle_incref` mints/reuses `advisory_id` |
| `aaiclick/backend.py`                                 | Add `is_distributed()` helper                              |
| `aaiclick/locks.py` (new)                             | `table_insert_lock(advisory_id)` async context manager + per-process `advisory_id` cache |
| `aaiclick/data/object/ingest.py`                      | Wrap `insert_objects_db` and `concat_objects_db`           |
| `aaiclick/data/data_context/data_context.py`          | Wrap append path of `create_object_from_value(name=)`      |
| `docs/object.md`                                      | Admonition on `copy()` clarifying it is not serialized     |
| `docs/future.md`                                      | Remove this item from High Priority once shipped           |

---

# Testing

Distributed-mode integration test only — local mode short-circuits the lock
path and has nothing to verify there.

- Two workers concurrently call `insert_objects_db` against the same `p_foo`
  destination. Assert the resulting `aai_id` values, sorted by insert
  timestamp, partition cleanly into two contiguous ranges with no
  interleaving.
- Two workers concurrently call `insert_objects_db` against two different
  destinations (`p_foo`, `p_bar`). Assert wall-clock overlap of the INSERTs
  to confirm no false serialization.
- Crash test: kill one worker mid-INSERT and assert the second worker
  acquires the lock without timeout (PG auto-release on disconnect).

---

# Open questions

- Should the per-process `advisory_id` cache be invalidated on table drop?
  Today, table drops go through the same lifecycle handler, and dropping a
  table also removes its `table_context_refs` rows, so a stale cached id
  would only cause a no-op lock acquisition against a key no other worker
  uses. Probably safe to leave; revisit if it becomes a leak source.
- Pairs naturally with the `table_registry` ClickHouse → SQL move
  (medium-priority item in `docs/future.md`). After that move, the
  `tables` registry can absorb `advisory_id` cleanly. No coupling required
  for this feature to ship.
