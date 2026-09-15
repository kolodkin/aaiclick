Change Signals — Consumers Beyond the UI
---

Make `cli_wait.wait_for_job` react to change signals instead of polling on a
fixed interval, and consolidate the wait primitive it would otherwise
duplicate. Implements the item of this name in `docs/designs/future.md`.

---

# Background

`aaiclick/orchestration/events` turns "a `jobs`, `tasks` or `groups` row
committed" into a payload-less signal. Session hooks flag the transaction, a
per-backend transport carries the signal, and an `EventBus` fans it out to
depth-1 subscriber mailboxes.

The SSE endpoint (`aaiclick/server/events.py`) is the only subscriber today.
`cli_wait.wait_for_job` still polls `internal_api.job_stats` every second until
the job is terminal, which costs a query per second per waiter and reacts up to
a second late.

# Constraints

**Local mode cannot deliver signals to the CLI.** chdb's file lock means one
process, so jobs execute only inside the `local start` server process — see
`start_local()` in `aaiclick/orchestration/cli.py`. The CLI's `job wait` opens a
SQLite-only orch context in a separate process. `LocalTransport.after_commit`
publishes to that process's own bus, which nothing ever writes to, so a naive
subscribe blocks forever.

**`state` answers the wrong question.** `LocalTransport.state` returns
`STATE_LISTENING` unconditionally — a direct call can neither connect nor drop.
Choosing the poll interval from `state` alone would give local mode the slow
interval and make `job wait` an order of magnitude *less* responsive than it is
today.

# Design

## The `cross_process` capability

Add one property to the `SignalTransport` protocol in
`aaiclick/orchestration/events/transport.py`:

```python
@property
def cross_process(self) -> bool:
    """Whether ``feed`` can deliver signals committed by other processes."""
```

| Transport           | Value   | Why                                                    |
|---------------------|---------|--------------------------------------------------------|
| `LocalTransport`    | `False` | chdb's file lock confines writers to this process      |
| `PostgresTransport` | `True`  | Postgres brokers `NOTIFY` to every `LISTEN` connection |

This keeps backend knowledge inside the events package rather than leaking
`is_local()` into `cli_wait`, and a future SSE-backed local transport flips the
value without touching any waiter.

## Event-driven `wait_for_job`

`wait_for_job` keeps its signature, contract and timeout semantics. It gains
one keyword argument and one branch.

When `transport.cross_process` is `False`, the function behaves exactly as it
does today: no bus, no feed task, `poll_interval` unchanged.

When `True`, it scopes a bus and transport, runs `transport.feed(bus,
stop=stop)` as a task, and holds one subscription across the loop. The wait at
the bottom of the loop becomes a bounded wait on that subscription instead of
`asyncio.sleep`.

**Ordering is the correctness argument.** The subscription opens *before* the
first `job_stats` read and stays open for the life of the loop. The mailbox is
depth-1, so a commit landing during a fetch is queued and the next wait returns
immediately. Subscribing after the read instead would drop that signal and
stall until the safety poll.

**The wake reason is deliberately ignored.** A signal, a timeout and a closed
bus all mean the same thing — refetch now. Correctness comes from the refetch,
not from why the wait returned, so no tri-state helper is needed.

**Two intervals, re-chosen each iteration.** `signal_poll_interval` (default
`10.0`) applies while `transport.state` is `STATE_LISTENING`; the existing
`poll_interval` (default `1.0`) applies while idle or reconnecting, which also
covers the window before the first `LISTEN` connection lands. Two explicit
keyword arguments rather than a derived value, so `poll_interval=0` in existing
tests keeps meaning what it means.

The slow poll is a safety net, not a formality: a hung CLI is a worse failure
than an extra query every ten seconds.

## Data flow

Distributed mode, once a waiter is running:

1. A worker commits a `tasks` row.
2. The session hook flags the transaction; `PostgresTransport.before_commit`
   issues `pg_notify` inside it, so the signal is delivered only if the write
   commits.
3. The CLI's `LISTEN` connection receives the notification and calls
   `bus.publish()`.
4. The depth-1 mailbox wakes the pending wait.
5. The loop refetches `job_stats` and invokes `on_change` if the per-status
   counts moved.

Local mode skips steps 2-4 entirely; the safety poll drives step 5.

## Error handling

No new error paths. The feed supervisor already retries with capped backoff and
republishes on recovery so subscribers resync — see `PostgresTransport._connected`
in `aaiclick/orchestration/events/postgres.py`. A dropped link degrades to the
1 s poll because `state` becomes `STATE_RECONNECTING`, then catches up.

The feed task is cancelled and awaited in a `finally`, so both the terminal
return and a `JobWaitTimeout` release the Postgres connection.

# Consolidation

## `wait_or_timeout`

Six sites ask "was the event set, or did the timeout elapse?". One is a named
helper; four are hand-rolled `try/except asyncio.TimeoutError`.

| Site                                                         | Form today               |
|--------------------------------------------------------------|--------------------------|
| `events/postgres.py` — `_wait_or_timeout`                    | named helper, private    |
| `events/postgres.py` — `_keep_alive`, `_back_off`            | use the helper           |
| `background/background_worker.py` — `_cleanup_loop`          | inline, result discarded |
| `orchestration/logging.py` — `run`                           | inline, returns on set   |
| `execution/execution_worker.py` — `_heartbeat_while_waiting` | inline, returns on set   |
| `execution/execution_worker.py` — `_watch_for_cancellation`  | inline, returns on set   |

Promote the helper to a new neutral module `aaiclick/async_wait.py`:

```python
async def wait_or_timeout(event: asyncio.Event, timeout: float) -> bool:
    """True once ``event`` is set, False when ``timeout`` elapses first."""
```

Top-level because consumers span `orchestration/background/`,
`orchestration/logging.py`, `orchestration/execution/`, `orchestration/events/`
and `cli_wait.py`. A neutral module both sides import is the shape CLAUDE.md
prefers over any import-cycle workaround.

Convert the four inline sites, each losing a `try/except`. The two in
`execution_worker.py` take the `while not await wait_or_timeout(...)` form that
`_keep_alive` already uses.

`wait_for_job` does **not** use this primitive — it waits on a `Subscription`,
not an `asyncio.Event`.

## One shared e2e job waiter

`test_e2e/docker/test_runner_e2e.py` and
`test_e2e/kubernetes/test_runner_e2e.py` each define a `_wait_for_job` differing
only in a docstring line. Collapse them into one helper checking
`TERMINAL_JOB_STATUSES`.

!!! warning "A cancelled job hangs both e2e waiters today"
    Both check `job.status in (JOB_COMPLETED, JOB_FAILED)`, but
    `TERMINAL_JOB_STATUSES` in `aaiclick/orchestration/models.py` also carries
    `JOB_CANCELLED`. A cancelled job spins the full 600 s timeout, then fails
    with a misleading "did not complete" dump.

There is no `__init__.py` under `test_e2e/`, so pytest puts each test
directory on `sys.path` individually. Adding `test_e2e/conftest.py` puts
`test_e2e/` on the path and makes the shared helper importable from both
suites.

The helper stays a plain 1 s ORM poll. It queries the ORM directly rather than
through `internal_api`, and an e2e test with a 600 s budget gains nothing from
sub-second reaction — fewer moving parts is worth more there.

# Testing

| Case                                   | Asserts                                                                |
|----------------------------------------|------------------------------------------------------------------------|
| Fake transport, `cross_process=True`   | A published signal advances the loop without waiting the slow interval |
| Signal published during a stats fetch  | Subscribe-before-fetch ordering — the signal is not lost               |
| `cross_process=False`                  | No feed task starts; interval selection is unchanged                   |
| Transport reports `STATE_RECONNECTING` | Falls back to the fast `poll_interval`                                 |
| Feed task lifetime                     | Cancelled and awaited on both terminal return and `JobWaitTimeout`     |
| `wait_or_timeout`                      | `True` on set, `False` on timeout                                      |

The existing `aaiclick/test_cli_wait.py` must pass untouched — it drives the
loop with `poll_interval=0` under a local backend, which stays
`cross_process=False`. That is the regression bar for this change.

# Out of scope

**MCP and SDK waiters.** `wait_for_job` already *is* the reusable
subscribe-then-refetch loop any in-process caller can await; no MCP wait tool
exists today, and adding one needs its own decisions about bounded timeouts and
timeout return values.

**A local-mode cross-process transport.** An SSE client in the CLI, or any
other bridge that would let local mode react to signals, is a larger change
that `cross_process` leaves room for.

# Documentation

Update the `docs/designs/future.md` item: remove the `cli_wait.wait_for_job`
bullet, and restate the MCP / SDK bullet as reusing `wait_for_job` rather than
as pending infrastructure. Record the local-mode gap as the remaining work,
since `cross_process` now names it explicitly.
