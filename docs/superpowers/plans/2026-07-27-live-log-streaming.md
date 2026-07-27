# Live Log Streaming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Task stdout/stderr streams to ClickHouse `task_logs` every 2 s while the task runs, for every task type (module and shell) on every runner, instead of one batch write at exit.

**Architecture:** `_ChLogSink` gains an incremental `drain()`; a shared `_SinkFlusher` writes drained lines with a running `seq` offset every `LOG_FLUSH_INTERVAL` seconds. Module tasks get the flusher inside `capture_task_output`. All shell tasks (host/docker/k8s) reroute through the chdb-holding task child via a new `ShellSpec` (docker/k8s argv wrapped as foreground `docker run --rm` / `kubectl run --attach --rm`), so every CH write happens in a process that legally holds the client; the `log_flush.py` spawn-child machinery is deleted.

**Tech Stack:** Python asyncio, chdb / clickhouse-connect, multiprocessing (spawn), pytest (existing fixtures `orch_ctx`, `orch_ctx_no_ch`).

**Spec:** `docs/designs/live_log_streaming.md` — read it before starting.

## Global Constraints

- ALL imports at top of file, three groups (stdlib / external / current package). No inline imports except the documented circular-dep last resort with a one-line comment.
- No `Any` shortcuts; no `TYPE_CHECKING` pattern.
- NamedTuples over plain tuples in APIs; named attribute access internally.
- No history comments (`# Removed: ...`); no `__all__`.
- Tests follow the `python-testing-style` skill: flat function style, async tests use the `orch_ctx` / `orch_ctx_no_ch` fixtures, no test classes.
- Docs follow the `markdown-style` skill (setext title, `#`/`##` only, name-based implementation references) and land as current-state descriptions, never before/after refactor narrative.
- Commit after every task; push and run the `check-pr` skill at the end.
- Branch: `claude/future-md-items-list-hihd66`.

---

### Task 1: `_ChLogSink.drain()` + `flush_task_logs(seq_offset=)`

**Files:**
- Modify: `aaiclick/orchestration/logging.py`
- Test: `aaiclick/orchestration/test_logging.py`

**Interfaces:**
- Produces: `_ChLogSink.drain() -> list[LogLine]` — returns completed lines accumulated so far and clears them; partial-line buffers untouched. `finalize()` behavior unchanged (partials flushed, then everything returned).
- Produces: `flush_task_logs(task_id: int, job_id: int, run_id: int, lines: list[LogLine], seq_offset: int = 0) -> None` — rows get `seq = seq_offset + index`.

- [ ] **Step 1: Write the failing tests**

Add to `aaiclick/orchestration/test_logging.py`:

```python
def test_sink_drain_returns_completed_lines_and_clears():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "a\nb\n")
    assert [line.text for line in sink.drain()] == ["a", "b"]
    assert sink.drain() == []


def test_sink_drain_holds_back_partial_line():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "complete\npartial")
    assert [line.text for line in sink.drain()] == ["complete"]
    sink.write(STDOUT_STREAM, " tail\n")
    assert [line.text for line in sink.drain()] == ["partial tail"]


def test_sink_finalize_after_drain_flushes_partials():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "done\nhalf")
    sink.drain()
    assert [line.text for line in sink.finalize()] == ["half"]
```

Import `_ChLogSink`, `STDOUT_STREAM` are already imported in this file.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest aaiclick/orchestration/test_logging.py -k drain -v`
Expected: FAIL with `AttributeError: '_ChLogSink' object has no attribute 'drain'`

- [ ] **Step 3: Implement `drain()` and refactor `finalize()`**

In `aaiclick/orchestration/logging.py`, add to `_ChLogSink` (above `finalize`) and make `finalize` reuse it:

```python
    def drain(self) -> list[LogLine]:
        """Return completed lines accumulated so far and clear them.

        Partial-line buffers stay untouched — a half-written line is never
        emitted early."""
        lines, self._lines = self._lines, []
        return lines

    def finalize(self) -> list[LogLine]:
        """Return all captured lines, flushing any unterminated trailing line."""
        for stream in (STDOUT_STREAM, STDERR_STREAM):
            if self._partial[stream]:
                self._lines.append(
                    LogLine(
                        stream=stream,
                        level=_DEFAULT_STREAM_LEVEL[stream],
                        text=self._partial[stream],
                        created_at=utc_now(),
                    )
                )
                self._partial[stream] = ""
        return self.drain()
```

Also update the `_ChLogSink` class docstring: the flush is no longer "once after the task body completes" — it now reads e.g. "Lines are drained incrementally by a periodic flusher and finally on exit (:func:`capture_task_output`)."

- [ ] **Step 4: Add `seq_offset` to `flush_task_logs`**

Change the signature and row build:

```python
async def flush_task_logs(task_id: int, job_id: int, run_id: int, lines: list[LogLine], seq_offset: int = 0) -> None:
    """Best-effort batch insert of captured log lines into CH ``task_logs``.

    ``seq_offset`` is the number of lines already written for this run —
    incremental flushes pass a running offset so ``seq`` stays strictly
    increasing per ``run_id``. A failed write must not fail the task, so
    errors are logged and swallowed — same contract as oplog row writes.
    """
    if not lines:
        return
    rows = [
        [task_id, job_id, run_id, seq_offset + i, line.stream, line.level, line.text, line.created_at]
        for i, line in enumerate(lines)
    ]
```

(rest of the body unchanged)

- [ ] **Step 5: Run the full logging test file**

Run: `pytest aaiclick/orchestration/test_logging.py aaiclick/orchestration/execution/test_execution.py -v`
Expected: PASS (existing finalize tests still green — `finalize()` totals unchanged)

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/logging.py aaiclick/orchestration/test_logging.py
git commit -m "Add incremental drain to _ChLogSink and seq_offset to flush_task_logs"
```

---

### Task 2: `_SinkFlusher` + periodic flushing in `capture_task_output`

**Files:**
- Modify: `aaiclick/orchestration/logging.py`
- Test: `aaiclick/orchestration/execution/test_execution.py`

**Interfaces:**
- Consumes: `_ChLogSink.drain()`, `flush_task_logs(..., seq_offset=)` from Task 1.
- Produces: `LOG_FLUSH_INTERVAL: float = 2.0` (module constant in `aaiclick/orchestration/logging.py`).
- Produces: `class _SinkFlusher` with `__init__(self, sink: _ChLogSink, task_id: int, job_id: int, run_id: int)`, `async run(self) -> None` (loop: sleep `LOG_FLUSH_INTERVAL`, flush pending), `async flush_pending(self) -> None`, `async flush_final(self) -> None`. Task 3 reuses this class for shell streaming.

- [ ] **Step 1: Write the failing test — logs visible mid-run**

Add to `aaiclick/orchestration/execution/test_execution.py` (imports for `LOG_FLUSH_INTERVAL` via monkeypatch of the module attribute):

```python
async def test_capture_task_output_streams_mid_run(orch_ctx, monkeypatch):
    """Completed lines are readable from task_logs while the task body is still running."""
    monkeypatch.setattr("aaiclick.orchestration.logging.LOG_FLUSH_INTERVAL", 0.05)
    task_id, job_id, run_id = 71, 1, 9101
    mid_run_lines: list[str] = []
    async with capture_task_output(task_id, job_id, run_id):
        print("early line")
        await asyncio.sleep(0.3)  # let the flusher tick
        mid_run_lines = [line.text for line in await read_task_logs(task_id, run_id)]
        print("late line")
    assert mid_run_lines == ["early line"]
    final = [line.text for line in await read_task_logs(task_id, run_id)]
    assert final == ["early line", "late line"]
```

Note: reading `task_logs` from inside the capture block is safe — same process, same client. Add `import asyncio` to the test file's stdlib import group if missing.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/execution/test_execution.py::test_capture_task_output_streams_mid_run -v`
Expected: FAIL — `mid_run_lines == []` (no flush happens before the body exits today; also `LOG_FLUSH_INTERVAL` doesn't exist yet, so the monkeypatch raises `AttributeError` first)

- [ ] **Step 3: Implement `_SinkFlusher` and wire into `capture_task_output`**

In `aaiclick/orchestration/logging.py` (add `import asyncio` and `from contextlib import asynccontextmanager, suppress` to the stdlib imports):

```python
LOG_FLUSH_INTERVAL = 2.0
```

```python
class _SinkFlusher:
    """Incrementally write a sink's completed lines to CH ``task_logs``.

    Tracks the running ``seq`` offset so successive flushes keep ``seq``
    strictly increasing per ``run_id``. ``run`` loops until cancelled; the
    owner cancels it and calls ``flush_final`` for the tail. Reads
    ``LOG_FLUSH_INTERVAL`` through the module on every tick so tests can
    monkeypatch it."""

    def __init__(self, sink: _ChLogSink, task_id: int, job_id: int, run_id: int) -> None:
        self._sink = sink
        self._task_id = task_id
        self._job_id = job_id
        self._run_id = run_id
        self._offset = 0

    async def _write(self, lines: list[LogLine]) -> None:
        if not lines:
            return
        await flush_task_logs(self._task_id, self._job_id, self._run_id, lines, seq_offset=self._offset)
        self._offset += len(lines)

    async def flush_pending(self) -> None:
        await self._write(self._sink.drain())

    async def flush_final(self) -> None:
        await self._write(self._sink.finalize())

    async def run(self) -> None:
        while True:
            await asyncio.sleep(LOG_FLUSH_INTERVAL)
            await self.flush_pending()
```

In `capture_task_output`, replace the single-flush structure:

```python
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sink = _ChLogSink()
    flusher = _SinkFlusher(sink, task_id, job_id, run_id)

    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    flusher_task = asyncio.create_task(flusher.run())
    try:
        sys.stdout = _TeeWriter(original_stdout, sink=sink, source=STDOUT_STREAM)
        sys.stderr = _TeeWriter(original_stderr, sink=sink, source=STDERR_STREAM)
        root.handlers = [_ChLogHandler(sink, original_stderr)]
        try:
            root.setLevel(os.getenv("AAICLICK_LOG_LEVEL", "INFO").upper())
        except ValueError:
            root.setLevel(logging.INFO)
        yield
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        flusher_task.cancel()
        with suppress(asyncio.CancelledError):
            await flusher_task
        await flusher.flush_final()
```

Update the `capture_task_output` docstring: the sink is drained to `task_logs` every `LOG_FLUSH_INTERVAL` seconds while the body runs and finally on exit, so long-running tasks are tailed live. Note the sync-body limitation (a body that never awaits starves the flusher; logs land at exit as before).

- [ ] **Step 4: Run tests**

Run: `pytest aaiclick/orchestration/execution/test_execution.py aaiclick/orchestration/test_logging.py -v`
Expected: PASS, including all pre-existing capture tests (final totals unchanged).

- [ ] **Step 5: Verify remote-CH client concurrency assumption**

The flusher shares the task's CH client. For chdb, calls are sync on the loop — serialized by construction. For clickhouse-connect (`AsyncClient`), check the installed package: read `.venv/lib/**/clickhouse_connect/driver/asyncclient.py` (or `pip show -f clickhouse-connect` to locate it). `AsyncClient` methods wrap the sync client in `run_in_executor` — confirm whether the sync `Client` documents thread-safety for concurrent queries (the driver docs state the HTTP client is thread-safe for independent queries; streaming queries are not — we never stream). If it is NOT safe: change `_SinkFlusher._write` to hold an `asyncio.Lock` shared with nothing else (still serializes flushes only) is NOT sufficient — instead have `_SinkFlusher` lazily open its own client via `aaiclick.data.data_context.create_ch_client()`-equivalent for the remote backend only, and document the finding in the class docstring. Record the outcome either way in the docstring (one line, e.g. "clickhouse-connect AsyncClient serializes through its executor and is safe for concurrent non-streaming calls").

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/logging.py aaiclick/orchestration/execution/test_execution.py
git commit -m "Stream module-task logs to task_logs every LOG_FLUSH_INTERVAL"
```

---

### Task 3: Streaming `execute_shell_task` with `ShellSpec`

**Files:**
- Modify: `aaiclick/orchestration/execution/runner.py`
- Test: `aaiclick/orchestration/execution/test_execution.py` (or wherever `execute_shell_task` is currently tested — check `grep -rn execute_shell_task aaiclick --include='test_*.py'` and update those call sites)

**Interfaces:**
- Consumes: `_SinkFlusher`, `LOG_FLUSH_INTERVAL` from Task 2 (import `_SinkFlusher` from `..logging`).
- Produces: `class ShellSpec(NamedTuple)` in `runner.py` with fields `argv: list[str]`, `env: dict[str, str] | None`, `cleanup_argv: list[str] | None = None`. Must stay picklable (crosses the spawn boundary in Task 4).
- Produces: `execute_shell_task(task: Task, spec: ShellSpec | None = None) -> None` — `spec=None` means run `task.command` / `task.command_env` directly (in-process worker, `job_test`).
- Produces: `execute_task(task: Task, shell_spec: ShellSpec | None = None)` — forwards `shell_spec` to `execute_shell_task` for `ENTRY_SHELL`; ignores it otherwise.

- [ ] **Step 1: Write the failing test — shell logs stream mid-run**

```python
async def test_execute_shell_task_streams_mid_run(orch_ctx, monkeypatch):
    """Shell output reaches task_logs while the process is still running."""
    monkeypatch.setattr("aaiclick.orchestration.logging.LOG_FLUSH_INTERVAL", 0.05)
    task = await _persisted_shell_task(["sh", "-c", "echo first; sleep 0.4; echo second"])

    exec_task = asyncio.create_task(execute_shell_task(task))
    await asyncio.sleep(0.25)
    refreshed = await get_task(task.id)
    mid = [line.text for line in await read_task_logs(task.id, refreshed.run_ids[-1])]
    await exec_task

    assert mid == ["first"]
    final = [line.text for line in await read_task_logs(task.id, refreshed.run_ids[-1])]
    assert final == ["first", "second"]
```

Reuse/create the `_persisted_shell_task` helper exactly as in `test_mp_worker_shell.py` (job via `create_job`, task via `create_task(None, entry_type="shell", command=...)`, `commit_tasks`). Place this test in the file that already holds `execute_shell_task` tests; add the helper there if absent.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest <chosen-file>::test_execute_shell_task_streams_mid_run -v`
Expected: FAIL — `mid == []` (today the flush happens after `proc.communicate()` returns)

- [ ] **Step 3: Rewrite `execute_shell_task`**

In `runner.py` — imports: add `ShellSpec` definition, import `_ChLogSink`, `_SinkFlusher` alongside the existing `capture_task_output` import from `..logging`; import `init_oplog_tables` from `aaiclick.oplog.models` and `get_ch_client` from `aaiclick.data.data_context`; `from contextlib import suppress`:

```python
class ShellSpec(NamedTuple):
    """A shell task's fully-resolved launch command.

    Built host-side (``dispatch.build_shell_spec``) so runner-specific
    wrapping (``docker run`` / ``kubectl run``) stays out of the executing
    process. ``cleanup_argv`` is run after the process ends however it ends —
    killing the wrapper CLI alone would leave its container/pod running."""

    argv: list[str]
    env: dict[str, str] | None
    cleanup_argv: list[str] | None = None


async def _run_cleanup_argv(cleanup_argv: list[str]) -> None:
    """Best-effort teardown command (``docker kill`` / ``kubectl delete``).

    Failure is expected on the happy path (the ``--rm`` container is already
    gone) — output and exit code are deliberately ignored."""
    proc = await asyncio.create_subprocess_exec(
        *cleanup_argv,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()


async def _pump_stream(stream: asyncio.StreamReader, sink: _ChLogSink) -> None:
    """Feed the merged stdout pipe into the sink chunk by chunk until EOF."""
    while True:
        chunk = await stream.read(65536)
        if not chunk:
            return
        sink.write(STDOUT_STREAM, chunk.decode(errors="replace"))


async def execute_shell_task(task: Task, spec: ShellSpec | None = None) -> None:
    """Run a shell task's argv, streaming its output to CH ``task_logs``.

    One code path for every shell runner: the in-process worker and
    ``job_test`` call it directly (``spec=None`` → the task's own argv), and
    the mp worker's task child calls it with a dispatch-built ``ShellSpec``
    (host argv, or ``docker run`` / ``kubectl run`` wrapped). Output is
    drained to ``task_logs`` every ``LOG_FLUSH_INTERVAL`` seconds and finally
    at exit, so long-running commands are tailed live.

    Returns None (shell tasks have no result). Raises ``RuntimeError`` on
    nonzero exit (``"exit <code>"``).
    """
    if spec is None:
        spec = ShellSpec(task.command or [], task.command_env)
    run_id = await register_run(task.id)
    # A shell-only job on a fresh DB may not have run task_scope's
    # init_oplog_tables yet; bring the schema up before streaming.
    try:
        await init_oplog_tables(get_ch_client())
    except Exception:
        logger.error("Failed to ensure task_logs for task %s run %s", task.id, run_id, exc_info=True)

    proc = await start_shell_process(spec.argv, spec.env)
    sink = _ChLogSink()
    flusher = _SinkFlusher(sink, task.id, task.job_id, run_id)
    flusher_task = asyncio.create_task(flusher.run())
    reader = asyncio.create_task(_pump_stream(proc.stdout, sink))
    try:
        await proc.wait()
        await reader
    except asyncio.CancelledError:
        proc.kill()
        await proc.wait()
        raise
    finally:
        reader.cancel()
        with suppress(asyncio.CancelledError):
            await reader
        flusher_task.cancel()
        with suppress(asyncio.CancelledError):
            await flusher_task
        await flusher.flush_final()
        if spec.cleanup_argv:
            await _run_cleanup_argv(spec.cleanup_argv)
    if proc.returncode != 0:
        raise RuntimeError(f"exit {proc.returncode}")
```

`STDOUT_STREAM` comes from `aaiclick.log_models` (add to imports if not present). `logger` already exists in `runner.py`; if not, add `logger = logging.getLogger(__name__)`.

- [ ] **Step 4: Thread `shell_spec` through `execute_task`**

```python
async def execute_task(task: Task, shell_spec: ShellSpec | None = None) -> Any:
```

and the shell branch becomes:

```python
    if task.entry_type == ENTRY_SHELL:
        return await execute_shell_task(task, spec=shell_spec)
```

Docstring: mention `shell_spec` is the dispatch-resolved launch command for shell tasks; `None` runs the task's own argv.

- [ ] **Step 5: Run the shell-affected test files**

Run: `pytest aaiclick/orchestration/execution/test_execution.py aaiclick/orchestration/execution/test_local_worker.py -v`
Expected: PASS — `test_local_worker_executes_shell_task` / `test_local_worker_shell_task_nonzero_exit` exercise the new streaming path end to end.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/execution/runner.py aaiclick/orchestration/execution/test_execution.py
git commit -m "Stream shell-task output through ShellSpec-aware execute_shell_task"
```

---

### Task 4: Shell tasks through the mp child; delete `_HostShellVehicle`

**Files:**
- Modify: `aaiclick/orchestration/execution/mp_worker.py`
- Modify: `aaiclick/orchestration/execution/dispatch.py`
- Test: `aaiclick/orchestration/execution/test_mp_worker_shell.py`

**Interfaces:**
- Consumes: `ShellSpec`, `execute_task(task, shell_spec=)` from Task 3; `_cancellation_monitor`, `parse_task_timeout` from `execution_worker.py`.
- Produces: `_run_task_in_child(task: Task, execution_worker_id: int, shell_spec: ShellSpec | None = None)` — Task 5/6 dispatch shell tasks of all runner modes through this.
- Produces: `build_shell_spec(task: Task, dispatch: JobDispatch, execution_worker_id: int) -> ShellSpec` in `dispatch.py` (subprocess mode only for now; Tasks 5–6 extend it).
- Produces: `CHILD_TIMEOUT_GRACE: float = 30.0` in `mp_worker.py` — the parent's backstop margin over the child-enforced timeout.

- [ ] **Step 1: Update the mp shell tests to the new entry point**

Rewrite `test_mp_worker_shell.py`: drop `_run_shell_on_host` / `JobDispatch` / `_shell_task` usage; call `_run_task_in_child(task, 1, shell_spec=ShellSpec(cmd, env))`. IMPORTANT: every task must be **persisted** — `_child_run_task` fetches the task from the DB with `scalar_one()`, so the old unpersisted `Task(id=1, ...)` helper would crash the child. Use `_persisted_shell_task` (already in the file) everywhere. Fixture stays `orch_ctx_no_ch` — the child opens its own chdb.

```python
async def test_shell_in_child_succeeds_on_exit_zero(orch_ctx_no_ch):
    task = await _persisted_shell_task(["true"])
    success, result_ref, error = await _run_task_in_child(task, 1, shell_spec=ShellSpec(["true"], None))
    assert success is True
    assert result_ref is None
    assert error is None


async def test_shell_in_child_fails_on_nonzero(orch_ctx_no_ch):
    task = await _persisted_shell_task(["false"])
    success, _, error = await _run_task_in_child(task, 1, shell_spec=ShellSpec(["false"], None))
    assert success is False
    assert "exit" in (error or "")


async def test_shell_in_child_command_env_overlaid(orch_ctx_no_ch):
    cmd = ["python", "-c", "import os,sys; sys.exit(0 if os.environ.get('K')=='v' else 3)"]
    task = await _persisted_shell_task(cmd, {"K": "v"})
    success, _, _ = await _run_task_in_child(task, 1, shell_spec=ShellSpec(cmd, {"K": "v"}))
    assert success is True


async def test_shell_in_child_streams_logs_to_clickhouse(orch_ctx_no_ch):
    """Shell output is captured to CH task_logs under a registered run_id."""
    cmd = ["sh", "-c", "echo first line; echo second line"]
    task = await _persisted_shell_task(cmd)

    success, _, _ = await _run_task_in_child(task, 1, shell_spec=ShellSpec(cmd, None))
    assert success is True

    refreshed = await get_task(task.id)
    assert refreshed is not None
    assert len(refreshed.run_ids) == 1
    assert read_logs_via_child(task.id, refreshed.run_ids[-1]) == ["first line", "second line"]
```

Imports: replace `_run_shell_on_host` with `_run_task_in_child` (from `.mp_worker`) and add `ShellSpec` (from `.runner`); drop `JobDispatch` and the `Task` import if now unused.

- [ ] **Step 2: Run to verify failure**

Run: `pytest aaiclick/orchestration/execution/test_mp_worker_shell.py -v`
Expected: FAIL — `_run_task_in_child` takes no `shell_spec` yet

- [ ] **Step 3: Extend the child path in `mp_worker.py`**

- `_child_process_target(task_id, job_id, result_queue, shell_spec: ShellSpec | None = None)` — forwards to `_child_run_task`.
- `_child_run_task` grows the shell branch with child-side timeout and cancellation:

```python
async def _child_run_task(
    task_id: int,
    job_id: int,
    result_queue: multiprocessing.Queue,
    shell_spec: ShellSpec | None = None,
) -> None:
    """Set up orch_context, fetch task from DB, execute, send result back.

    Shell tasks enforce their own timeout and poll cancellation here — the
    parent holds no CH client and its kill is only the backstop."""
    from ..orch_context import orch_context

    async with orch_context():
        async with get_sql_session() as session:
            db_result = await session.execute(select(Task).where(Task.id == task_id))
            task = db_result.scalar_one()

        if shell_spec is not None:
            timeout = parse_task_timeout()
            exec_task = asyncio.create_task(
                asyncio.wait_for(execute_task(task, shell_spec=shell_spec), timeout=timeout)
            )
            monitor = asyncio.create_task(_cancellation_monitor(task.id, exec_task, task.run_epoch))
            try:
                await exec_task
                result_queue.put(RunnerResult(success=True, result_ref=None, error=None))
            except asyncio.CancelledError:
                result_queue.put(RunnerResult(success=False, result_ref=None, error="cancelled"))
            except asyncio.TimeoutError:
                result_queue.put(
                    RunnerResult(success=False, result_ref=None, error=f"Task timed out after {timeout}s")
                )
            except Exception as e:
                result_queue.put(RunnerResult(success=False, result_ref=None, error=str(e)))
            finally:
                monitor.cancel()
                with suppress(asyncio.CancelledError):
                    await monitor
            return

        data_result = await execute_task(task)
        result_ref = serialize_task_result(data_result, job_id)
        result_queue.put(RunnerResult(success=True, result_ref=result_ref, error=None))
```

Imports to add at top: `from contextlib import suppress`; `from .execution_worker import _cancellation_monitor` (extend the existing import block); `from .runner import ShellSpec` (extend existing runner import). Note `asyncio.wait_for(..., timeout=None)` is a no-op wrapper — no branch needed.

- `_MpVehicle` gains `def __init__(self, shell_spec: ShellSpec | None = None)`, stores it as `self._shell_spec`, and `launch` passes it: `args=(task.id, task.job_id, result_queue, self._shell_spec)`.
- `_run_task_in_child` grows the parameter and the parent-backstop grace (child-enforced timeout fires first; the parent's kill in `_poll_child` is the last resort):

```python
CHILD_TIMEOUT_GRACE = 30.0


async def _run_task_in_child(
    task: Task,
    execution_worker_id: int,
    shell_spec: ShellSpec | None = None,
) -> tuple[bool, dict | None, str | None]:
    """ExecuteFn for the multiprocessing worker.

    Hands an ``_MpVehicle`` to the shared ``drive_vehicle`` driver. Shell
    tasks enforce timeout and cancellation inside the child; the parent's
    timeout gets ``CHILD_TIMEOUT_GRACE`` slack so it only fires as a backstop
    when the child itself wedges.
    """
    timeout = parse_task_timeout()
    if shell_spec is not None and timeout is not None:
        timeout += CHILD_TIMEOUT_GRACE

    result = await drive_vehicle(
        task,
        execution_worker_id,
        _MpVehicle(shell_spec),
        timeout=timeout,
        poll_interval=POLL_INTERVAL,
        heartbeat_fn=execution_worker_heartbeat,
    )
    return result.success, result.result_ref, result.error
```
- Delete `_HostShellHandle`, `_HostShellVehicle`, `_run_shell_on_host`, and the now-unused imports (`flush_shell_logs`, `start_shell_process`, `register_run`, `check_task_cancelled`, `JobDispatch` — keep only what's still used).

- [ ] **Step 4: Route dispatch through the child**

In `dispatch.py`:

```python
def build_shell_spec(task: Task, dispatch: JobDispatch, execution_worker_id: int) -> ShellSpec:
    """Resolve a shell task's launch command for its runner mode.

    Subprocess mode runs the argv directly on the host."""
    return ShellSpec(dispatch.command or [], dispatch.command_env)


async def dispatch_execute(task: Task, execution_worker_id: int) -> ExecuteResult:
    """ExecuteFn that picks the runner per task."""
    dispatch = await _resolve_dispatch(task)
    if dispatch.entry_type == ENTRY_SHELL:
        spec = build_shell_spec(task, dispatch, execution_worker_id)
        return await _run_task_in_child(task, execution_worker_id, shell_spec=spec)
    handler = _IMAGE_RUNNERS.get(dispatch.runner_mode)
    if handler is not None:
        return await handler(task, execution_worker_id, dispatch)
    return await _run_task_in_child(task, execution_worker_id)
```

(`build_shell_spec` stays sync in this task; Task 5 makes it async when image resolution enters. Import `ShellSpec` from `.runner`; drop the `_run_shell_on_host` import.)

NOTE: after this task, docker/k8s **shell** tasks run their *raw argv on the worker host* until Tasks 5–6 land the wrapping. `test_dispatch.py` may assert the old routing — update its shell-routing expectations to `_run_task_in_child`. Tasks 4–6 must land in the same PR.

- [ ] **Step 5: Run tests**

Run: `pytest aaiclick/orchestration/execution/test_mp_worker_shell.py aaiclick/orchestration/execution/test_dispatch.py aaiclick/orchestration/execution/test_mp_worker.py aaiclick/orchestration/execution/test_worker_mp.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/execution/mp_worker.py aaiclick/orchestration/execution/dispatch.py aaiclick/orchestration/execution/test_mp_worker_shell.py aaiclick/orchestration/execution/test_dispatch.py
git commit -m "Run host shell tasks inside the mp task child with live streaming"
```

---

### Task 5: Docker shell via foreground `docker run --rm`

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_worker.py`
- Modify: `aaiclick/orchestration/execution/dispatch.py`
- Test: `aaiclick/orchestration/execution/test_docker_worker.py`

**Interfaces:**
- Consumes: `ShellSpec` (Task 3), `build_shell_spec` (Task 4 — becomes `async`).
- Produces: `build_shell_run_spec(task: Task, image_tag: str) -> ShellSpec` in `docker_worker.py`.

- [ ] **Step 1: Write the failing builder test**

Add to `test_docker_worker.py`:

```python
def test_build_shell_run_spec_wraps_argv():
    task = Task(
        id=7, job_id=1, name="t", entrypoint="", entry_type="shell",
        command=["echo", "hi"], command_env={"K": "v"}, run_epoch=2,
    )
    spec = build_shell_run_spec(task, "img:tag")
    assert spec.argv[:2] == ["docker", "run"]
    assert "--rm" in spec.argv
    assert "--name" in spec.argv and "aaiclick-task-7-2" in spec.argv
    assert ["-e", "K=v"] == spec.argv[spec.argv.index("-e") : spec.argv.index("-e") + 2]
    assert spec.argv[-3:] == ["img:tag", "echo", "hi"]
    assert spec.env is None
    assert spec.cleanup_argv == ["docker", "kill", "aaiclick-task-7-2"]
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest aaiclick/orchestration/execution/test_docker_worker.py::test_build_shell_run_spec_wraps_argv -v`
Expected: FAIL — `build_shell_run_spec` not defined

- [ ] **Step 3: Implement the builder; strip the shell branch from the vehicle**

In `docker_worker.py`:

```python
def _shell_container_name(task: Task) -> str:
    """Unique-per-attempt container name so cleanup can address it."""
    return f"aaiclick-task-{task.id}-{task.run_epoch}"


def build_shell_run_spec(task: Task, image_tag: str) -> ShellSpec:
    """Wrap a shell task's argv as a foreground ``docker run``.

    ``--rm`` is safe here (unlike module tasks' detached run): the docker CLI
    is the wrapper process, so its own exit code *is* the container's — no
    ``docker wait`` race. ``cleanup_argv`` kills the container by name for
    the timeout/cancel path, where killing the CLI alone would leave it
    running."""
    name = _shell_container_name(task)
    argv = [
        _docker_bin(),
        "run",
        "--rm",
        "--name",
        name,
        *add_host_flags("AAICLICK_DOCKER_RUN_ADD_HOST"),
    ]
    for key, value in (task.command_env or {}).items():
        argv.extend(["-e", f"{key}={value}"])
    argv.append(image_tag)
    argv.extend(task.command or [])
    return ShellSpec(argv, None, cleanup_argv=[_docker_bin(), "kill", name])
```

(import `ShellSpec` from `.runner`.) Then remove the shell path from the module-task machinery:

- `_build_docker_run_cmd`: drop the `ENTRY_SHELL` branch and the `entry_type` mention in its docstring.
- `_DockerVehicle`: drop the `entry_type` ctor param and the shell branches in `launch` (no `register_run`), `wait` (no logs flush), `collect`; `_DockerHandle` loses `run_id` (and `task_id`/`job_id` if now unused).
- Delete `_container_logs_text`; drop the `flush_shell_logs` and `register_run` imports.
- `_run_task_in_container`: drop the `entry_type` conditional on `env` (always `build_runner_env()`), and the `_DockerVehicle(...)` call loses `task.entry_type`.

- [ ] **Step 4: Extend `build_shell_spec` in `dispatch.py`**

Make it async and mode-aware (docker only in this task; k8s raises until Task 6):

```python
async def build_shell_spec(task: Task, dispatch: JobDispatch, execution_worker_id: int) -> ShellSpec:
    """Resolve a shell task's launch command for its runner mode.

    Subprocess mode runs the argv directly; container modes wrap it as a
    foreground ``docker run`` / ``kubectl run`` so the wrapper's exit code
    and merged stdout are the task's."""
    if dispatch.runner_mode == RUNNER_DOCKER:
        image_tag = await resolve_image_tag(task, dispatch.image_source, dispatch.image_tag, execution_worker_id)
        await _docker_pull_if_registered(image_tag)
        return build_shell_run_spec(task, image_tag)
    if dispatch.runner_mode == RUNNER_KUBERNETES:
        image_tag = await resolve_image_tag(task, dispatch.image_source, dispatch.image_tag, execution_worker_id)
        return build_shell_pod_spec(task, dispatch, image_tag)
    return ShellSpec(dispatch.command or [], dispatch.command_env)
```

Imports: `resolve_image_tag` from `.image_builder`; `_docker_pull_if_registered`, `build_shell_run_spec` from `.docker_worker`; `build_shell_pod_spec` from `.kubernetes_worker` (created in Task 6 — if executing Task 5 standalone, stub the k8s branch with `raise NotImplementedError` and no import, then replace in Task 6). Update the `dispatch_execute` call site to `await build_shell_spec(...)`.

- [ ] **Step 5: Fix docker tests that asserted the shell branch**

In `test_docker_worker.py`: shell-vehicle tests (search for `ENTRY_SHELL` / `entry_type="shell"` / `flush_shell_logs` usage, e.g. via `log_test_helpers.flush_recorder`) are superseded — the vehicle no longer handles shell. Delete those vehicle-level shell tests; the builder test plus Task 4's child tests carry the coverage. Update `_DockerVehicle(...)` constructions for the removed `entry_type` arg.

- [ ] **Step 6: Run tests**

Run: `pytest aaiclick/orchestration/execution/test_docker_worker.py aaiclick/orchestration/execution/test_dispatch.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add aaiclick/orchestration/execution/docker_worker.py aaiclick/orchestration/execution/dispatch.py aaiclick/orchestration/execution/test_docker_worker.py aaiclick/orchestration/execution/test_dispatch.py
git commit -m "Wrap docker shell tasks as foreground docker run --rm"
```

---

### Task 6: Kubernetes shell via `kubectl run --attach --rm`

**Files:**
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py`
- Modify: `aaiclick/orchestration/execution/dispatch.py` (only if the Task 5 stub was used)
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py`

**Interfaces:**
- Consumes: `ShellSpec`, `_pod_spec_from`, `_build_pod_manifest` (existing).
- Produces: `build_shell_pod_spec(task: Task, dispatch: JobDispatch, image_tag: str) -> ShellSpec` in `kubernetes_worker.py`.

- [ ] **Step 1: Write the failing builder test**

```python
def test_build_shell_pod_spec_wraps_argv():
    task = Task(
        id=9, job_id=1, name="t", entrypoint="", entry_type="shell",
        command=["echo", "hi"], command_env={"K": "v"}, run_epoch=1,
    )
    dispatch = JobDispatch(
        "kubernetes", "img:tag", {"namespace": "jobs", "service_account": "sa",
                                  "image_pull_secret": None, "resources": None},
        "shell", ["echo", "hi"], {"K": "v"},
    )
    spec = build_shell_pod_spec(task, dispatch, "img:tag")
    assert spec.argv[:3] == ["kubectl", "run", "aaiclick-task-9-1"]
    assert {"--attach", "--rm", "--quiet", "--restart=Never"} <= set(spec.argv)
    assert "--image=img:tag" in spec.argv
    overrides = json.loads(next(a for a in spec.argv if a.startswith("--overrides=")).removeprefix("--overrides="))
    assert overrides["spec"]["containers"][0]["command"] == ["echo", "hi"]
    assert overrides["spec"]["containers"][0]["env"] == [{"name": "K", "value": "v"}]
    assert overrides["spec"]["serviceAccountName"] == "sa"
    assert ["-n", "jobs"] == spec.argv[spec.argv.index("-n") : spec.argv.index("-n") + 2]
    assert spec.cleanup_argv == ["kubectl", "delete", "pod", "aaiclick-task-9-1", "-n", "jobs", "--ignore-not-found"]
```

(add `import json` to the test file's stdlib imports if missing)

- [ ] **Step 2: Run to verify failure**

Run: `pytest aaiclick/orchestration/execution/test_kubernetes_worker.py::test_build_shell_pod_spec_wraps_argv -v`
Expected: FAIL — `build_shell_pod_spec` not defined

- [ ] **Step 3: Implement the builder; strip the vehicle's shell branch**

```python
def build_shell_pod_spec(task: Task, dispatch: JobDispatch, image_tag: str) -> ShellSpec:
    """Wrap a shell task's argv as a foreground ``kubectl run --attach --rm``.

    The full container spec (command, env, resources, serviceAccount,
    imagePullSecrets) rides in ``--overrides`` built from the same manifest
    as before, so shell pods keep their cluster config; ``--attach``
    propagates the container's exit code and streams its output on the
    kubectl process's stdout. ``--quiet`` keeps kubectl's own chatter out of
    the captured log."""
    pod = _pod_spec_from(task, dispatch._replace(image_tag=image_tag))
    name = _pod_name(task.id, task.run_epoch)
    manifest = _build_pod_manifest(
        name=name,
        namespace=pod.namespace,
        image_tag=image_tag,
        task_id=task.id,
        run_epoch=task.run_epoch,
        env={},
        service_account=pod.service_account,
        image_pull_secret=pod.image_pull_secret,
        resources=pod.resources,
        entry_type=ENTRY_SHELL,
        command=pod.command,
        command_env=pod.command_env,
    )
    overrides = {"apiVersion": "v1", "spec": manifest["spec"]}
    argv = [
        _kubectl_bin(),
        "run",
        name,
        "-n",
        pod.namespace,
        "--attach",
        "--rm",
        "--quiet",
        "--restart=Never",
        f"--image={image_tag}",
        f"--overrides={json.dumps(overrides)}",
    ]
    return ShellSpec(
        argv,
        None,
        cleanup_argv=[_kubectl_bin(), "delete", "pod", name, "-n", pod.namespace, "--ignore-not-found"],
    )
```

(import `ShellSpec` from `.runner`; `json` is already imported.) Then strip shell handling from the module machinery:

- `_KubernetesVehicle.launch`: drop the `register_run` shell branch; `_PodHandle` loses `run_id`.
- `_KubernetesVehicle.wait`: drop the shell logs-flush branch; `collect`: drop the `ENTRY_SHELL` branch.
- `_build_pod_manifest` **keeps** its shell branch — `build_shell_pod_spec` reuses it.
- Delete `_pod_logs_text`; drop the `flush_shell_logs` and `register_run` imports.
- If Task 5 stubbed the k8s branch in `dispatch.build_shell_spec`, wire the real import/call now.

- [ ] **Step 4: Fix k8s tests that asserted the vehicle shell branch**

Same treatment as docker: delete superseded vehicle-level shell tests; keep manifest tests for `_build_pod_manifest`'s shell branch (still live via the builder).

- [ ] **Step 5: Run tests**

Run: `pytest aaiclick/orchestration/execution/test_kubernetes_worker.py aaiclick/orchestration/execution/test_dispatch.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/execution/kubernetes_worker.py aaiclick/orchestration/execution/dispatch.py aaiclick/orchestration/execution/test_kubernetes_worker.py
git commit -m "Wrap kubernetes shell tasks as foreground kubectl run --attach"
```

---

### Task 7: Delete the spawn-flush machinery

**Files:**
- Delete: `aaiclick/orchestration/execution/log_flush.py`, `aaiclick/orchestration/execution/test_log_flush_mp.py`
- Modify: `aaiclick/orchestration/logging.py` (remove `shell_text_to_lines`), `aaiclick/orchestration/test_logging.py`, `aaiclick/orchestration/execution/log_test_helpers.py`

**Interfaces:**
- Consumes: nothing new. Precondition: Tasks 3–6 removed every import of `flush_shell_logs` / `flush_shell_logs_inline`.

- [ ] **Step 1: Verify nothing references the dead code**

Run: `grep -rn "flush_shell_logs\|shell_text_to_lines\|log_flush" aaiclick src docs --include='*.py' --include='*.md'`
Expected: only `log_flush.py` itself, its test, `shell_text_to_lines` def + tests, and doc mentions (docs handled in Task 8). If any code hit remains, fix it first.

- [ ] **Step 2: Delete**

```bash
git rm aaiclick/orchestration/execution/log_flush.py aaiclick/orchestration/execution/test_log_flush_mp.py
```

Remove `shell_text_to_lines` from `logging.py` and its four tests from `test_logging.py` (streaming made it dead — output now enters through `_ChLogSink.write`). In `log_test_helpers.py`, remove `flush_recorder` if Tasks 5–6 left it unreferenced (`grep -rn flush_recorder aaiclick`); keep `read_logs_via_child`. Update the `logging.py` module docstring (no more "flushed host-side via execution.log_flush").

- [ ] **Step 3: Run the full orchestration suite**

Run: `pytest aaiclick/orchestration -x -q`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "Delete spawn-child shell log flush machinery"
```

---

### Task 8: Docs, future.md, full suite, push, check-pr

**Files:**
- Modify: `docs/designs/orchestration.md`, `docs/designs/task_log_retirement.md`, `docs/user_guide/orchestration.md`, `docs/designs/future.md`
- Delete: `docs/designs/live_log_streaming.md`, `docs/superpowers/plans/2026-07-27-live-log-streaming.md`

**Interfaces:** none — documentation only.

- [ ] **Step 1: Rewrite docs as current state**

All edits describe what IS — no before/after narrative, no status icons (per `markdown-style` + CLAUDE.md):

- `docs/designs/orchestration.md`: find the shell-task logging paragraph (grep "in-process paths" / "flush child") and the capture description; rewrite to: all task output streams to CH `task_logs` incrementally (`LOG_FLUSH_INTERVAL`); shell tasks of every runner mode execute via `execute_shell_task` with a dispatch-built `ShellSpec` (docker/k8s wrapped as foreground `docker run --rm` / `kubectl run --attach --rm`) inside the worker's task child or the in-process worker. Implementation references by name: `aaiclick/orchestration/logging.py` — see `_SinkFlusher`, `capture_task_output`; `aaiclick/orchestration/execution/runner.py` — see `execute_shell_task`, `ShellSpec`; `aaiclick/orchestration/execution/dispatch.py` — see `build_shell_spec`.
- `docs/designs/task_log_retirement.md`: update the per-runner capture table to the streamed reality (all runners: "streamed → CH incrementally") and drop mentions of `flush_shell_logs` / the spawn flush child (reference `execute_shell_task` instead).
- `docs/user_guide/orchestration.md`: logs section states logs are tailed live (~2 s lag) while a task runs.
- `docs/designs/future.md`: delete the "Live Log Streaming" section; in the SSE section keep the `task.log` event mention (SSE remains deferred).

- [ ] **Step 2: Delete the spec and this plan**

```bash
git rm docs/designs/live_log_streaming.md docs/superpowers/plans/2026-07-27-live-log-streaming.md
```

Then re-grep for dangling references: `grep -rn "live_log_streaming" docs aaiclick` — fix any.

- [ ] **Step 3: Run the full test suite**

Run: `pytest aaiclick -q`
Expected: PASS (fix anything red before continuing)

- [ ] **Step 4: Commit and push**

```bash
git add -A
git commit -m "Stream task logs live; docs describe streaming as current state"
git push -u origin claude/future-md-items-list-hihd66
```

- [ ] **Step 5: Verify CI**

Use the `check-pr` skill (per CLAUDE.md) to watch GitHub Actions to completion; fix failures and re-push until green.
