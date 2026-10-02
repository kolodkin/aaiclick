"""Task logging utilities for orchestration backend.

Task stdout/stderr is captured to ClickHouse ``task_logs`` and/or the console
(:func:`task_logs_destination`), streamed to ClickHouse every
``LOG_FLUSH_INTERVAL`` seconds through :func:`stream_to_task_logs`. Module tasks feed it from inside the task process
(:func:`capture_task_output`); shell tasks and ``jvm`` containers are fed by the
host (``execution.runner``'s ``execute_shell_task`` / ``follow_vehicle_output``).
All runs surface their logs through one cross-host read path
(:func:`read_task_logs`) no matter which host wrote them.
"""

import asyncio
import logging
import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from typing import Literal, cast, get_args

from aaiclick.async_wait import wait_or_timeout
from aaiclick.backend import is_chdb, is_local
from aaiclick.data.data_context import ChClient, get_ch_client
from aaiclick.data.data_context.ch_client import create_ch_client
from aaiclick.datetime_utils import utc_now
from aaiclick.log_models import (
    MAX_TASK_LOG_LINES,
    STDERR_STREAM,
    STDOUT_STREAM,
    LogLevel,
    LogLine,
    LogStream,
    normalize_level,
)
from aaiclick.oplog.models import get_column_types, init_oplog_tables

logger = logging.getLogger(__name__)

_TASK_LOG_COLS = ["task_id", "job_id", "run_id", "seq", "stream", "level", "line", "created_at"]


TASK_LOGS_BOTH = "both"
TASK_LOGS_CLICKHOUSE = "clickhouse"
TASK_LOGS_CONSOLE = "console"
TaskLogsDestination = Literal["both", "clickhouse", "console"]


def task_logs_destination() -> TaskLogsDestination:
    """Where captured task output goes, from ``AAICLICK_TASK_LOGS``:
    ``"both"`` (the default), ``"clickhouse"`` (``task_logs`` only — what the
    UI log panel reads) or ``"console"`` (the process's stdout/stderr only).
    aaiclick's own framework logs always go to the console."""
    value = os.environ.get("AAICLICK_TASK_LOGS") or TASK_LOGS_BOTH
    allowed = get_args(TaskLogsDestination)
    if value not in allowed:
        raise ValueError(f"AAICLICK_TASK_LOGS must be one of {list(allowed)}, got {value!r}")
    return cast(TaskLogsDestination, value)


# How often a running task's captured output is drained to CH task_logs.
# Matches the UI poll interval — flushing faster buys nothing.
LOG_FLUSH_INTERVAL = 2.0


# stderr defaults to WARNING, not ERROR: tools routinely write progress and
# diagnostics to stderr, and provenance is already recorded in ``stream``.
# True ERROR is reserved for ``logging.error`` records, which keep their level.
_DEFAULT_STREAM_LEVEL: dict[LogStream, LogLevel] = {STDOUT_STREAM: "INFO", STDERR_STREAM: "WARNING"}


class ChLogSink:
    """Route captured output, line by line, to its destinations: buffered for a
    CH batch write (``keep``) and/or printed to the console (``console``).

    ``write`` is sync — it's driven by ``print`` through ``_SinkWriter`` while
    the task runs. Each stream (stdout / stderr) keeps its own partial-line
    buffer so a line is tagged with the stream that emitted it; completed lines
    are kept in emission order, each stamped with its own emit time. ``record``
    is the logging path: already-leveled lines from ``_ChLogHandler``. Console
    lines go to the stdout / stderr in place when the sink is built, each
    prefixed with ``prefix``. Kept lines are drained incrementally by a
    periodic flusher while the task runs and finally on exit.
    """

    def __init__(self, *, keep: bool = True, console: bool = False, prefix: str = "") -> None:
        self._partial: dict[LogStream, str] = {STDOUT_STREAM: "", STDERR_STREAM: ""}
        self._lines: list[LogLine] = []
        self._keep = keep
        self._console = {STDOUT_STREAM: sys.stdout, STDERR_STREAM: sys.stderr} if console else None
        self._prefix = prefix

    def _emit(self, stream: LogStream, level: LogLevel, text: str, created_at: datetime) -> None:
        if self._console is not None:
            out = self._console[stream]
            out.write(f"{self._prefix}{text}\n")
            out.flush()
        if self._keep:
            self._lines.append(LogLine(stream=stream, level=level, text=text, created_at=created_at))

    def write(self, stream: LogStream, data: str) -> None:
        parts = (self._partial[stream] + data).split("\n")
        self._partial[stream] = parts.pop()
        for part in parts:
            self._emit(stream, _DEFAULT_STREAM_LEVEL[stream], part, utc_now())

    def record(self, level: LogLevel, text: str) -> None:
        """Emit a logging record's message as level-tagged line(s), all
        stamped with the record's one emit time."""
        now = utc_now()
        for part in text.rstrip("\n").split("\n"):
            self._emit(STDERR_STREAM, level, part, now)

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
                self._emit(stream, _DEFAULT_STREAM_LEVEL[stream], self._partial[stream], utc_now())
                self._partial[stream] = ""
        return self.drain()


class _SinkFlusher:
    """Incrementally write a sink's completed lines to CH ``task_logs``.

    Tracks the running ``seq`` offset so successive flushes keep ``seq``
    strictly increasing per ``run_id``. ``run`` loops until ``request_stop``;
    the owner signals stop, awaits ``run`` so an in-flight flush completes
    (cancelling mid-write would lose the drained-but-unwritten batch), then
    calls ``flush_final`` for the tail. Reads ``LOG_FLUSH_INTERVAL`` through
    the module on every tick so tests can monkeypatch it.

    Client choice per backend: chdb calls are sync on the event loop, so the
    flusher shares the task's client — the two can never interleave. A remote
    clickhouse-connect ``AsyncClient`` autogenerates a server ``session_id``
    that rejects concurrent queries, so there the flusher lazily opens its
    own client (closed by ``flush_final``) instead of racing the task body's.
    """

    def __init__(self, sink: ChLogSink, task_id: int, job_id: int, run_id: int) -> None:
        self._sink = sink
        self._task_id = task_id
        self._job_id = job_id
        self._run_id = run_id
        self._offset = 0
        self._own_client: ChClient | None = None
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        """Signal ``run`` to exit at its next check; awaiting ``run`` after
        this drains gracefully instead of dropping an in-flight flush."""
        self._stop.set()

    async def _client(self) -> ChClient:
        if is_chdb():
            return get_ch_client()
        if self._own_client is None:
            self._own_client = await create_ch_client()
        return self._own_client

    async def _write(self, lines: list[LogLine]) -> None:
        if not lines:
            return
        try:
            ch_client = await self._client()
        except Exception:  # same best-effort contract as flush_task_logs
            logger.error(
                "Failed to open CH client for task %s run %s log flush", self._task_id, self._run_id, exc_info=True
            )
            return
        await flush_task_logs(
            self._task_id, self._job_id, self._run_id, lines, seq_offset=self._offset, ch_client=ch_client
        )
        self._offset += len(lines)

    async def flush_pending(self) -> None:
        await self._write(self._sink.drain())

    async def flush_final(self) -> None:
        try:
            await self._write(self._sink.finalize())
        finally:
            if self._own_client is not None:
                with suppress(Exception):
                    await self._own_client.close()
                self._own_client = None

    async def run(self) -> None:
        while not await wait_or_timeout(self._stop, LOG_FLUSH_INTERVAL):
            await self.flush_pending()


class _SinkWriter:
    """File-like stand-in for ``sys.stdout`` / ``sys.stderr`` that feeds a sink.

    ``source`` tags the sink rows with the stream this writer fronts so the
    captured lines carry their origin."""

    def __init__(self, sink: ChLogSink, source: LogStream):
        self._sink = sink
        self._source = source

    def write(self, data: str) -> int:
        self._sink.write(self._source, data)
        return len(data)

    def flush(self) -> None:
        pass


class _ChLogHandler(logging.Handler):
    """Route ``logging`` records into the active sink with their true level,
    bypassing ``_SinkWriter`` so a record is not captured a second time as raw
    stderr text."""

    def __init__(self, sink: ChLogSink):
        super().__init__()
        self._sink = sink
        self.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._sink.record(normalize_level(record.levelno), self.format(record))
        except Exception:  # never let logging crash the task
            self.handleError(record)


async def flush_task_logs(
    task_id: int,
    job_id: int,
    run_id: int,
    lines: list[LogLine],
    seq_offset: int = 0,
    ch_client: ChClient | None = None,
) -> None:
    """Best-effort batch insert of captured log lines into CH ``task_logs``.

    ``seq_offset`` is the number of lines already written for this run —
    incremental flushes pass a running offset so ``seq`` stays strictly
    increasing per ``run_id``. ``ch_client`` overrides the context client
    (:class:`_SinkFlusher` passes its own in remote mode). A failed write must
    not fail the task, so errors are logged and swallowed — same contract as
    oplog row writes.
    """
    if not lines:
        return
    rows = [
        [task_id, job_id, run_id, seq_offset + i, line.stream, line.level, line.text, line.created_at]
        for i, line in enumerate(lines)
    ]
    try:
        if ch_client is None:
            ch_client = get_ch_client()
        column_types = await get_column_types(ch_client, "task_logs")
        await ch_client.insert(
            "task_logs",
            rows,
            column_names=_TASK_LOG_COLS,
            column_type_names=[column_types[c] for c in _TASK_LOG_COLS],
        )
    except Exception:
        logger.error("Failed to write task_logs for task %s run %s", task_id, run_id, exc_info=True)


async def read_task_logs(task_id: int, run_id: int, tail: int = MAX_TASK_LOG_LINES) -> list[LogLine]:
    """Return the last ``tail`` (at most ``MAX_TASK_LOG_LINES``) captured log lines
    of one task attempt from CH ``task_logs``.

    Each line carries its ``stream``, ``level`` and emit time. Lines come back in
    emission order, fetched with a ``seq``-descending ``LIMIT``.
    """
    result = await get_ch_client().query(
        "SELECT stream, level, line, created_at FROM task_logs "
        "WHERE task_id = {task_id:UInt64} AND run_id = {run_id:UInt64} "
        "ORDER BY seq DESC LIMIT {tail:UInt64}",
        parameters={"task_id": task_id, "run_id": run_id, "tail": min(tail, MAX_TASK_LOG_LINES)},
    )
    rows = reversed(result.result_rows)
    return [LogLine(stream=row[0], level=row[1], text=row[2], created_at=row[3]) for row in rows]


async def _ensure_task_logs_table(task_id: int, run_id: int) -> None:
    """Bring the local CH schema up before streaming: a run that fails before
    ``task_scope`` (an import error, a shell or jvm task on a fresh DB) has not
    run its ``init_oplog_tables`` yet.

    Distributed mode never writes the schema (the operator migrates), and the
    host worker there holds no CH client, so it is skipped."""
    if not is_local():
        return
    try:
        await init_oplog_tables(get_ch_client())
    except Exception:
        logger.error("Failed to ensure task_logs for task %s run %s", task_id, run_id, exc_info=True)


@asynccontextmanager
async def stream_to_task_logs(
    task_id: int, job_id: int, run_id: int | None, *, console_prefix: str = ""
) -> AsyncIterator[ChLogSink]:
    """Yield the sink every capture path feeds, routed per
    :func:`task_logs_destination`. Lines bound for CH ``task_logs`` are flushed
    every ``LOG_FLUSH_INTERVAL`` seconds and finally on exit, so long-running
    tasks are tailed live; ``run_id=None`` (no run to key them by) sends to the
    console only. Console lines carry ``console_prefix``."""
    destination = task_logs_destination()
    console = destination != TASK_LOGS_CLICKHOUSE
    if run_id is None or destination == TASK_LOGS_CONSOLE:
        sink = ChLogSink(keep=False, console=console, prefix=console_prefix)
        try:
            yield sink
        finally:
            sink.finalize()
        return
    await _ensure_task_logs_table(task_id, run_id)
    sink = ChLogSink(console=console, prefix=console_prefix)
    flusher = _SinkFlusher(sink, task_id, job_id, run_id)
    flusher_task = asyncio.create_task(flusher.run())
    try:
        yield sink
    finally:
        flusher.request_stop()
        await flusher_task
        await flusher.flush_final()


@asynccontextmanager
async def capture_task_output(task_id: int, job_id: int, run_id: int):
    """
    Context manager to capture stdout, stderr, and ``logging`` for one task run.

    Output goes to a :func:`stream_to_task_logs` sink, which sends it to
    ``task_logs`` and/or the console. ``logging`` records are routed through
    :class:`_ChLogHandler` so each carries its true level; for the run the
    root logger's handlers are replaced with ours (restored on exit) so
    records are captured exactly once. A body that never awaits
    starves the periodic flusher — its logs land at exit.

    Args:
        task_id: Task ID the captured rows are keyed by.
        job_id: Job ID recorded on each row.
        run_id: Per-attempt snowflake ID — each retry keeps its own log stream.
    """
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    async with stream_to_task_logs(task_id, job_id, run_id) as sink:
        try:
            sys.stdout = _SinkWriter(sink, STDOUT_STREAM)
            sys.stderr = _SinkWriter(sink, STDERR_STREAM)
            root.handlers = [_ChLogHandler(sink)]
            try:
                root.setLevel(os.getenv("AAICLICK_LOG_LEVEL", "INFO").upper())
            except ValueError:
                root.setLevel(logging.INFO)
            try:
                yield
            except Exception:
                # The run's own log keeps why it failed; ``Task.error`` holds only
                # the latest attempt's one-line message.
                logger.exception("Task failed")
                raise
        finally:
            root.handlers = saved_handlers
            root.setLevel(saved_level)
            sys.stdout = original_stdout
            sys.stderr = original_stderr
