"""Task logging utilities for orchestration backend.

Task stdout/stderr is captured to ClickHouse ``task_logs``, streamed from
inside the task process. Every runner (subprocess, docker, kubernetes) runs
the same ``capture_task_output`` path — and shell tasks are flushed host-side
via ``execution.log_flush`` — so all runs surface their logs through one
cross-host read path (:func:`read_task_logs`) no matter which host wrote them.
"""

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager, suppress
from typing import TextIO

from aaiclick.backend import is_chdb
from aaiclick.data.data_context import ChClient, get_ch_client
from aaiclick.data.data_context.clickhouse_client import create_clickhouse_client
from aaiclick.datetime_utils import utc_now
from aaiclick.log_models import STDERR_STREAM, STDOUT_STREAM, LogLevel, LogLine, LogStream, normalize_level
from aaiclick.oplog.models import get_column_types

logger = logging.getLogger(__name__)

_TASK_LOG_COLS = ["task_id", "job_id", "run_id", "seq", "stream", "level", "line", "created_at"]

# How often a running task's captured output is drained to CH task_logs.
# Matches the UI poll interval — flushing faster buys nothing.
LOG_FLUSH_INTERVAL = 2.0


_DEFAULT_STREAM_LEVEL: dict[LogStream, LogLevel] = {STDOUT_STREAM: "INFO", STDERR_STREAM: "ERROR"}


class _ChLogSink:
    """Accumulate captured output as level-tagged lines for a CH batch write.

    ``write`` is sync — it's driven by ``print`` through ``_TeeWriter`` while the
    task runs. Each stream (stdout / stderr) keeps its own partial-line buffer so
    a line is tagged with the stream that emitted it; completed lines are
    appended in emission order, each stamped with its own emit time. ``record``
    is the logging path: it appends already-leveled lines from ``_ChLogHandler``.
    Lines are drained incrementally by a periodic flusher while the task runs
    and finally on exit (:func:`capture_task_output`).
    """

    def __init__(self) -> None:
        self._partial: dict[LogStream, str] = {STDOUT_STREAM: "", STDERR_STREAM: ""}
        self._lines: list[LogLine] = []

    def write(self, stream: LogStream, data: str) -> None:
        parts = (self._partial[stream] + data).split("\n")
        self._partial[stream] = parts.pop()
        level = _DEFAULT_STREAM_LEVEL[stream]
        self._lines.extend(LogLine(stream=stream, level=level, text=p, created_at=utc_now()) for p in parts)

    def record(self, level: LogLevel, text: str) -> None:
        """Append a logging record's message as level-tagged line(s)."""
        now = utc_now()
        self._lines.extend(
            LogLine(stream=STDERR_STREAM, level=level, text=p, created_at=now) for p in text.rstrip("\n").split("\n")
        )

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


class _SinkFlusher:
    """Incrementally write a sink's completed lines to CH ``task_logs``.

    Tracks the running ``seq`` offset so successive flushes keep ``seq``
    strictly increasing per ``run_id``. ``run`` loops until cancelled; the
    owner cancels it and calls ``flush_final`` for the tail. Reads
    ``LOG_FLUSH_INTERVAL`` through the module on every tick so tests can
    monkeypatch it.

    Client choice per backend: chdb calls are sync on the event loop, so the
    flusher shares the task's client — the two can never interleave. A remote
    clickhouse-connect ``AsyncClient`` autogenerates a server ``session_id``
    that rejects concurrent queries, so there the flusher lazily opens its
    own client (closed by ``flush_final``) instead of racing the task body's.
    """

    def __init__(self, sink: _ChLogSink, task_id: int, job_id: int, run_id: int) -> None:
        self._sink = sink
        self._task_id = task_id
        self._job_id = job_id
        self._run_id = run_id
        self._offset = 0
        self._own_client: ChClient | None = None

    async def _client(self) -> ChClient:
        if is_chdb():
            return get_ch_client()
        if self._own_client is None:
            self._own_client = await create_clickhouse_client()
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
        while True:
            await asyncio.sleep(LOG_FLUSH_INTERVAL)
            await self.flush_pending()


class _TeeWriter:
    """Writer that outputs to multiple streams and an optional CH sink.

    ``source`` tags the sink rows with the stream this writer fronts (stdout /
    stderr) so the captured lines carry their origin."""

    def __init__(self, *streams: TextIO, sink: _ChLogSink | None = None, source: LogStream | None = None):
        self.streams = streams
        self._sink = sink
        self._source = source

    def write(self, data: str) -> int:
        for stream in self.streams:
            stream.write(data)
            stream.flush()
        if self._sink is not None and self._source is not None:
            self._sink.write(self._source, data)
        return len(data)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()


class _ChLogHandler(logging.Handler):
    """Route ``logging`` records into the active CH sink with their true level.

    Echoes the formatted message to the original stderr for visibility,
    bypassing the tee so the record is not captured a second time as raw
    stderr text.
    """

    def __init__(self, sink: _ChLogSink, console: TextIO):
        super().__init__()
        self._sink = sink
        self._console = console
        self.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = normalize_level(record.levelno)
            msg = self.format(record)
            self._console.write(msg + "\n")
            self._console.flush()
            self._sink.record(level, msg)
        except Exception:  # never let logging crash the task
            self.handleError(record)


def shell_text_to_lines(text: str) -> list[LogLine]:
    """Convert a shell task's captured output text into ``LogLine`` rows.

    Shell runners capture stdout and stderr merged into one text blob (host
    pipe, ``docker logs``, ``kubectl logs``), so every line is tagged
    ``stdout`` / ``INFO``. A trailing newline does not produce an empty line.
    """
    if not text:
        return []
    parts = text.split("\n")
    if parts and parts[-1] == "":
        parts.pop()
    return [LogLine(stream=STDOUT_STREAM, level="INFO", text=p) for p in parts]


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


async def read_task_logs(task_id: int, run_id: int, tail: int | None = None) -> list[LogLine]:
    """Return captured log lines for a single task attempt from CH ``task_logs``.

    Each line carries its source ``stream`` (stdout / stderr), its ``level``, and
    the time it was emitted. When ``tail`` is given, returns only the last
    ``tail`` lines (still in emission order) — fetched with a ``seq``-descending
    ``LIMIT`` so the read stays bounded for large logs.
    """
    where = "WHERE task_id = {task_id:UInt64} AND run_id = {run_id:UInt64}"
    parameters: dict[str, int] = {"task_id": task_id, "run_id": run_id}
    cols = "stream, level, line, created_at"
    if tail is not None:
        parameters["tail"] = tail
        result = await get_ch_client().query(
            f"SELECT {cols} FROM task_logs {where} ORDER BY seq DESC LIMIT {{tail:UInt64}}",
            parameters=parameters,
        )
        rows = list(reversed(result.result_rows))
    else:
        result = await get_ch_client().query(
            f"SELECT {cols} FROM task_logs {where} ORDER BY seq",
            parameters=parameters,
        )
        rows = result.result_rows
    return [LogLine(stream=row[0], level=row[1], text=row[2], created_at=row[3].replace(tzinfo=None)) for row in rows]


@asynccontextmanager
async def capture_task_output(task_id: int, job_id: int, run_id: int):
    """
    Context manager to capture stdout, stderr, and ``logging`` for one task run.

    Output is teed to the original streams and a ClickHouse sink. ``logging``
    records are routed through :class:`_ChLogHandler` so each carries its true
    level; for the duration of the run the root logger's handlers are replaced
    with ours (restored on exit) so records are captured exactly once. The sink
    is drained to ``task_logs`` every ``LOG_FLUSH_INTERVAL`` seconds while the
    body runs and finally on exit (success or failure), so long-running tasks
    are tailed live and every runner gets a host-independent log source. A body
    that never awaits starves the periodic flusher — its logs land at exit.

    Args:
        task_id: Task ID the captured rows are keyed by.
        job_id: Job ID recorded on each row.
        run_id: Per-attempt snowflake ID — each retry keeps its own log stream.
    """
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
