"""Task logging utilities for orchestration backend.

Task stdout/stderr is captured two ways:

- **Local file** — teed to ``{logs_dir}/{job_id}/{task_id}/{run_id}.log`` for
  on-host debugging.
- **ClickHouse ``task_logs``** — streamed from inside the task process. Every
  runner (subprocess, docker, kubernetes) runs the same ``capture_task_output``
  path, so distributed and containerized runs surface their logs through one
  cross-host read path (:func:`read_task_logs`) no matter which host wrote them.
"""

import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TextIO

from aaiclick.backend import get_root, is_local
from aaiclick.data.data_context import get_ch_client
from aaiclick.datetime_utils import utc_now
from aaiclick.oplog.models import TASK_LOGS_EXPECTED_COLUMNS
from aaiclick.view_models import STDERR_STREAM, STDOUT_STREAM, LogLevel, LogLine, LogStream, normalize_level

logger = logging.getLogger(__name__)

_TASK_LOG_COLS = list(TASK_LOGS_EXPECTED_COLUMNS)
_TASK_LOG_TYPE_NAMES = list(TASK_LOGS_EXPECTED_COLUMNS.values())


def get_logs_dir() -> str:
    """
    Get task log directory.

    The directory is created if it doesn't exist.

    Environment Variables:
        AAICLICK_LOG_DIR: Override default log directory

    Defaults:
        Local mode:       {AAICLICK_LOCAL_ROOT}/logs (i.e. ~/.aaiclick/logs)
        Distributed mode: /var/log/aaiclick (Linux), ~/.aaiclick/logs (macOS)

    Returns:
        str: Log directory path
    """
    if custom_dir := os.getenv("AAICLICK_LOG_DIR"):
        log_dir = custom_dir
    elif is_local():
        log_dir = str(get_root() / "logs")
    elif sys.platform == "darwin":
        log_dir = os.path.expanduser("~/.aaiclick/logs")
    else:
        log_dir = "/var/log/aaiclick"

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    return log_dir


_DEFAULT_STREAM_LEVEL: dict[LogStream, LogLevel] = {STDOUT_STREAM: "INFO", STDERR_STREAM: "ERROR"}


class _ChLogSink:
    """Accumulate captured output as level-tagged lines for a CH batch write.

    ``write`` is sync — it's driven by ``print`` through ``_TeeWriter`` while the
    task runs. Each stream (stdout / stderr) keeps its own partial-line buffer so
    a line is tagged with the stream that emitted it; completed lines are
    appended in emission order, each stamped with its own emit time. ``record``
    is the logging path: it appends already-leveled lines from ``_ChLogHandler``.
    The async flush happens once after the task body completes
    (:func:`capture_task_output`), so the task's own ClickHouse work never races
    the log write on a shared (chdb single-session) client.
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
        lines, self._lines = self._lines, []
        return lines


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

    Echoes the formatted message to the on-host log file and the original stderr
    for visibility, bypassing the tee so the record is not captured a second time
    as raw stderr text.
    """

    def __init__(self, sink: _ChLogSink, log_file: TextIO, console: TextIO):
        super().__init__()
        self._sink = sink
        self._log_file = log_file
        self._console = console
        self.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = normalize_level(record.levelno)
            msg = self.format(record)
            for stream in (self._log_file, self._console):
                stream.write(msg + "\n")
                stream.flush()
            self._sink.record(level, msg)
        except Exception:  # never let logging crash the task
            self.handleError(record)


async def flush_task_logs(task_id: int, job_id: int, run_id: int, lines: list[LogLine]) -> None:
    """Best-effort batch insert of captured log lines into CH ``task_logs``.

    A failed write must not fail the task, so errors are logged and swallowed —
    same contract as oplog row writes.
    """
    if not lines:
        return
    rows = [
        [task_id, job_id, run_id, seq, line.stream, line.level, line.text, line.created_at]
        for seq, line in enumerate(lines)
    ]
    try:
        await get_ch_client().insert(
            "task_logs",
            rows,
            column_names=_TASK_LOG_COLS,
            column_type_names=_TASK_LOG_TYPE_NAMES,
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

    Output is teed to the original streams, an on-host log file, and a ClickHouse
    sink. ``logging`` records are routed through :class:`_ChLogHandler` so each
    carries its true level; for the duration of the run the root logger's
    handlers are replaced with ours (restored on exit) so records are captured
    exactly once. The sink is flushed to ``task_logs`` once the body exits (on
    success or failure), giving every runner a host-independent log source.

    Log files are organized as: {base}/{job_id}/{task_id}/{run_id}.log

    Args:
        task_id: Task ID used to generate log file path.
        job_id: Job ID for the directory hierarchy.
        run_id: Per-attempt snowflake ID — each retry gets its own log file.

    Yields:
        str: Path to the log file
    """
    log_dir = os.path.join(get_logs_dir(), str(job_id), str(task_id))
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path = os.path.join(log_dir, f"{run_id}.log")

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sink = _ChLogSink()
    log_file = open(log_path, "w")

    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    try:
        sys.stdout = _TeeWriter(original_stdout, log_file, sink=sink, source=STDOUT_STREAM)
        sys.stderr = _TeeWriter(original_stderr, log_file, sink=sink, source=STDERR_STREAM)
        root.handlers = [_ChLogHandler(sink, log_file, original_stderr)]
        try:
            root.setLevel(os.getenv("AAICLICK_LOG_LEVEL", "INFO").upper())
        except ValueError:
            root.setLevel(logging.INFO)
        yield log_path
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        log_file.close()
        await flush_task_logs(task_id, job_id, run_id, sink.finalize())
