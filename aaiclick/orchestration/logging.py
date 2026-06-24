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
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

from aaiclick.backend import get_root, is_local
from aaiclick.data.data_context import get_ch_client
from aaiclick.oplog.models import TASK_LOGS_EXPECTED_COLUMNS

logger = logging.getLogger(__name__)

_TASK_LOG_COLS = list(TASK_LOGS_EXPECTED_COLUMNS)
_TASK_LOG_TYPE_NAMES = [TASK_LOGS_EXPECTED_COLUMNS[c] for c in _TASK_LOG_COLS]


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


class _ChLogSink:
    """Accumulate captured output as discrete lines for a ClickHouse batch write.

    ``write`` is sync — it's driven by ``print`` through ``_TeeWriter`` while the
    task runs. The async flush happens once after the task body completes
    (:func:`capture_task_output`), so the task's own ClickHouse work never races
    the log write on a shared (chdb single-session) client.
    """

    def __init__(self) -> None:
        self._partial = ""
        self._lines: list[str] = []

    def write(self, data: str) -> None:
        """Split incoming text on newlines, buffering the trailing partial line."""
        parts = (self._partial + data).split("\n")
        self._partial = parts.pop()
        self._lines.extend(parts)

    def finalize(self) -> list[str]:
        """Return all buffered lines, including any unterminated trailing line."""
        if self._partial:
            self._lines.append(self._partial)
            self._partial = ""
        lines, self._lines = self._lines, []
        return lines


class _TeeWriter:
    """Writer that outputs to multiple streams and an optional CH sink."""

    def __init__(self, *streams: TextIO, sink: _ChLogSink | None = None):
        self.streams = streams
        self._sink = sink

    def write(self, data: str) -> int:
        for stream in self.streams:
            stream.write(data)
            stream.flush()
        if self._sink is not None:
            self._sink.write(data)
        return len(data)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()


async def flush_task_logs(task_id: int, job_id: int, run_id: int, lines: list[str]) -> None:
    """Best-effort batch insert of captured log lines into CH ``task_logs``.

    A failed write must not fail the task, so errors are logged and swallowed —
    same contract as oplog row writes.
    """
    if not lines:
        return
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [[task_id, job_id, run_id, seq, line, now] for seq, line in enumerate(lines)]
    try:
        await get_ch_client().insert(
            "task_logs",
            rows,
            column_names=_TASK_LOG_COLS,
            column_type_names=_TASK_LOG_TYPE_NAMES,
        )
    except Exception:
        logger.error("Failed to write task_logs for task %s run %s", task_id, run_id, exc_info=True)


async def read_task_logs(task_id: int, run_id: int) -> list[str]:
    """Return captured log lines for a single task attempt from CH ``task_logs``."""
    result = await get_ch_client().query(
        "SELECT line FROM task_logs WHERE task_id = {task_id:UInt64} AND run_id = {run_id:UInt64} ORDER BY seq",
        parameters={"task_id": task_id, "run_id": run_id},
    )
    return [row[0] for row in result.result_rows]


@asynccontextmanager
async def capture_task_output(task_id: int, job_id: int, run_id: int):
    """
    Context manager to capture stdout and stderr for a single task run.

    Output is teed to the original streams, an on-host log file, and a
    ClickHouse sink. The sink is flushed to ``task_logs`` once the body exits
    (on success or failure), giving every runner a host-independent log source.

    Log files are organized as: {base}/{job_id}/{task_id}/{run_id}.log

    Args:
        task_id: Task ID used to generate log file path.
        job_id: Job ID for the directory hierarchy.
        run_id: Per-attempt snowflake ID — each retry gets its own log file.

    Yields:
        str: Path to the log file

    Example:
        async with capture_task_output(task.id, task.job_id, run_id) as log_path:
            print("This goes to console, the log file, and ClickHouse")
            # Result: {get_logs_dir()}/{job_id}/{task_id}/{run_id}.log
    """
    log_dir = os.path.join(get_logs_dir(), str(job_id), str(task_id))
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path = os.path.join(log_dir, f"{run_id}.log")

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sink = _ChLogSink()
    log_file = open(log_path, "w")

    sys.stdout = _TeeWriter(original_stdout, log_file, sink=sink)
    sys.stderr = _TeeWriter(original_stderr, log_file, sink=sink)

    try:
        yield log_path
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        log_file.close()
        await flush_task_logs(task_id, job_id, run_id, sink.finalize())
