"""Task logging utilities for orchestration backend."""

import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO


def get_logs_dir() -> str:
    """
    Get task log directory with OS-dependent defaults.

    The directory is created if it doesn't exist.

    Environment Variables:
        AAICLICK_LOG_DIR: Override default log directory

    Defaults:
        macOS: ~/.aaiclick/logs
        Linux: /var/log/aaiclick

    Returns:
        str: Log directory path
    """
    if custom_dir := os.getenv("AAICLICK_LOG_DIR"):
        log_dir = custom_dir
    elif sys.platform == "darwin":
        log_dir = os.path.expanduser("~/.aaiclick/logs")
    else:
        log_dir = "/var/log/aaiclick"

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    return log_dir


class _TeeWriter:
    """Writer that outputs to multiple streams."""

    def __init__(self, *streams: TextIO):
        self.streams = streams

    def write(self, data: str) -> int:
        for stream in self.streams:
            stream.write(data)
            stream.flush()
        return len(data)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()


@contextmanager
def capture_task_output(task_id: int):
    """
    Context manager to capture stdout and stderr to a task log file.

    Both stdout and stderr are captured to the same log file.
    Output is also preserved to the original streams (tee behavior).

    Args:
        task_id: Task ID used to generate log file path

    Yields:
        str: Path to the log file

    Example:
        with capture_task_output(task.id) as log_path:
            print("This goes to both console and log file")
            # Result: {get_logs_dir()}/{task_id}.log
    """
    log_dir = get_logs_dir()
    log_path = os.path.join(log_dir, f"{task_id}.log")

    original_stdout = sys.stdout
    original_stderr = sys.stderr

    with open(log_path, "w") as log_file:
        tee_stdout = _TeeWriter(original_stdout, log_file)
        tee_stderr = _TeeWriter(original_stderr, log_file)

        sys.stdout = tee_stdout
        sys.stderr = tee_stderr

        try:
            yield log_path
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr
