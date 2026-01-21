"""
aaiclick.orchestration.logging - Task logging utilities.

This module provides logging utilities for capturing task stdout/stderr
to log files with OS-dependent default directories.
"""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO


def get_logs_dir() -> str:
    """
    Get task log directory with OS-dependent defaults.

    The log directory can be overridden with AAICLICK_LOG_DIR environment variable.
    For distributed workers, set AAICLICK_LOG_DIR to a shared mount point.

    Returns:
        str: Path to log directory

    Defaults:
        - macOS: ~/.aaiclick/logs
        - Linux: /var/log/aaiclick
        - Override: AAICLICK_LOG_DIR environment variable
    """
    # Check for override
    if custom_dir := os.getenv("AAICLICK_LOG_DIR"):
        return custom_dir

    # OS-dependent defaults
    if sys.platform == "darwin":  # macOS
        return os.path.expanduser("~/.aaiclick/logs")
    else:  # Linux and others
        return "/var/log/aaiclick"


@contextmanager
def capture_task_output(task_id: int):
    """
    Context manager to capture stdout and stderr to task log file.

    Redirects all print() statements and errors to {get_logs_dir()}/{task_id}.log.
    Both stdout and stderr are written to the same log file.
    Logs are flushed after each write for real-time visibility.

    Args:
        task_id: Snowflake ID of the task

    Example:
        >>> with capture_task_output(task_id):
        ...     print("This goes to the log file")
        ...     # All stdout/stderr captured here
    """
    log_dir = get_logs_dir()
    log_file_path = Path(log_dir) / f"{task_id}.log"

    # Ensure log directory exists
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    # Save original stdout/stderr
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    # Open log file for writing
    log_file: TextIO = open(log_file_path, "w", buffering=1)  # Line buffered

    try:
        # Redirect both stdout and stderr to log file
        sys.stdout = log_file
        sys.stderr = log_file
        yield log_file_path
    finally:
        # Restore original stdout/stderr
        sys.stdout = original_stdout
        sys.stderr = original_stderr

        # Close log file
        log_file.close()
