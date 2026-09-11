"""Sample task functions for orchestration tests."""

import asyncio
import logging
import sys
from pathlib import Path

# A module attribute that resolves but is intentionally not callable. Used by
# entrypoint-validation tests; keep it a plain constant so the "not callable"
# guarantee never drifts.
not_callable = "i am not a function"


def simple_task():
    """A simple task that does basic arithmetic and prints."""
    a = 1
    b = 2
    c = a + b
    print(c)


async def async_task():
    """An async task that does basic arithmetic."""
    a = 10
    b = 20
    print(a + b)


def failing_task():
    """A task that intentionally fails."""
    raise ValueError("This task failed intentionally")


def task_with_output():
    """A task that produces both stdout and stderr output."""
    print("This is stdout")
    print("Error message", file=sys.stderr)


def task_with_log_levels():
    """Emit one logging record at each level so the UI can be checked for coloring."""
    log = logging.getLogger("sample")
    log.info("info line")
    log.warning("warning line")
    log.error("error line")


async def slow_task(seconds: float, steps: int = 20):
    """Stay RUNNING for a few seconds, emitting a log line per step.

    Every other task here finishes in milliseconds, which shows the UI one
    final state and nothing to update. This one lives long enough for a
    browser test to watch a status badge flip and log lines accumulate — the
    two live-update paths the operator UI actually has, and they are not the
    same path: status changes ride the ``/events`` stream, log lines do not.

    Sleeps with ``asyncio.sleep`` so an in-process execution worker keeps
    serving requests (and the SSE stream) while this runs.
    """
    log = logging.getLogger("sample")
    for step in range(1, steps + 1):
        log.info("step %d of %d", step, steps)
        await asyncio.sleep(seconds / steps)


def flaky_task(counter_file: str):
    """A task that fails twice then succeeds on the third attempt.

    Uses a file-based counter to track attempts across retries.
    The counter file persists between task invocations since each
    retry runs in a fresh data_context.

    Args:
        counter_file: Path to a file used as an attempt counter

    Returns:
        str: "success" on the third attempt
    """
    path = Path(counter_file)
    count = int(path.read_text()) if path.exists() else 0
    count += 1
    path.write_text(str(count))

    print(f"Attempt {count}")

    if count < 3:
        raise RuntimeError(f"Attempt {count}, need 3")

    return "success"
