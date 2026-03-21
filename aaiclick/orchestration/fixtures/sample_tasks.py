"""Sample task functions for orchestration tests."""

import sys
from pathlib import Path

from aaiclick.data.data_context import create_object_from_value


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


async def data_task():
    """A task that creates Objects — used for oplog and lifecycle tests."""
    obj = await create_object_from_value([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    return obj


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
