"""Sample task functions for orchestration tests.

Note: Task parameters must be aaiclick Objects or Views.
Native Python values are not supported as task parameters.
"""

import sys

from aaiclick.data.data_context import create_object_from_value, get_ch_client
from aaiclick.data.object import Object


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


async def increment_counter(counter: Object) -> Object:
    """Increment a counter Object and fail if count < 3.

    Reads the current counter value, increments it in-place via INSERT,
    and raises if the new count hasn't reached 3 yet. Since the Object
    persists in ClickHouse, the increment survives across retries.

    Args:
        counter: Object containing a single integer counter value

    Returns:
        Object: The counter Object (after reaching count >= 3)
    """
    ch = get_ch_client()

    # Read current count (number of rows = counter value)
    result = await ch.query(f"SELECT count() FROM {counter.table}")
    count = result.result_rows[0][0]
    new_count = count + 1

    # Append a row to increment the counter
    new_row = await create_object_from_value([1])
    await ch.command(
        f"INSERT INTO {counter.table} SELECT * FROM {new_row.table}"
    )

    print(f"Counter: {count} -> {new_count}")

    if new_count < 3:
        raise RuntimeError(f"Counter at {new_count}, need 3")

    return counter
