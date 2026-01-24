"""Sample task functions for orchestration tests.

Note: Task parameters must be aaiclick Objects or Views.
Native Python values are not supported as task parameters.
"""


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
    import sys

    print("This is stdout")
    print("Error message", file=sys.stderr)
