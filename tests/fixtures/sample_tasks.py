"""Sample task functions for orchestration tests."""


def simple_task():
    """A simple task that does basic arithmetic and prints."""
    a = 1
    b = 2
    c = a + b
    print(c)
    return c


async def async_task():
    """An async task that does basic arithmetic."""
    a = 10
    b = 20
    return a + b


def task_with_args(x: int, y: int) -> int:
    """A task that takes arguments."""
    result = x * y
    print(f"Result: {result}")
    return result


async def async_task_with_args(x: int, y: int) -> int:
    """An async task that takes arguments."""
    result = x + y
    print(f"Sum: {result}")
    return result


def failing_task():
    """A task that intentionally fails."""
    raise ValueError("This task failed intentionally")


def task_with_output():
    """A task that produces both stdout and stderr output."""
    import sys

    print("This is stdout")
    print("Error message", file=sys.stderr)
    return "done"
