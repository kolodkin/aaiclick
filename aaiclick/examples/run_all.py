"""
Run all aaiclick examples.

This script runs all example modules in sequence within a single data_context().
Orchestration examples run with their own context management.

Output is formatted as markdown with collapsible sections per example.
"""

import asyncio
import contextlib
import io

from aaiclick.data.data_context import data_context

from .aggregation_table import example as aggregation_table_example
from .array_operators import example as array_operators_example
from .basic_operators import example as basic_operators_example
from .data_manipulation import example as data_manipulation_example
from .group_by import example as group_by_example
from .nested_arrays import example as nested_arrays_example
from .nullable import example as nullable_example
from .orchestration_basic import amain as orchestration_basic_example
from .orchestration_dynamic import amain as orchestration_dynamic_example
from .selectors import example as selectors_example
from .statistics import example as statistics_example
from .views import example as views_example


async def _run_and_capture(func):
    """Run an async function and capture its stdout output."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        await func()
    return buf.getvalue().rstrip()


def _print_collapsible(title, output):
    """Print captured output as a collapsible markdown section."""
    print(f"<details>")
    print(f"<summary>{title}</summary>\n")
    print(f"```")
    print(output)
    print(f"```")
    print(f"</details>\n")


async def main():
    """Run all examples."""
    context_examples = [
        ("Array Operators", array_operators_example),
        ("Basic Operators", basic_operators_example),
        ("Data Manipulation", data_manipulation_example),
        ("Nested Arrays", nested_arrays_example),
        ("Statistics", statistics_example),
        ("Views", views_example),
        ("Group By", group_by_example),
        ("Nullable Columns", nullable_example),
        ("Dict Selectors", selectors_example),
        ("Aggregation Table", aggregation_table_example),
    ]

    async with data_context():
        for title, func in context_examples:
            output = await _run_and_capture(func)
            _print_collapsible(title, output)

    # Orchestration examples manage their own contexts (OrchContext + data_context())
    orchestration_examples = [
        ("Orchestration Basic", orchestration_basic_example),
        ("Orchestration Dynamic", orchestration_dynamic_example),
    ]

    for title, func in orchestration_examples:
        output = await _run_and_capture(func)
        _print_collapsible(title, output)

    print("**ALL EXAMPLES COMPLETED SUCCESSFULLY**")


if __name__ == "__main__":
    asyncio.run(main())
