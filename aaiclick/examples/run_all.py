"""
Run all aaiclick examples using multiprocessing across available CPUs.

Each example runs in its own process with its own data_context(), capturing
output to .tmp/<example_name>.txt. Results are printed in execution order.
"""

import asyncio
import contextlib
import os
import pathlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable, Coroutine

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
from .transforms import example as transforms_example
from .views import example as views_example

TMP_DIR = pathlib.Path(".tmp")


def _example_path(title: str) -> pathlib.Path:
    name = title.lower().replace(" ", "_")
    return TMP_DIR / f"{name}.txt"


def _run_example(
    title: str,
    func: Callable[[], Coroutine],
    needs_context: bool,
) -> pathlib.Path:
    """Run a single example in a subprocess, capturing stdout to a file."""
    out_path = _example_path(title)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    async def _with_context():
        async with data_context():
            await func()

    with out_path.open("w") as f:
        with contextlib.redirect_stdout(f):
            if needs_context:
                asyncio.run(_with_context())
            else:
                asyncio.run(func())

    return out_path


def _print_collapsible(title: str, out_path: pathlib.Path):
    print("<details>")
    print(f"<summary>{title}</summary>\n")
    print("```")
    print(out_path.read_text().rstrip())
    print("```")
    print("</details>\n")


def main():
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    examples = [
        ("Array Operators", array_operators_example, True),
        ("Basic Operators", basic_operators_example, True),
        ("Data Manipulation", data_manipulation_example, True),
        ("Nested Arrays", nested_arrays_example, True),
        ("Statistics", statistics_example, True),
        ("Transforms", transforms_example, True),
        ("Views", views_example, True),
        ("Group By", group_by_example, True),
        ("Nullable Columns", nullable_example, True),
        ("Dict Selectors", selectors_example, True),
        ("Aggregation Table", aggregation_table_example, True),
        ("Orchestration Basic", orchestration_basic_example, False),
        ("Orchestration Dynamic", orchestration_dynamic_example, False),
    ]

    results: dict[str, pathlib.Path] = {}
    max_workers = os.cpu_count() or 1

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_run_example, title, func, needs_context): title
            for title, func, needs_context in examples
        }
        for future in as_completed(futures):
            title = futures[future]
            results[title] = future.result()

    for title, _, _ in examples:
        _print_collapsible(title, results[title])

    print("**ALL EXAMPLES COMPLETED SUCCESSFULLY**")


if __name__ == "__main__":
    main()
