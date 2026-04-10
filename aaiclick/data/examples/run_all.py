"""
Run all aaiclick data examples using multiprocessing across available CPUs.

Each example runs in its own process via its amain() entrypoint, capturing
output to .tmp/<example_name>.txt. Results are printed in execution order.
"""

import asyncio
import contextlib
import os
import pathlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable, Coroutine

from .aggregation_table import amain as aggregation_table_example
from .array_operators import amain as array_operators_example
from .basic_operators import amain as basic_operators_example
from .data_manipulation import amain as data_manipulation_example
from .explode import amain as explode_example
from .group_by import amain as group_by_example
from .isin import amain as isin_example
from .nested_arrays import amain as nested_arrays_example
from .nullable import amain as nullable_example
from .order_by import amain as order_by_example
from .persistent import amain as persistent_example
from .selectors import amain as selectors_example
from .statistics import amain as statistics_example
from .transforms import amain as transforms_example
from .views import amain as views_example

TMP_DIR = pathlib.Path(".tmp")


def _example_path(title: str) -> pathlib.Path:
    name = title.lower().replace(" ", "_")
    return TMP_DIR / f"{name}.txt"


def _run_example(
    title: str,
    func: Callable[[], Coroutine],
) -> pathlib.Path:
    """Run a single example in a subprocess, capturing stdout to a file."""
    out_path = _example_path(title)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w") as f:
        with contextlib.redirect_stdout(f):
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
        ("Array Operators", array_operators_example),
        ("Explode", explode_example),
        ("Basic Operators", basic_operators_example),
        ("Data Manipulation", data_manipulation_example),
        ("Nested Arrays", nested_arrays_example),
        ("Statistics", statistics_example),
        ("Transforms", transforms_example),
        ("Views", views_example),
        ("Group By", group_by_example),
        ("Isin", isin_example),
        ("Nullable Columns", nullable_example),
        ("Dict Selectors", selectors_example),
        ("Aggregation Table", aggregation_table_example),
        ("Order By", order_by_example),
        ("Persistent Objects", persistent_example),
    ]

    results: dict[str, pathlib.Path] = {}
    max_workers = os.cpu_count() or 1

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_run_example, title, func): title
            for title, func in examples
        }
        for future in as_completed(futures):
            title = futures[future]
            results[title] = future.result()

    for title, _ in examples:
        _print_collapsible(title, results[title])

    print("**ALL DATA EXAMPLES COMPLETED SUCCESSFULLY**")


if __name__ == "__main__":
    main()
