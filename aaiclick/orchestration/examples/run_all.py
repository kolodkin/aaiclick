"""
Run all aaiclick orchestration examples using multiprocessing across available CPUs.

Each example runs in its own process via its amain() entrypoint, capturing
output to .tmp/<example_name>.txt. Results are printed in execution order.
"""

import asyncio
import contextlib
import os
import pathlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable, Coroutine

from .orchestration_basic import amain as orchestration_basic_example
from .orchestration_dynamic import amain as orchestration_dynamic_example

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
        ("Orchestration Basic", orchestration_basic_example),
        ("Orchestration Dynamic", orchestration_dynamic_example),
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

    print("**ALL ORCHESTRATION EXAMPLES COMPLETED SUCCESSFULLY**")


if __name__ == "__main__":
    main()
