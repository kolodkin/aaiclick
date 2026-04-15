"""
Shared utilities for running aaiclick examples via multiprocessing.

Each example runs in its own process via its amain() entrypoint, capturing
output to .tmp/<example_name>.txt. Results are printed in execution order.
"""

import asyncio
import contextlib
import multiprocessing
import os
import pathlib
from collections.abc import Callable, Coroutine
from concurrent.futures import ProcessPoolExecutor, as_completed

TMP_DIR = pathlib.Path(".tmp")

ExampleList = list[tuple[str, Callable[[], Coroutine]]]


def _example_path(title: str) -> pathlib.Path:
    name = title.lower().replace(" ", "_")
    return TMP_DIR / f"{name}.txt"


def run_example(
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


def run_all(examples: ExampleList, banner: str):
    """Run all examples in parallel and print collapsible results."""
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    results: dict[str, pathlib.Path] = {}

    if len(examples) == 1:
        title, func = examples[0]
        results[title] = run_example(title, func)
    else:
        max_workers = min(len(examples), os.cpu_count() or 1)
        # "spawn" starts a fresh interpreter — no inherited chdb C++ singleton.
        mp_ctx = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=max_workers, mp_context=mp_ctx) as executor:
            futures = {
                executor.submit(run_example, title, func): title
                for title, func in examples
            }
            for future in as_completed(futures):
                title = futures[future]
                results[title] = future.result()

    for title, _ in examples:
        _print_collapsible(title, results[title])

    print(f"**{banner}**")
