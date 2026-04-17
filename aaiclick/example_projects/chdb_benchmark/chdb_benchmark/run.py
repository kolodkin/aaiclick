"""Benchmark runner — generates data, measures chdb vs aaiclick, prints report.

Each benchmark operation runs inside its own fresh context (chdb Session or
aaiclick DataContext). This isolates memory measurements per operation and
removes the need for manual result-table cleanup between benchmarks.

Memory is tracked via process RSS (/proc/self/statm) rather than
``tracemalloc`` so that native C++ allocations from chdb show up — using
``tracemalloc`` would under-measure chdb while fully counting aaiclick's
Python orchestration, producing an unfair comparison.

Usage:
    python -m chdb_benchmark
    python -m chdb_benchmark --rows 500000 --runs 5
"""

import argparse
import asyncio
import contextlib
import os
import random
import time

from . import bench_aaiclick, bench_chdb_native
from .config import BENCH_NAMES, CATEGORIES, FILTER_THRESHOLD, NUM_ROWS, NUM_RUNS, SUBCATEGORIES
from .report import console, print_results

MODULES = [bench_chdb_native, bench_aaiclick]


def generate_raw_data(num_rows):
    random.seed(42)
    return {
        "id": list(range(num_rows)),
        "category": [random.choice(CATEGORIES) for _ in range(num_rows)],
        "subcategory": [random.choice(SUBCATEGORIES) for _ in range(num_rows)],
        "amount": [random.uniform(0, 1000) for _ in range(num_rows)],
        "quantity": [random.randint(1, 100) for _ in range(num_rows)],
    }


def _get_rss():
    """Current RSS in bytes via /proc/self/statm (Linux)."""
    with open("/proc/self/statm") as f:
        pages = int(f.read().split()[1])
    return pages * os.sysconf("SC_PAGE_SIZE")


def measure_sync(fn, data, num_runs):
    fn(data)  # warmup
    times = []
    peak_mem = 0
    for _ in range(num_runs):
        rss_before = _get_rss()
        t0 = time.perf_counter()
        fn(data)
        elapsed = time.perf_counter() - t0
        rss_after = _get_rss()
        times.append(elapsed)
        peak_mem = max(peak_mem, rss_after - rss_before)
    return sum(times) / num_runs, peak_mem


async def measure_async(fn, data, num_runs):
    await fn(data)  # warmup
    times = []
    peak_mem = 0
    for _ in range(num_runs):
        rss_before = _get_rss()
        t0 = time.perf_counter()
        await fn(data)
        elapsed = time.perf_counter() - t0
        rss_after = _get_rss()
        times.append(elapsed)
        peak_mem = max(peak_mem, rss_after - rss_before)
    return sum(times) / num_runs, peak_mem


@contextlib.contextmanager
def _nullctx():
    yield


async def _run_in_ctx(ctx_fn, coro_fn):
    """Run a coroutine inside a sync or async context."""
    ctx = _nullctx() if ctx_fn is None else ctx_fn()
    if hasattr(ctx, "__aenter__"):
        async with ctx:
            return await coro_fn()
    with ctx:
        return await coro_fn()


async def bench_module(mod, raw_data, num_runs, results):
    """Run every benchmark for one library, with a fresh context per operation."""
    is_async = getattr(mod, "IS_ASYNC", False)
    ctx_fn = getattr(mod, "context", None)

    for bench_name in BENCH_NAMES:
        if bench_name == "Ingest":
            console.print(f"  Ingest ({mod.NAME})...")

            async def _ingest():
                if is_async:
                    return await measure_async(mod.convert, raw_data, num_runs)
                return measure_sync(mod.convert, raw_data, num_runs)

            avg_time, peak_mem = await _run_in_ctx(ctx_fn, _ingest)
            results["Ingest"][mod.NAME] = {"time": avg_time, "memory": peak_mem}
            continue

        if bench_name not in mod.BENCHMARKS:
            continue
        console.print(f"  {bench_name} ({mod.NAME})...")
        fn = mod.BENCHMARKS[bench_name]

        async def _bench(fn=fn):
            if is_async:
                dataset = await mod.convert(raw_data)
                return await measure_async(fn, dataset, num_runs)
            dataset = mod.convert(raw_data)
            return measure_sync(fn, dataset, num_runs)

        avg_time, peak_mem = await _run_in_ctx(ctx_fn, _bench)
        results[bench_name][mod.NAME] = {"time": avg_time, "memory": peak_mem}


async def run(num_rows, num_runs):
    versions = [f"{m.NAME} {m.VERSION}" for m in MODULES]
    lib_names = [m.NAME for m in MODULES]

    console.print("\n[bold]chdb vs aaiclick Benchmark[/bold]")
    console.print(f"  {', '.join(versions)}")
    console.print(f"  {num_rows:,} rows, {num_runs} runs per operation")
    console.print(f"  Filter threshold: {FILTER_THRESHOLD}")
    console.print(f"  Categories: {len(CATEGORIES)}, Subcategories: {len(SUBCATEGORIES)}")
    console.print("  Fresh context per operation (chdb runs first, then aaiclick)\n")

    raw_data = generate_raw_data(num_rows)
    results = {name: {} for name in BENCH_NAMES}

    for mod in MODULES:
        await bench_module(mod, raw_data, num_runs, results)

    print_results(results, BENCH_NAMES, lib_names, num_rows, num_runs)


def main():
    parser = argparse.ArgumentParser(description="chdb vs aaiclick benchmark")
    parser.add_argument("--rows", type=int, default=NUM_ROWS, help="Number of rows")
    parser.add_argument("--runs", type=int, default=NUM_RUNS, help="Runs per operation")
    args = parser.parse_args()

    # Use in-memory chdb — must be set before any Session is created
    # (including the Snowflake ID generator). chdb allows one path per process.
    os.environ["AAICLICK_CH_URL"] = "chdb://:memory:"

    asyncio.run(run(args.rows, args.runs))


if __name__ == "__main__":
    main()
