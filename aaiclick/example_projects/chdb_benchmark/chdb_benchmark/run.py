"""Benchmark runner — generates data, measures chdb vs aaiclick, prints report.

Libraries run in strict serial order: all chdb benchmarks first (with its
context), then all aaiclick benchmarks (with its context). Only one chdb
engine is active at a time, avoiding compute resource competition.

Usage:
    python -m chdb_benchmark
    python -m chdb_benchmark --rows 500000 --runs 5
"""

import argparse
import asyncio
import os
import random
import time
import tracemalloc

from . import bench_aaiclick, bench_chdb_native
from .report import console, print_results

BENCH_NAMES = [
    "Ingest",
    "Column sum",
    "Column multiply",
    "Filter rows",
    "Sort",
    "Count distinct",
    "Group-by sum",
    "Group-by count",
    "Group-by multi-agg",
    "Multi-key group-by",
    "High-card group-by",
]

CATEGORIES = [f"cat_{i}" for i in range(10)]
SUBCATEGORIES = [f"sub_{i}" for i in range(1000)]
FILTER_THRESHOLD = 500.0


def generate_raw_data(num_rows):
    random.seed(42)
    return {
        "id": list(range(num_rows)),
        "category": [random.choice(CATEGORIES) for _ in range(num_rows)],
        "subcategory": [random.choice(SUBCATEGORIES) for _ in range(num_rows)],
        "amount": [random.uniform(0, 1000) for _ in range(num_rows)],
        "quantity": [random.randint(1, 100) for _ in range(num_rows)],
    }


def measure_sync(fn, data, num_runs):
    fn(data)  # warmup
    times = []
    peak_mem = 0
    for _ in range(num_runs):
        tracemalloc.start()
        t0 = time.perf_counter()
        fn(data)
        elapsed = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        times.append(elapsed)
        peak_mem = max(peak_mem, peak)
    return sum(times) / num_runs, peak_mem


async def measure_async(fn, data, num_runs):
    await fn(data)  # warmup
    times = []
    peak_mem = 0
    for _ in range(num_runs):
        tracemalloc.start()
        t0 = time.perf_counter()
        await fn(data)
        elapsed = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        times.append(elapsed)
        peak_mem = max(peak_mem, peak)
    return sum(times) / num_runs, peak_mem


async def run(num_rows, num_runs):
    lib_names = [bench_chdb_native.NAME, bench_aaiclick.NAME]
    versions = [
        f"{bench_chdb_native.NAME} {bench_chdb_native.VERSION}",
        f"{bench_aaiclick.NAME} {bench_aaiclick.VERSION}",
    ]

    console.print("\n[bold]chdb vs aaiclick Benchmark[/bold]")
    console.print(f"  {', '.join(versions)}")
    console.print(f"  {num_rows:,} rows, {num_runs} runs per operation")
    console.print(f"  Filter threshold: {FILTER_THRESHOLD}")
    console.print(f"  Categories: {len(CATEGORIES)}, Subcategories: {len(SUBCATEGORIES)}")
    console.print("  Serial execution (chdb context → aaiclick context)\n")

    raw_data = generate_raw_data(num_rows)
    results = {name: {} for name in BENCH_NAMES}

    # Phase 1: chdb — open context, run all benchmarks, close context
    chdb_mod = bench_chdb_native
    with chdb_mod.context():
        chdb_dataset = chdb_mod.convert(raw_data, FILTER_THRESHOLD)
        chdb_benchmarks = chdb_mod.make_benchmarks(FILTER_THRESHOLD)

        console.print(f"  Ingest [{chdb_mod.NAME}]...")
        t, m = measure_sync(
            lambda d: chdb_mod.ingest_only(d, FILTER_THRESHOLD),
            raw_data,
            num_runs,
        )
        results["Ingest"]["chdb"] = {"time": t, "memory": m}
        chdb_mod.cleanup_results()

        for bench_name in BENCH_NAMES:
            if bench_name == "Ingest" or bench_name not in chdb_benchmarks:
                continue
            console.print(f"  {bench_name} [{chdb_mod.NAME}]...")
            t, m = measure_sync(chdb_benchmarks[bench_name], chdb_dataset, num_runs)
            results[bench_name]["chdb"] = {"time": t, "memory": m}
            chdb_mod.cleanup_results()

    # Phase 2: aaiclick — open context, run all benchmarks, close context
    aai_mod = bench_aaiclick
    async with aai_mod.context():
        aai_dataset = await aai_mod.convert(raw_data, FILTER_THRESHOLD)
        aai_benchmarks = aai_mod.make_benchmarks(FILTER_THRESHOLD)

        console.print(f"  Ingest [{aai_mod.NAME}]...")
        t, m = await measure_async(
            lambda d: aai_mod.convert(d, FILTER_THRESHOLD),
            raw_data,
            num_runs,
        )
        results["Ingest"]["aaiclick"] = {"time": t, "memory": m}

        for bench_name in BENCH_NAMES:
            if bench_name == "Ingest" or bench_name not in aai_benchmarks:
                continue
            console.print(f"  {bench_name} [{aai_mod.NAME}]...")
            t, m = await measure_async(aai_benchmarks[bench_name], aai_dataset, num_runs)
            results[bench_name]["aaiclick"] = {"time": t, "memory": m}

    print_results(results, BENCH_NAMES, lib_names, num_rows, num_runs)


def main():
    parser = argparse.ArgumentParser(description="chdb vs aaiclick benchmark")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows")
    parser.add_argument("--runs", type=int, default=10, help="Runs per operation")
    args = parser.parse_args()

    # Use in-memory chdb — must be set before any Session is created
    # (including snowflake ID generator). chdb allows only one path per process.
    os.environ["AAICLICK_CH_URL"] = "chdb://:memory:"

    asyncio.run(run(args.rows, args.runs))


if __name__ == "__main__":
    main()
