"""Benchmark runner — generates data, measures chdb vs aaiclick, prints report.

Each library defines a context() (context manager wrapping convert + benchmarks)
so chdb releases its in-memory engine before aaiclick claims its own.

Usage:
    python -m aaiclick.example_projects.chdb_benchmark
    python -m aaiclick.example_projects.chdb_benchmark --rows 500000 --runs 5
"""

import argparse
import asyncio
import random
import time
import tracemalloc

from . import bench_aaiclick, bench_chdb_native

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


def fmt_time(seconds):
    if seconds >= 1:
        return f"{seconds:.2f} sec"
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.2f} ms"
    if seconds >= 1e-6:
        return f"{seconds * 1e6:.2f} us"
    return f"{seconds * 1e9:.2f} ns"


def fmt_mem(mem_bytes):
    if mem_bytes < 1024:
        return f"{mem_bytes}B"
    if mem_bytes < 1024 * 1024:
        return f"{mem_bytes / 1024:.1f}KB"
    return f"{mem_bytes / (1024 * 1024):.1f}MB"


async def bench_chdb(raw_data, num_runs, results):
    """Run all benchmarks for native chdb."""
    mod = bench_chdb_native
    benchmarks = mod.make_benchmarks(FILTER_THRESHOLD)

    with mod.context():
        # Ingest
        print(f"  Ingest [{mod.NAME}]...")
        avg_time, peak_mem = measure_sync(
            lambda d: mod.convert(d, FILTER_THRESHOLD), raw_data, num_runs,
        )
        results["Ingest"][mod.NAME] = {"time": avg_time, "memory": peak_mem}

        dataset = mod.convert(raw_data, FILTER_THRESHOLD)

        for bench_name, fn in benchmarks.items():
            print(f"  {bench_name} [{mod.NAME}]...")
            avg_time, peak_mem = measure_sync(fn, dataset, num_runs)
            results[bench_name][mod.NAME] = {"time": avg_time, "memory": peak_mem}


async def bench_aai(raw_data, num_runs, results):
    """Run all benchmarks for aaiclick."""
    mod = bench_aaiclick
    benchmarks = mod.make_benchmarks(FILTER_THRESHOLD)

    async with mod.context():
        # Ingest
        print(f"  Ingest [{mod.NAME}]...")
        avg_time, peak_mem = await measure_async(
            lambda d: mod.convert(d, FILTER_THRESHOLD), raw_data, num_runs,
        )
        results["Ingest"][mod.NAME] = {"time": avg_time, "memory": peak_mem}

        dataset = await mod.convert(raw_data, FILTER_THRESHOLD)

        for bench_name, fn in benchmarks.items():
            print(f"  {bench_name} [{mod.NAME}]...")
            avg_time, peak_mem = await measure_async(fn, dataset, num_runs)
            results[bench_name][mod.NAME] = {"time": avg_time, "memory": peak_mem}


def print_results(results, lib_names, num_rows, num_runs):
    """Print benchmark results as markdown tables."""
    print(f"\n## Benchmark Results — {num_rows:,} rows, {num_runs} runs\n")

    # Timing table
    header = "| Operation | " + " | ".join(lib_names) + " | Overhead |"
    sep = "|" + "|".join(["-" * 22] + ["-" * 14] * len(lib_names) + ["-" * 14]) + "|"
    print(header)
    print(sep)

    for bench_name in BENCH_NAMES:
        lib_results = results.get(bench_name, {})
        row = [f" {bench_name:<20} "]
        times = {}
        for lib in lib_names:
            if lib in lib_results:
                t = lib_results[lib]["time"]
                times[lib] = t
                row.append(f" {fmt_time(t):>12} ")
            else:
                row.append(f" {'—':>12} ")

        # Overhead column: aaiclick time / chdb time
        if "chdb" in times and "aaiclick" in times and times["chdb"] > 0:
            ratio = times["aaiclick"] / times["chdb"]
            if ratio >= 1:
                row.append(f" {'x%.1f slower' % ratio:>12} ")
            else:
                row.append(f" {'x%.1f faster' % (1 / ratio):>12} ")
        else:
            row.append(f" {'—':>12} ")

        print("|" + "|".join(row) + "|")

    # Memory table
    print(f"\n### Peak Memory\n")
    header = "| Operation | " + " | ".join(lib_names) + " |"
    sep = "|" + "|".join(["-" * 22] + ["-" * 14] * len(lib_names)) + "|"
    print(header)
    print(sep)

    for bench_name in BENCH_NAMES:
        lib_results = results.get(bench_name, {})
        row = [f" {bench_name:<20} "]
        for lib in lib_names:
            if lib in lib_results:
                row.append(f" {fmt_mem(lib_results[lib]['memory']):>12} ")
            else:
                row.append(f" {'—':>12} ")
        print("|" + "|".join(row) + "|")


async def run(num_rows, num_runs):
    lib_names = [bench_chdb_native.NAME, bench_aaiclick.NAME]
    versions = [
        f"{bench_chdb_native.NAME} {bench_chdb_native.VERSION}",
        f"{bench_aaiclick.NAME} {bench_aaiclick.VERSION}",
    ]

    print(f"## chdb vs aaiclick Benchmark\n")
    print(f"- Libraries: {', '.join(versions)}")
    print(f"- Data: {num_rows:,} rows, {num_runs} runs per operation")
    print(f"- Filter threshold: {FILTER_THRESHOLD}")
    print(f"- Categories: {len(CATEGORIES)}, Subcategories: {len(SUBCATEGORIES)}\n")

    raw_data = generate_raw_data(num_rows)
    results = {name: {} for name in BENCH_NAMES}

    print("### Running benchmarks...\n")
    await bench_chdb(raw_data, num_runs, results)
    await bench_aai(raw_data, num_runs, results)

    print_results(results, lib_names, num_rows, num_runs)

    # Analysis section
    print("\n### Performance Analysis\n")
    print("**Sources of aaiclick overhead vs native chdb:**\n")
    print("- **Ingest**: aaiclick uses `ch.insert()` (column-oriented binary protocol)")
    print("  vs chdb's `INSERT...SELECT FROM Python(arrow_table)` (PyArrow zero-copy)")
    print("- **Column sum**: aaiclick issues 2 queries (type lookup + INSERT...SELECT)")
    print("  vs chdb's single `SELECT sum()`")
    print("- **Column multiply**: aaiclick creates an intermediate result table via")
    print("  INNER JOIN on row_number(); chdb computes inline with `FORMAT Null`")
    print("- **Filter/Sort**: aaiclick materializes results via `copy()` (INSERT...SELECT);")
    print("  chdb uses `FORMAT Null` to skip serialization entirely")
    print("- **Count distinct**: aaiclick chains unique() + count() = 4 queries;")
    print("  chdb uses single `SELECT count() FROM (... GROUP BY ...)`")
    print("- **Group-by**: aaiclick adds CREATE TABLE + INSERT overhead per query;")
    print("  chdb also lacks `optimize_aggregation_in_order` (no MergeTree ORDER BY)")
    print("- **Multi-agg**: aaiclick `.agg()` uses a single GROUP BY with all aggregates,")
    print("  matching chdb's pattern (no per-aggregate overhead)")


def main():
    parser = argparse.ArgumentParser(description="chdb vs aaiclick benchmark")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows")
    parser.add_argument("--runs", type=int, default=10, help="Runs per operation")
    args = parser.parse_args()
    asyncio.run(run(args.rows, args.runs))


if __name__ == "__main__":
    main()
