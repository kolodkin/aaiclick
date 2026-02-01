"""Operator benchmarks comparing aaiclick Objects vs numpy arrays."""

from __future__ import annotations

import asyncio
import os
import platform
import timeit
from dataclasses import dataclass

import numpy as np

from aaiclick import DataContext, create_object_from_value


def format_bytes(bytes_val: int) -> str:
    """Format bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_val < 1024:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.2f} PB"


def get_machine_specs() -> dict[str, str]:
    """Get machine specifications."""
    specs = {
        "platform": platform.platform(),
        "processor": platform.processor() or "Unknown",
        "cpu_count": str(os.cpu_count() or "Unknown"),
    }

    # RAM (Linux)
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    specs["ram"] = format_bytes(kb * 1024)
                    break
    except (FileNotFoundError, PermissionError):
        specs["ram"] = "Unknown"

    # Disk (current directory)
    try:
        stat = os.statvfs(".")
        total = stat.f_blocks * stat.f_frsize
        free = stat.f_bavail * stat.f_frsize
        specs["disk_total"] = format_bytes(total)
        specs["disk_free"] = format_bytes(free)
    except (OSError, AttributeError):
        specs["disk_total"] = "Unknown"
        specs["disk_free"] = "Unknown"

    return specs


def print_machine_specs() -> None:
    """Print machine specifications."""
    specs = get_machine_specs()
    print("=" * 70)
    print("Machine Specifications")
    print("=" * 70)
    print(f"Platform:    {specs['platform']}")
    print(f"Processor:   {specs['processor']}")
    print(f"CPU Count:   {specs['cpu_count']}")
    print(f"RAM:         {specs['ram']}")
    print(f"Disk Total:  {specs['disk_total']}")
    print(f"Disk Free:   {specs['disk_free']}")
    print("=" * 70)


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    operator: str
    size: int
    aaiclick_time: float  # seconds
    numpy_time: float  # seconds
    speedup: float  # numpy_time / aaiclick_time (>1 means aaiclick is faster)


def format_time(seconds: float) -> str:
    """Format time in seconds to human-readable string with 2 decimal precision.

    Examples:
        0.0000567 -> "56.70 us"
        0.00234 -> "2.34 ms"
        0.5 -> "500.00 ms"
        1.5 -> "1.50 s"
        100.3 -> "1 min 40.30 s"
    """
    if seconds >= 60:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes} min {remaining_seconds:.2f} s"
    elif seconds >= 1:
        return f"{seconds:.2f} s"
    elif seconds >= 0.001:
        return f"{seconds * 1000:.2f} ms"
    else:
        return f"{seconds * 1_000_000:.2f} us"


async def run_add_benchmark(
    size: int,
    reps: int = 10,
    runs: int = 5,
) -> BenchmarkResult:
    """Run addition benchmark for a specific size.

    Args:
        size: Number of elements in the arrays
        reps: Number of repetitions per run for timing
        runs: Number of runs to average

    Returns:
        BenchmarkResult with timing data
    """
    # Create numpy arrays (outside timing)
    np_a = np.arange(size, dtype=np.float64)
    np_b = np.arange(size, dtype=np.float64) * 2

    # Run numpy benchmark using timeit
    numpy_times = timeit.repeat(lambda: np_a + np_b, number=reps, repeat=runs)
    numpy_avg = sum(numpy_times) / len(numpy_times) / reps

    # Run aaiclick benchmark (timeit doesn't support async natively)
    aaiclick_times = []
    for _ in range(runs):
        async with DataContext():
            obj_a = await create_object_from_value(np_a.tolist())
            obj_b = await create_object_from_value(np_b.tolist())

            start = timeit.default_timer()
            for _ in range(reps):
                await (obj_a + obj_b)
            elapsed = timeit.default_timer() - start
            aaiclick_times.append(elapsed / reps)

    aaiclick_avg = sum(aaiclick_times) / len(aaiclick_times)

    speedup = numpy_avg / aaiclick_avg if aaiclick_avg > 0 else 0

    return BenchmarkResult(
        operator="add",
        size=size,
        aaiclick_time=aaiclick_avg,
        numpy_time=numpy_avg,
        speedup=speedup,
    )


async def run_operator_benchmarks(
    sizes: list[int] | None = None,
    reps: int = 10,
    runs: int = 5,
) -> list[BenchmarkResult]:
    """Run all operator benchmarks.

    Args:
        sizes: List of array sizes to benchmark. Defaults to [100, 10000].
        reps: Number of repetitions per run for timing. Defaults to 10.
        runs: Number of runs to average. Defaults to 5.

    Returns:
        List of BenchmarkResult objects
    """
    if sizes is None:
        sizes = [100, 10000]

    results = []
    for size in sizes:
        result = await run_add_benchmark(size=size, reps=reps, runs=runs)
        results.append(result)

    return results


def print_benchmark_results(results: list[BenchmarkResult]) -> None:
    """Print benchmark results in a formatted table."""
    print("\n" + "=" * 70)
    print("Operator Benchmark Results: aaiclick vs numpy")
    print("=" * 70)
    print(
        f"{'Operator':<10} {'Size':>10} {'aaiclick':>15} "
        f"{'numpy':>15} {'Speedup':>10}"
    )
    print("-" * 70)

    for r in results:
        print(
            f"{r.operator:<10} {r.size:>10} {format_time(r.aaiclick_time):>15} "
            f"{format_time(r.numpy_time):>15} {r.speedup:>9.2f}x"
        )

    print("=" * 70)
    print("Note: Speedup > 1 means aaiclick is faster")
    print()


async def main() -> None:
    """Run benchmarks with default parameters."""
    print_machine_specs()
    results = await run_operator_benchmarks()
    print_benchmark_results(results)


if __name__ == "__main__":
    asyncio.run(main())
