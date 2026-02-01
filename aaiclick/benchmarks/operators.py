"""Operator benchmarks comparing aaiclick Objects vs numpy arrays."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import numpy as np

from aaiclick import DataContext, create_object_from_value


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    operator: str
    size: int
    aaiclick_time_ms: float
    numpy_time_ms: float
    speedup: float  # numpy_time / aaiclick_time (>1 means aaiclick is faster)


async def benchmark_add_aaiclick(obj_a, obj_b, reps: int) -> float:
    """Benchmark addition operator for aaiclick Objects.

    Returns average time in milliseconds.
    """
    times = []
    for _ in range(reps):
        start = time.perf_counter()
        _ = await (obj_a + obj_b)
        end = time.perf_counter()
        times.append((end - start) * 1000)
    return sum(times) / len(times)


def benchmark_add_numpy(arr_a: np.ndarray, arr_b: np.ndarray, reps: int) -> float:
    """Benchmark addition operator for numpy arrays.

    Returns average time in milliseconds.
    """
    times = []
    for _ in range(reps):
        start = time.perf_counter()
        _ = arr_a + arr_b
        end = time.perf_counter()
        times.append((end - start) * 1000)
    return sum(times) / len(times)


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

    # Run numpy benchmark (multiple runs, average)
    numpy_times = []
    for _ in range(runs):
        numpy_times.append(benchmark_add_numpy(np_a, np_b, reps))
    numpy_avg = sum(numpy_times) / len(numpy_times)

    # Run aaiclick benchmark (multiple runs, average)
    aaiclick_times = []
    for _ in range(runs):
        async with DataContext():
            # Create objects (outside timing)
            obj_a = await create_object_from_value(np_a.tolist())
            obj_b = await create_object_from_value(np_b.tolist())

            # Benchmark the operator
            aaiclick_times.append(await benchmark_add_aaiclick(obj_a, obj_b, reps))

    aaiclick_avg = sum(aaiclick_times) / len(aaiclick_times)

    # Calculate speedup (>1 means aaiclick is faster)
    speedup = numpy_avg / aaiclick_avg if aaiclick_avg > 0 else 0

    return BenchmarkResult(
        operator="add",
        size=size,
        aaiclick_time_ms=aaiclick_avg,
        numpy_time_ms=numpy_avg,
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
        f"{'Operator':<10} {'Size':>10} {'aaiclick (ms)':>15} "
        f"{'numpy (ms)':>15} {'Speedup':>10}"
    )
    print("-" * 70)

    for r in results:
        speedup_str = f"{r.speedup:.2f}x"
        print(
            f"{r.operator:<10} {r.size:>10} {r.aaiclick_time_ms:>15.3f} "
            f"{r.numpy_time_ms:>15.3f} {speedup_str:>10}"
        )

    print("=" * 70)
    print("Note: Speedup > 1 means aaiclick is faster")
    print()


async def main() -> None:
    """Run benchmarks with default parameters."""
    results = await run_operator_benchmarks()
    print_benchmark_results(results)


if __name__ == "__main__":
    asyncio.run(main())
