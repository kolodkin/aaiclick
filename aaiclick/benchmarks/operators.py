"""Operator benchmarks measuring aaiclick latency and overhead."""

from __future__ import annotations

import asyncio
import os
import platform
import subprocess
import timeit
from dataclasses import dataclass

from aaiclick import DataContext, create_object_from_value
from aaiclick.data.models import ENGINE_MEMORY


def detect_os() -> str:
    """Detect the operating system. Returns 'linux', 'macos', or 'unknown'."""
    system = platform.system().lower()
    match system:
        case "linux":
            return "linux"
        case "darwin":
            return "macos"
        case _:
            return "unknown"


def format_bytes(bytes_val: int) -> str:
    """Format bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_val < 1024:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.2f} PB"


def get_ram_bytes() -> int | None:
    """Get total RAM in bytes. Returns None if unable to detect."""
    match detect_os():
        case "linux":
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        return int(line.split()[1]) * 1024
        case "macos":
            result = subprocess.run(
                ["sysctl", "-n", "hw.memsize"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return int(result.stdout.strip())
    return None


def get_machine_specs() -> dict[str, str]:
    """Get machine specifications."""
    specs = {
        "platform": platform.platform(),
        "processor": platform.processor() or "Unknown",
        "cpu_count": str(os.cpu_count() or "Unknown"),
    }

    # RAM
    ram_bytes = get_ram_bytes()
    specs["ram"] = format_bytes(ram_bytes) if ram_bytes else "Unknown"

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
    total_time: float  # seconds - total time including Python overhead
    server_time: float  # seconds - ClickHouse server-side time


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
    total_times = []
    server_times = []

    for _ in range(runs):
        async with DataContext(engine=ENGINE_MEMORY) as ctx:
            client = ctx._ch_client
            obj_a = await create_object_from_value(list(range(size)))
            obj_b = await create_object_from_value(list(range(size)))

            # Collect result table names during timing
            result_tables = []

            # Time only the operators
            start = timeit.default_timer()
            for _ in range(reps):
                result = await (obj_a + obj_b)
                result_tables.append(result._table_name)
            run_total = timeit.default_timer() - start

            # Query server times AFTER timing is complete
            await client.command("SYSTEM FLUSH LOGS")
            conditions = " OR ".join(f"query LIKE '%{t}%SELECT%'" for t in result_tables)
            rows = await client.query(f"""
                SELECT sum(query_duration_ms) / 1000.0
                FROM system.query_log
                WHERE type = 2 AND ({conditions})
            """)
            run_server = rows.result_rows[0][0] if rows.result_rows else 0.0

            total_times.append(run_total / reps)
            server_times.append(run_server / reps)

    total_avg = sum(total_times) / len(total_times)
    server_avg = sum(server_times) / len(server_times)

    return BenchmarkResult(
        operator="add",
        size=size,
        total_time=total_avg,
        server_time=server_avg,
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
    print("Operator Latency Benchmarks")
    print("=" * 70)
    print(
        f"{'Op':<5} {'Size':>10} {'Total':>12} "
        f"{'CH Server':>12} {'Overhead':>12}"
    )
    print("-" * 70)

    for r in results:
        overhead = r.total_time - r.server_time
        print(
            f"{r.operator:<5} {r.size:>10,} {format_time(r.total_time):>12} "
            f"{format_time(r.server_time):>12} {format_time(overhead):>12}"
        )

    print("=" * 70)
    print("Total = CH Server + Overhead (network, table creation, Python async)")
    print()


async def main() -> None:
    """Run benchmarks with default parameters."""
    print_machine_specs()
    results = await run_operator_benchmarks()
    print_benchmark_results(results)


if __name__ == "__main__":
    asyncio.run(main())
