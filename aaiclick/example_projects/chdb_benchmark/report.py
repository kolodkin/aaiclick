"""Pretty-print benchmark results as rich tables."""

from rich.console import Console
from rich.table import Table

console = Console(width=200)


def fmt_time(seconds):
    """Format time with full unit name and .2 precision."""
    if seconds >= 3600:
        return f"{seconds / 3600:.2f} hour"
    if seconds >= 60:
        return f"{seconds / 60:.2f} min"
    if seconds >= 1:
        return f"{seconds:.2f} sec"
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.2f} ms"
    if seconds >= 1e-6:
        return f"{seconds * 1e6:.2f} us"
    return f"{seconds * 1e9:.2f} ns"


def fmt_mem(mem_bytes):
    """Format memory with appropriate unit."""
    if mem_bytes < 1024:
        return f"{mem_bytes}B"
    if mem_bytes < 1024 * 1024:
        return f"{mem_bytes / 1024:.1f}KB"
    return f"{mem_bytes / (1024 * 1024):.1f}MB"


def print_results(results, bench_names, lib_names, num_rows, num_runs):
    """Print timing and memory tables using rich."""
    table = Table(title=f"chdb vs aaiclick Benchmark — {num_rows:,} rows, {num_runs} runs")
    table.add_column("Operation", style="bold", no_wrap=True)
    for lib in lib_names:
        table.add_column(lib, justify="right", no_wrap=True)
    table.add_column("Fastest", justify="right", style="green", no_wrap=True)

    for bench_name in bench_names:
        lib_results = results.get(bench_name, {})
        row = [bench_name]
        times = {}
        for lib in lib_names:
            if lib in lib_results:
                t = lib_results[lib]["time"]
                times[lib] = t
                row.append(fmt_time(t))
            else:
                row.append("—")

        if len(times) >= 2:
            fastest_lib = min(times, key=times.get)
            slowest_time = max(times.values())
            fastest_time = times[fastest_lib]
            if fastest_time > 0 and fastest_time < slowest_time:
                speedup = slowest_time / fastest_time
                row.append(f"{fastest_lib} ~x{speedup:.1f}")
            else:
                row.append(fastest_lib)
        else:
            row.append("—")

        table.add_row(*row)

    console.print()
    console.print(table)

    mem_table = Table(title="Peak Memory")
    mem_table.add_column("Operation", style="bold", no_wrap=True)
    for lib in lib_names:
        mem_table.add_column(lib, justify="right", no_wrap=True)

    for bench_name in bench_names:
        lib_results = results.get(bench_name, {})
        row = [bench_name]
        for lib in lib_names:
            if lib in lib_results:
                row.append(fmt_mem(lib_results[lib]["memory"]))
            else:
                row.append("—")
        mem_table.add_row(*row)

    console.print()
    console.print(mem_table)
