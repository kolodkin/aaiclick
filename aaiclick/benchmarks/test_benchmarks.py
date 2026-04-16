"""Tests for operator benchmarks."""

from aaiclick.benchmarks.operators import BenchmarkResult, run_operator_benchmarks


async def test_run_operator_benchmarks():
    """Test running all operator benchmarks with small sizes."""
    results = await run_operator_benchmarks(sizes=[10, 20], reps=2, runs=2)

    assert len(results) == 2
    assert results[0].size == 10
    assert results[1].size == 20

    for result in results:
        assert isinstance(result, BenchmarkResult)
        assert result.operator == "add"
        assert result.total_time > 0
        assert result.server_time >= 0
