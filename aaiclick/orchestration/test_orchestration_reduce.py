"""Execution tests for reduce() via ajob_test."""

import tempfile

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration.debug_execution import ajob_test
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.models import JobStatus
from aaiclick.orchestration.orch_helpers import reduce


# --- Reduction callbacks (module-level for entrypoint resolution) ---


@task
async def sum_reduce(partition: Object, output: Object) -> None:
    """Sum all values in the partition and insert result into output."""
    values = await partition.data()
    await output.insert(int(sum(values)))


# --- Job pipelines (module-level for entrypoint resolution) ---


@task
async def create_test_object(values: list) -> Object:
    return await create_object_from_value(values)


@job("test_reduce_single_layer")
def reduce_single_layer(values: list):
    data = create_test_object(values=values)
    reduced = reduce(sum_reduce, data, partition=100)
    return [data, reduced]


@job("test_reduce_multi_layer")
def reduce_multi_layer(values: list, partition_size: int):
    data = create_test_object(values=values)
    reduced = reduce(sum_reduce, data, partition=partition_size)
    return [data, reduced]


@job("test_reduce_with_init")
def reduce_with_initializer(values: list):
    data = create_test_object(values=values)
    reduced = reduce(sum_reduce, data, initializer=0, partition=100)
    return [data, reduced]


@job("test_reduce_empty")
def reduce_empty():
    data = create_test_object(values=[])
    reduced = reduce(sum_reduce, data, partition=100)
    return [data, reduced]


@job("test_reduce_single_row")
def reduce_single_row():
    data = create_test_object(values=[42])
    reduced = reduce(sum_reduce, data, partition=100)
    return [data, reduced]


# --- Tests ---


async def test_reduce_single_layer(orch_ctx, monkeypatch):
    """reduce() with all rows in one partition produces correct sum."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        j = await reduce_single_layer(values=[1, 2, 3, 4, 5])
        await ajob_test(j)

        assert j.status == JobStatus.COMPLETED, f"Job failed: {j.error}"


async def test_reduce_multi_layer(orch_ctx, monkeypatch):
    """reduce() with partition=2 creates multiple layers for [1,2,3,4,5]."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        # partition=2: layer 0 has ceil(5/2)=3 tasks, layer 1 has ceil(3/2)=2,
        # layer 2 has ceil(2/2)=1 → 3 layers total
        j = await reduce_multi_layer(values=[1, 2, 3, 4, 5], partition_size=2)
        await ajob_test(j)

        assert j.status == JobStatus.COMPLETED, f"Job failed: {j.error}"


async def test_reduce_with_initializer(orch_ctx, monkeypatch):
    """reduce() with initializer=0 prepends a zero before reduction."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        j = await reduce_with_initializer(values=[10, 20, 30])
        await ajob_test(j)

        assert j.status == JobStatus.COMPLETED, f"Job failed: {j.error}"


async def test_reduce_empty_raises(orch_ctx, monkeypatch):
    """reduce() of empty Object without initializer fails with TypeError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        j = await reduce_empty()
        await ajob_test(j)

        assert j.status == JobStatus.FAILED
        assert "reduce() of empty sequence" in (j.error or "")


async def test_reduce_single_row(orch_ctx, monkeypatch):
    """reduce() of a single-row Object returns that row unchanged."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        j = await reduce_single_row()
        await ajob_test(j)

        assert j.status == JobStatus.COMPLETED, f"Job failed: {j.error}"


async def test_reduce_native_api(orch_ctx, monkeypatch):
    """reduce() with native API callback (partition.data() + sum()) completes successfully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)

        j = await reduce_single_layer(values=[10, 20, 30, 40])
        await ajob_test(j)

        assert j.status == JobStatus.COMPLETED, f"Job failed: {j.error}"
