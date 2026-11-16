"""A callable task argument must be importable by path, or the call that passes it fails."""

import functools

import pytest

from aaiclick.data.object_context import create_object_from_value
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.models import JOB_FAILED
from aaiclick.orchestration.operators import foreach, map, reduce


def identity(row, k: int = 1):
    return row


class Holder:
    def method(self, row):
        return row


def _make_nested():
    def nested(row):
        return row

    return nested


UNIMPORTABLE = {
    "lambda": lambda row: row,
    "nested": _make_nested(),
    "bound_method": Holder().method,
    "partial": functools.partial(identity, k=2),
}
CALLS = {
    "kwarg": lambda fn, data: call_fn(fn=fn),
    "map": map,
    "foreach": foreach,
    "reduce": reduce,
}


@task
async def make_data():
    return await create_object_from_value([1, 2, 3], aai_id=True)


@task
async def call_fn(fn) -> int:
    return fn(5)


@job("test_unimportable_callable")
def unimportable_callable_pipeline(kind: str, call: str):
    data = make_data()
    return [data, CALLS[call](UNIMPORTABLE[kind], data)]


@pytest.mark.parametrize("kind", list(UNIMPORTABLE))
@pytest.mark.parametrize(
    "call, arg",
    [
        pytest.param("kwarg", "fn", id="task_kwarg"),
        pytest.param("map", "cbk", id="map"),
        pytest.param("foreach", "cbk", id="foreach"),
        pytest.param("reduce", "cbk", id="reduce"),
    ],
)
async def test_unimportable_callable_fails_in_the_job_body(orch_ctx, kind, call, arg):
    """The job body that passes the callable fails, naming the argument, not a downstream task on import."""
    j = await ajob_test(unimportable_callable_pipeline, kind=kind, call=call)
    assert j.status == JOB_FAILED
    assert f"Argument '{arg}' got" in j.error
    assert "a module-level function is required" in j.error
