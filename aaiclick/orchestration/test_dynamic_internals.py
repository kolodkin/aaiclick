"""Tests for map() and _map_part() task definition internals."""

from aaiclick.data.object import Object
from aaiclick.orchestration.decorators import TaskFactory, _serialize_value
from aaiclick.orchestration.execution.runner import import_callback
from aaiclick.orchestration.factories import create_task
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.operators import map


async def _dummy_func(row):
    pass


def test_map_returns_expander(orch_ctx):
    """map() returns the expander Task with correct kwargs."""
    obj_task = create_task("mymodule.load_data")

    expander = map(cbk=_dummy_func, obj=obj_task, partition=500, args=(10,), kwargs={"factor": 2})

    assert isinstance(expander, Task)
    assert expander.entrypoint == "aaiclick.orchestration.operators._expand_map"
    assert expander.kwargs["partition"] == 500
    assert expander.kwargs["cbk_args"] == [10]
    assert expander.kwargs["cbk_kwargs"]["factor"] == 2
    # obj Task creates upstream ref + dependency
    assert expander.kwargs["obj"]["ref_type"] == "upstream"
    assert expander.kwargs["obj"]["task_id"] == obj_task.id
    assert any(d.previous_id == obj_task.id for d in expander.previous_dependencies)


def test_serialize_callable_roundtrip(orch_ctx):
    """Callable and TaskFactory serialize to callable refs and roundtrip via import_callback."""
    # Plain callable
    serialized = _serialize_value(_dummy_func)
    assert serialized["ref_type"] == "callable"
    assert import_callback(serialized["entrypoint"]) is _dummy_func

    # TaskFactory
    factory = TaskFactory(_dummy_func, name="_dummy_func")
    serialized = _serialize_value(factory)
    assert serialized["ref_type"] == "callable"
    assert import_callback(serialized["entrypoint"]) is _dummy_func


def test_map_args_with_object_and_task(orch_ctx):
    """Object in args/kwargs is serialized; Task in args creates dependency."""
    obj_task = create_task("mymodule.load_data")
    extra_task = create_task("mymodule.extra")
    lookup = Object(table="t_lookup")

    # Object in kwargs
    expander = map(cbk=_dummy_func, obj=obj_task, kwargs={"lookup": lookup})
    assert expander.kwargs["cbk_kwargs"]["lookup"]["object_type"] == "object"
    assert expander.kwargs["cbk_kwargs"]["lookup"]["table"] == "t_lookup"

    # Task in args creates dependency
    expander2 = map(cbk=_dummy_func, obj=obj_task, args=(extra_task,))
    dep_ids = {d.previous_id for d in expander2.previous_dependencies}
    assert obj_task.id in dep_ids
    assert extra_task.id in dep_ids


def test_same_upstream_in_two_kwargs_wires_one_dependency(orch_ctx):
    """One Dependency row per upstream: the composite PK would reject a duplicate on commit."""
    factory = TaskFactory(_dummy_func, name="_dummy_func")
    upstream = create_task("mymodule.producer")

    consumer = factory(left=upstream, right=[upstream, {"nested": upstream}])

    assert [d.previous_id for d in consumer.previous_dependencies] == [upstream.id]
