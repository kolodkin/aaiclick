"""Tests for map() and _map_part() task definition internals."""

from aaiclick.data.object import Object
from aaiclick.orchestration.decorators import TaskFactory, _serialize_value
from aaiclick.orchestration.execution import import_callback
from aaiclick.orchestration.factories import create_task
from aaiclick.orchestration.models import Group, Task
from aaiclick.orchestration.orch_helpers import _map_part, map


async def _dummy_func(row):
    pass


def test_map_creates_group_with_expander(orch_ctx):
    """map() returns a Group containing an expander Task with correct kwargs."""
    obj_task = create_task("mymodule.load_data")

    group = map(cbk=_dummy_func, obj=obj_task, partition=500,
                args=(10,), kwargs={"factor": 2})

    assert isinstance(group, Group)
    expander = group.get_tasks()[0]
    assert isinstance(expander, Task)
    assert expander.entrypoint == "aaiclick.orchestration.orch_helpers._expand_map"
    assert expander.kwargs["partition"] == 500
    assert expander.kwargs["group_id"] == group.id
    assert expander.kwargs["cbk_args"] == [10]
    assert expander.kwargs["cbk_kwargs"]["factor"] == 2
    # obj Task creates upstream ref + dependency
    assert expander.kwargs["obj"]["ref_type"] == "upstream"
    assert expander.kwargs["obj"]["task_id"] == obj_task.id
    assert any(d.previous_id == obj_task.id for d in expander.previous_dependencies)


def test_map_part_creates_task_with_dependencies(orch_ctx):
    """_map_part() returns a Task depending on part and out."""
    part_task = create_task("mymodule.load_data")
    out_task = create_task("mymodule.output")

    result = _map_part(cbk=_dummy_func, part=part_task, out=out_task)

    assert isinstance(result, Task)
    assert result.entrypoint == "aaiclick.orchestration.orch_helpers._map_part"
    dep_ids = {d.previous_id for d in result.previous_dependencies}
    assert part_task.id in dep_ids
    assert out_task.id in dep_ids


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
    group = map(cbk=_dummy_func, obj=obj_task, kwargs={"lookup": lookup})
    expander = group.get_tasks()[0]
    assert expander.kwargs["cbk_kwargs"]["lookup"]["object_type"] == "object"
    assert expander.kwargs["cbk_kwargs"]["lookup"]["table"] == "t_lookup"

    # Task in args creates dependency
    group2 = map(cbk=_dummy_func, obj=obj_task, args=(extra_task,))
    expander2 = group2.get_tasks()[0]
    dep_ids = {d.previous_id for d in expander2.previous_dependencies}
    assert obj_task.id in dep_ids
    assert extra_task.id in dep_ids
