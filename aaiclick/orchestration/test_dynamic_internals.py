"""Tests for dynamic task definition internals (map and _map_part)."""

from aaiclick.data.object import Object
from aaiclick.orchestration.decorators import TaskFactory, _serialize_value
from aaiclick.orchestration.execution import _extract_task_items, import_callback
from aaiclick.orchestration.factories import create_task
from aaiclick.orchestration.models import (
    DEPENDENCY_TASK,
    Group,
    Task,
)
from aaiclick.orchestration.orch_helpers import _map_part, map


async def _dummy_func(row):
    pass


def test_map_returns_group(orch_ctx):
    """map() returns a Group."""
    obj_task = create_task("mymodule.load_data")

    result = map(cbk=_dummy_func, obj=obj_task, partition=500)

    assert isinstance(result, Group)
    assert result.name == "map"


def test_map_group_carries_expander(orch_ctx):
    """map() Group carries an expander Task."""
    obj_task = create_task("mymodule.load_data")

    group = map(cbk=_dummy_func, obj=obj_task, partition=500)

    tasks = group.get_tasks()
    assert len(tasks) == 1
    expander = tasks[0]
    assert isinstance(expander, Task)
    assert expander.entrypoint == "aaiclick.orchestration.orch_helpers._expand_map"


def test_map_expander_kwargs(orch_ctx):
    """Expander task stores cbk, obj, partition, group_id, cbk_args, cbk_kwargs."""
    obj_task = create_task("mymodule.load_data")

    group = map(cbk=_dummy_func, obj=obj_task, partition=500)

    expander = group.get_tasks()[0]
    kwargs = expander.kwargs
    assert kwargs["partition"] == 500
    assert kwargs["group_id"] == group.id
    assert kwargs["cbk"]["ref_type"] == "callable"
    assert kwargs["cbk"]["entrypoint"].endswith("_dummy_func")
    assert kwargs["obj"]["ref_type"] == "upstream"
    assert kwargs["obj"]["task_id"] == obj_task.id
    assert kwargs["cbk_args"] == []
    assert kwargs["cbk_kwargs"] == {}


def test_map_with_args_kwargs(orch_ctx):
    """args and kwargs are serialized in the expander task."""
    obj_task = create_task("mymodule.load_data")

    group = map(
        cbk=_dummy_func, obj=obj_task, partition=500,
        args=(10,), kwargs={"factor": 2, "mode": "fast"},
    )

    expander = group.get_tasks()[0]
    assert expander.kwargs["cbk_args"] == [10]
    assert expander.kwargs["cbk_kwargs"]["factor"] == 2
    assert expander.kwargs["cbk_kwargs"]["mode"] == "fast"


def test_map_expander_dependency(orch_ctx):
    """When obj is a Task, expander task depends on it."""
    upstream = create_task("mymodule.load_data")

    group = map(cbk=_dummy_func, obj=upstream, partition=1000)

    expander = group.get_tasks()[0]
    deps = expander.previous_dependencies
    assert len(deps) == 1
    assert deps[0].previous_id == upstream.id
    assert deps[0].previous_type == DEPENDENCY_TASK


def test_map_unique_ids(orch_ctx):
    """Each map() call creates a unique Group."""
    t = create_task("mymodule.load_data")

    g1 = map(cbk=_dummy_func, obj=t, partition=1000)
    g2 = map(cbk=_dummy_func, obj=t, partition=1000)

    assert g1.id != g2.id


def test_map_extract_task_items(orch_ctx):
    """_extract_task_items flattens Group with attached tasks."""
    obj_task = create_task("mymodule.load_data")

    group = map(cbk=_dummy_func, obj=obj_task, partition=500)

    items, data = _extract_task_items(group)
    assert data is None
    assert len(items) == 2  # Group + expander Task
    assert items[0] is group
    assert isinstance(items[1], Task)


def test__map_part_returns_task(orch_ctx):
    """_map_part() returns a Task."""
    obj_task = create_task("mymodule.load_data")

    result = _map_part(cbk=_dummy_func, part=obj_task, out=obj_task)

    assert isinstance(result, Task)
    assert result.entrypoint == "aaiclick.orchestration.orch_helpers._map_part"


def test_serialize_callable(orch_ctx):
    """_serialize_value() serializes a callable as a callable ref."""
    serialized = _serialize_value(_dummy_func)

    assert serialized["ref_type"] == "callable"
    assert serialized["entrypoint"].endswith("_dummy_func")


def test_serialize_task_factory_callable(orch_ctx):
    """_serialize_value() serializes a TaskFactory using its entrypoint."""
    factory = TaskFactory(_dummy_func, name="_dummy_func")

    serialized = _serialize_value(factory)

    assert serialized["ref_type"] == "callable"
    assert serialized["entrypoint"] == factory.entrypoint


def test_callable_roundtrip(orch_ctx):
    """Callable serialized via _serialize_value can be deserialized via import_callback."""
    serialized = _serialize_value(_dummy_func)
    restored = import_callback(serialized["entrypoint"])

    assert restored is _dummy_func


def test_task_factory_callable_roundtrip(orch_ctx):
    """TaskFactory serialized as callable can be roundtripped via import_callback."""
    factory = TaskFactory(_dummy_func, name="_dummy_func")
    serialized = _serialize_value(factory)
    restored = import_callback(serialized["entrypoint"])

    # import_callback unwraps TaskFactory to get the original function
    assert restored is _dummy_func


def test__map_part_kwargs(orch_ctx):
    """_map_part() stores cbk, part, and out in kwargs."""
    part_task = create_task("mymodule.load_data")
    out_task = create_task("mymodule.output")

    result = _map_part(cbk=_dummy_func, part=part_task, out=out_task)

    kwargs = result.kwargs
    assert kwargs["cbk"]["ref_type"] == "callable"
    assert kwargs["part"]["ref_type"] == "upstream"
    assert kwargs["part"]["task_id"] == part_task.id
    assert kwargs["out"]["ref_type"] == "upstream"
    assert kwargs["out"]["task_id"] == out_task.id


def test__map_part_dependencies(orch_ctx):
    """_map_part() creates dependencies on part and out tasks."""
    part_task = create_task("mymodule.load_data")
    out_task = create_task("mymodule.output")

    result = _map_part(cbk=_dummy_func, part=part_task, out=out_task)

    dep_ids = {d.previous_id for d in result.previous_dependencies}
    assert part_task.id in dep_ids
    assert out_task.id in dep_ids


def test_map_args_with_object(orch_ctx):
    """Object in args is serialized via _serialize_value."""
    obj_task = create_task("mymodule.load_data")
    lookup = Object(table="t_lookup")

    group = map(cbk=_dummy_func, obj=obj_task, args=(lookup,))

    expander = group.get_tasks()[0]
    serialized_args = expander.kwargs["cbk_args"]
    assert len(serialized_args) == 1
    assert serialized_args[0]["object_type"] == "object"
    assert serialized_args[0]["table"] == "t_lookup"


def test_map_kwargs_with_object(orch_ctx):
    """Object in kwargs is serialized via _serialize_value."""
    obj_task = create_task("mymodule.load_data")
    lookup = Object(table="t_lookup")

    group = map(cbk=_dummy_func, obj=obj_task, kwargs={"lookup": lookup})

    expander = group.get_tasks()[0]
    serialized_kwargs = expander.kwargs["cbk_kwargs"]
    assert serialized_kwargs["lookup"]["object_type"] == "object"
    assert serialized_kwargs["lookup"]["table"] == "t_lookup"


def test_map_args_with_task_creates_dependency(orch_ctx):
    """Task in args creates a dependency on the expander."""
    obj_task = create_task("mymodule.load_data")
    extra_task = create_task("mymodule.extra")

    group = map(cbk=_dummy_func, obj=obj_task, args=(extra_task,))

    expander = group.get_tasks()[0]
    dep_ids = {d.previous_id for d in expander.previous_dependencies}
    assert obj_task.id in dep_ids
    assert extra_task.id in dep_ids
