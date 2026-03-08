"""Tests for dynamic task creation operators (map and map_apply)."""

from aaiclick.orchestration.decorators import TaskFactory, _serialize_value
from aaiclick.orchestration.dynamic import map, map_apply
from aaiclick.orchestration.factories import create_task
from aaiclick.orchestration.models import (
    DEPENDENCY_TASK,
    Task,
    TaskStatus,
)


async def _dummy_func(row):
    pass


def test_map_returns_task(orch_ctx):
    """map() returns a Task."""
    obj_task = create_task("mymodule.load_data")

    result = map(cbk=_dummy_func, obj=obj_task, partition=500)

    assert isinstance(result, Task)
    assert result.status == TaskStatus.PENDING
    assert result.entrypoint == "aaiclick.orchestration.dynamic.map"


def test_map_task_kwargs(orch_ctx):
    """map() stores cbk as callable ref, obj as upstream ref, partition as int."""
    obj_task = create_task("mymodule.load_data")

    result = map(cbk=_dummy_func, obj=obj_task, partition=500)

    kwargs = result.kwargs
    assert kwargs["partition"] == 500
    assert kwargs["cbk"]["ref_type"] == "callable"
    assert kwargs["cbk"]["entrypoint"].endswith("_dummy_func")
    assert kwargs["obj"]["ref_type"] == "upstream"
    assert kwargs["obj"]["task_id"] == obj_task.id


def test_map_task_dependency(orch_ctx):
    """When obj is a Task, map task depends on it."""
    upstream = create_task("mymodule.load_data")

    result = map(cbk=_dummy_func, obj=upstream, partition=1000)

    deps = result.previous_dependencies
    assert len(deps) == 1
    assert deps[0].previous_id == upstream.id
    assert deps[0].previous_type == DEPENDENCY_TASK
    assert deps[0].next_id == result.id
    assert deps[0].next_type == DEPENDENCY_TASK


def test_map_unique_ids(orch_ctx):
    """Each map() call creates a unique task."""
    t = create_task("mymodule.load_data")

    t1 = map(cbk=_dummy_func, obj=t, partition=1000)
    t2 = map(cbk=_dummy_func, obj=t, partition=1000)

    assert t1.id != t2.id


def test_map_apply_returns_task(orch_ctx):
    """map_apply() returns a Task."""
    obj_task = create_task("mymodule.load_data")

    result = map_apply(cbk=_dummy_func, part=obj_task, out=obj_task)

    assert isinstance(result, Task)
    assert result.entrypoint == "aaiclick.orchestration.dynamic.map_apply"


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
