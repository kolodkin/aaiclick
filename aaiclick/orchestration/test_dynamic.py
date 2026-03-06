"""Tests for dynamic task creation operators (map and reduce)."""

from aaiclick.orchestration.decorators import TaskFactory, _serialize_value
from aaiclick.orchestration.dynamic import MapHandle, _get_entrypoint, map, reduce
from aaiclick.orchestration.factories import create_task
from aaiclick.orchestration.models import (
    DEPENDENCY_GROUP,
    DEPENDENCY_TASK,
    Group,
    Task,
    TaskStatus,
)


async def _dummy_func(partition):
    return partition


async def _dummy_reduce(results):
    return results


def test_map_returns_map_handle(orch_ctx):
    """map() returns a MapHandle with expander task and group."""
    obj_task = create_task("mymodule.load_data")

    handle = map(_dummy_func, obj_task, partition_size=500)

    assert isinstance(handle, MapHandle)
    assert isinstance(handle.expander, Task)
    assert isinstance(handle.group, Group)
    assert handle.expander.is_expander is True
    assert handle.expander.status == TaskStatus.PENDING
    assert handle.expander.entrypoint == "aaiclick.orchestration.dynamic._expand_map"


def test_map_expander_kwargs(orch_ctx):
    """map() stores target entrypoint, partition_size, and group_id in expander kwargs."""
    obj_task = create_task("mymodule.load_data")

    handle = map(_dummy_func, obj_task, partition_size=500)

    kwargs = handle.expander.kwargs
    assert kwargs["partition_size"] == 500
    assert kwargs["group_id"] == handle.group.id
    assert "target_entrypoint" in kwargs
    assert "object_ref" in kwargs


def test_map_with_task_dependency(orch_ctx):
    """When obj is a Task, expander depends on it."""
    upstream = create_task("mymodule.load_data")

    handle = map(_dummy_func, upstream, partition_size=1000)

    # Expander should have a dependency on the upstream task
    deps = handle.expander.previous_dependencies
    assert len(deps) == 1
    assert deps[0].previous_id == upstream.id
    assert deps[0].previous_type == DEPENDENCY_TASK
    assert deps[0].next_id == handle.expander.id
    assert deps[0].next_type == DEPENDENCY_TASK


def test_map_with_extra_kwargs(orch_ctx):
    """Extra kwargs are stored in expander for passing to partition tasks."""
    obj_task = create_task("mymodule.load_data")

    handle = map(_dummy_func, obj_task, partition_size=1000, factor=2, mode="fast")

    extra = handle.expander.kwargs["extra_kwargs"]
    assert extra["factor"] == 2
    assert extra["mode"] == "fast"


def test_map_extra_kwargs_task_dependency(orch_ctx):
    """Tasks in extra kwargs create dependencies on the expander."""
    data_task = create_task("mymodule.load_data")
    config_task = create_task("mymodule.load_config")

    handle = map(_dummy_func, data_task, partition_size=1000, config=config_task)

    # Expander depends on both data_task and config_task
    deps = handle.expander.previous_dependencies
    dep_ids = {d.previous_id for d in deps}
    assert data_task.id in dep_ids
    assert config_task.id in dep_ids


def test_map_group_has_unique_id(orch_ctx):
    """Each map() call creates a group with a unique snowflake ID."""
    t = create_task("mymodule.load_data")

    h1 = map(_dummy_func, t, partition_size=1000)
    h2 = map(_dummy_func, t, partition_size=1000)

    assert h1.group.id != h2.group.id
    assert h1.expander.id != h2.expander.id


def test_reduce_returns_task(orch_ctx):
    """reduce() returns a Task that depends on the map group."""
    upstream = create_task("mymodule.load_data")
    mapped = map(_dummy_func, upstream, partition_size=1000)

    result = reduce(_dummy_reduce, mapped)

    assert isinstance(result, Task)
    assert result.status == TaskStatus.PENDING
    assert result.entrypoint == "aaiclick.orchestration.dynamic._execute_reduce"


def test_reduce_depends_on_map_group(orch_ctx):
    """reduce() task depends on the map group (waits for all partitions)."""
    upstream = create_task("mymodule.load_data")
    mapped = map(_dummy_func, upstream, partition_size=1000)

    result = reduce(_dummy_reduce, mapped)

    deps = result.previous_dependencies
    assert len(deps) == 1
    assert deps[0].previous_id == mapped.group.id
    assert deps[0].previous_type == DEPENDENCY_GROUP
    assert deps[0].next_id == result.id
    assert deps[0].next_type == DEPENDENCY_TASK


def test_reduce_kwargs_contain_group_id(orch_ctx):
    """reduce() task kwargs contain the group_id for result collection."""
    upstream = create_task("mymodule.load_data")
    mapped = map(_dummy_func, upstream, partition_size=1000)

    result = reduce(_dummy_reduce, mapped)

    assert result.kwargs["group_id"] == mapped.group.id
    assert "target_entrypoint" in result.kwargs


def test_map_handle_rshift_operator(orch_ctx):
    """MapHandle >> Task creates dependency on the map group."""
    upstream = create_task("mymodule.load_data")
    mapped = map(_dummy_func, upstream, partition_size=1000)
    downstream = create_task("mymodule.process")

    mapped >> downstream

    deps = downstream.previous_dependencies
    assert len(deps) == 1
    assert deps[0].previous_id == mapped.group.id
    assert deps[0].previous_type == DEPENDENCY_GROUP


def test_map_handle_depends_on(orch_ctx):
    """MapHandle.depends_on() makes the expander depend on a task."""
    upstream = create_task("mymodule.load_data")
    mapped = map(_dummy_func, upstream, partition_size=1000)
    extra_dep = create_task("mymodule.extra")

    mapped.depends_on(extra_dep)

    dep_ids = {d.previous_id for d in mapped.expander.previous_dependencies}
    assert extra_dep.id in dep_ids


def test_chained_map_reduce(orch_ctx):
    """map() followed by reduce() creates proper dependency chain."""
    data = create_task("mymodule.load")

    mapped = map(_dummy_func, data, partition_size=100)
    result = reduce(_dummy_reduce, mapped)

    # data >> expander (via map)
    expander_deps = mapped.expander.previous_dependencies
    assert any(d.previous_id == data.id for d in expander_deps)

    # map_group >> reduce (via reduce)
    reduce_deps = result.previous_dependencies
    assert any(d.previous_id == mapped.group.id for d in reduce_deps)


def test_serialize_value_handles_map_handle(orch_ctx):
    """_serialize_value() serializes MapHandle as group_results reference."""
    upstream = create_task("mymodule.load_data")
    mapped = map(_dummy_func, upstream, partition_size=1000)

    serialized = _serialize_value(mapped)

    assert serialized == {"ref_type": "group_results", "group_id": mapped.group.id}


def test_get_entrypoint_with_callable(orch_ctx):
    """_get_entrypoint resolves callable to module.function string."""
    entrypoint = _get_entrypoint(_dummy_func)
    assert entrypoint.endswith("_dummy_func")


def test_get_entrypoint_with_task_factory(orch_ctx):
    """_get_entrypoint resolves TaskFactory to its entrypoint."""
    factory = TaskFactory(_dummy_func)
    entrypoint = _get_entrypoint(factory)
    assert entrypoint == factory.entrypoint


def test_map_with_map_handle_dependency(orch_ctx):
    """Chaining map() calls: second map depends on first map's group."""
    data = create_task("mymodule.load")
    first_map = map(_dummy_func, data, partition_size=100)
    second_map = map(_dummy_func, first_map, partition_size=50)

    # Second expander depends on first map's group
    deps = second_map.expander.previous_dependencies
    assert any(
        d.previous_id == first_map.group.id and d.previous_type == DEPENDENCY_GROUP
        for d in deps
    )
