"""Tasks that return the JSON form of a dynamic DAG — ``task``, ``group`` and
``task_result`` dicts with local handles — instead of Python ``Task`` objects.

Module-level so a worker can import every child by entrypoint.
"""

from aaiclick.data.object.refs import group_results_ref, native_ref, upstream_ref
from aaiclick.orchestration.decorators import task

_MODULE = "aaiclick.orchestration.fixtures.dict_return_pipelines"


def _task_dict(handle: str, entrypoint: str, kwargs: dict) -> dict:
    return {"_aaiclick_type": "task", "id": handle, "entrypoint": f"{_MODULE}.{entrypoint}", "kwargs": kwargs}


@task
async def double(x: int) -> int:
    return x * 2


@task
async def total(parts: list[int]) -> int:
    return sum(parts)


@task
async def spawn_dicts(n: int) -> dict:
    """Two workers and a finalize consuming both, wired by handles."""
    return {
        "_aaiclick_type": "task_result",
        "data": upstream_ref("fin"),
        "tasks": [
            _task_dict("w1", "double", {"x": n}),
            _task_dict("w2", "double", {"x": n + 1}),
            _task_dict("fin", "total", {"parts": [upstream_ref("w1"), upstream_ref("w2")]}),
        ],
    }


@task
async def spawn_group(n: int) -> dict:
    """A group with a handle, consumed through a ``group_results`` ref."""
    return {
        "_aaiclick_type": "task_result",
        "data": upstream_ref("fin"),
        "tasks": [
            {
                "_aaiclick_type": "group",
                "id": "g",
                "name": "parts",
                "tasks": [_task_dict("w1", "double", {"x": n}), _task_dict("w2", "double", {"x": n + 1})],
            },
            _task_dict("fin", "total", {"parts": group_results_ref("g")}),
        ],
    }


@task
async def spawn_duplicate_handle() -> list[dict]:
    return [_task_dict("w1", "double", {"x": 1}), _task_dict("w1", "double", {"x": 2})]


@task
async def spawn_unknown_handle() -> dict:
    return {
        "_aaiclick_type": "task_result",
        "data": None,
        "tasks": [_task_dict("fin", "total", {"parts": [upstream_ref("nope")]})],
    }


@task
async def spawn_raw_task_id() -> dict:
    return {
        "_aaiclick_type": "task_result",
        "data": None,
        "tasks": [_task_dict("fin", "total", {"parts": [upstream_ref(1)]})],
    }


@task
async def spawn_bad_entry_type() -> dict:
    return {"_aaiclick_type": "task", "entry_type": "banana", "entrypoint": f"{_MODULE}.double", "kwargs": {"x": 1}}


@task
async def spawn_native_data() -> dict:
    """``data`` is a plain value next to a child, unwrapped like out.json's top level."""
    return {"_aaiclick_type": "task_result", "data": native_ref(5), "tasks": [_task_dict("w1", "double", {"x": 1})]}


@task
async def spawn_group_data() -> dict:
    """``data`` may name a child task, never a group."""
    return {
        "_aaiclick_type": "task_result",
        "data": group_results_ref("g"),
        "tasks": [{"_aaiclick_type": "group", "id": "g", "name": "g", "tasks": [_task_dict("w1", "double", {"x": 1})]}],
    }


@task
async def spawn_wrong_handle_kind() -> list[dict]:
    """A task handle where a group handle is expected."""
    return [_task_dict("w1", "double", {"x": 1}), _task_dict("fin", "total", {"parts": group_results_ref("w1")})]


@task
async def spawn_group_depends_on() -> list[dict]:
    """A group dict's ``depends_on`` is an edge like a task's."""
    return [
        _task_dict("prep", "double", {"x": 1}),
        {
            "_aaiclick_type": "group",
            "id": "g",
            "name": "g",
            "depends_on": ["prep"],
            "tasks": [_task_dict("w1", "double", {"x": 2})],
        },
    ]


@task
async def spawn_result_without_tasks() -> dict:
    """``tasks`` is optional: a task_result may carry data alone."""
    return {"_aaiclick_type": "task_result", "data": native_ref(5)}
