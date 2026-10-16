"""Tasks that return the JSON form of a dynamic DAG — ``task``, ``group`` and
``task_result`` dicts with local handles — instead of Python ``Task`` objects.

Module-level so a worker can import every child by entrypoint.
"""

from aaiclick.data.object.refs import group_results_ref, upstream_ref
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
