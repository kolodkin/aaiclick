"""
aaiclick.orchestration.lineage - Task-graph lineage helpers.

Bridge layer between the orchestration task graph (``Task`` rows in SQL)
and the data oplog (``operation_log`` in ClickHouse). Provides helpers
for detecting input tasks (tasks whose results are persistent and can
be reused in a replay) and for walking the task graph backward from a
terminal task.
"""

from __future__ import annotations

from .models import Task


def is_input_task(task: Task) -> bool:
    """Return ``True`` when the task's result is a persistent Object.

    An *input task* fetches external data into a persistent table and
    returns it. Its output survives job cleanup, so a replay can reuse
    it in place instead of re-running the fetch.

    Detection relies on the serialized result shape produced by
    ``serialize_task_result()`` — a persistent Object ref is stored as::

        {"object_type": "object", "table": "p_...", "persistent": true, ...}

    Tasks that haven't run yet (``result is None``) are not input tasks.
    Tasks whose result is a non-Object value (int, dict, pydantic model)
    are not input tasks either — only persistent data objects qualify.
    """
    if task.result is None or not isinstance(task.result, dict):
        return False
    return (
        task.result.get("object_type") == "object"
        and task.result.get("persistent") is True
        and isinstance(task.result.get("table"), str)
        and task.result["table"].startswith("p_")
    )
