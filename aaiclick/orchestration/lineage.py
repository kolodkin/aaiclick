"""
aaiclick.orchestration.lineage - Task-graph lineage helpers.

Bridge layer between the orchestration task graph (``Task`` rows in SQL)
and the data oplog (``operation_log`` in ClickHouse). Provides helpers
for detecting input tasks (tasks whose results are persistent and can
be reused in a replay) and for walking the task graph backward from a
terminal task.
"""

from __future__ import annotations

from aaiclick.data.object.refs import TABLE, is_persistent_object_ref

from .models import Task


def is_input_task(task: Task) -> bool:
    """Return ``True`` when the task's result is a persistent Object.

    An *input task* fetches external data into a persistent table and
    returns it. Its output survives job cleanup, so a replay can reuse
    it in place instead of re-running the fetch.

    Detection delegates to ``is_persistent_object_ref`` (the reference
    schema lives in ``aaiclick.data.object.refs``) and adds the ``p_``
    table-name guard that distinguishes aaiclick-managed persistent
    tables from anything else.

    Tasks that haven't run yet (``result is None``) are not input tasks.
    Tasks whose result is a non-Object value (int, dict, pydantic model)
    are not input tasks either — only persistent data objects qualify.
    """
    return is_persistent_object_ref(task.result) and task.result is not None and task.result[TABLE].startswith("p_")
