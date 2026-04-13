"""
aaiclick.data.object.refs - Task kwarg/result reference schema.

Single source of truth for the string keys used to serialize ``Task``
kwargs and results. ``Object._serialize_ref()`` produces these shapes,
``orchestration.execution.runner`` consumes them, and
``orchestration.replay`` rewrites them during Phase 3b task-graph
replay. Keeping the vocabulary here means a schema change lands in one
file instead of silently drifting across producers and consumers.

Reference shapes
----------------

**Upstream task result** — injected when one ``Task`` is passed as a
kwarg to another::

    {"ref_type": "upstream", "task_id": <int>}

**Group result aggregation** — collects results of all tasks in a
Group (e.g. output of ``reduce``)::

    {"ref_type": "group_results", "group_id": <int>}

**Callable reference** — the @task wrapper itself being passed as a
value (e.g. a map callback)::

    {"ref_type": "callable", "entrypoint": "mod.fn"}

**Persistent / ephemeral Object** — a materialized ClickHouse table.
``persistent`` is omitted (and defaults False) for ephemeral tables::

    {"object_type": "object", "table": "p_foo", "persistent": true}
    {"object_type": "object", "table": "t_12345"}

**View** — a filtered/projected view over an Object::

    {"object_type": "view", "table": "...", "where": ..., "limit": ..., ...}

**Native Python values** — anything JSON-serializable that isn't one of
the above::

    {"native_value": <value>}

**Pydantic models** — serialized with the model's dotted class path::

    {"pydantic_type": "mod.Model", "data": {...}}
"""

from __future__ import annotations

from typing import Any, Dict


# --- Key names ---------------------------------------------------------

REF_TYPE = "ref_type"
TASK_ID = "task_id"
GROUP_ID = "group_id"
ENTRYPOINT = "entrypoint"

OBJECT_TYPE = "object_type"
TABLE = "table"
PERSISTENT = "persistent"
JOB_ID = "job_id"

NATIVE_VALUE = "native_value"
PYDANTIC_TYPE = "pydantic_type"
PYDANTIC_DATA = "data"


# --- ref_type values ---------------------------------------------------

UPSTREAM = "upstream"
GROUP_RESULTS = "group_results"
CALLABLE = "callable"


# --- object_type values ------------------------------------------------

OBJECT = "object"
VIEW = "view"


# --- Constructors ------------------------------------------------------

def upstream_ref(task_id: int) -> Dict[str, Any]:
    """Build an upstream task reference dict."""
    return {REF_TYPE: UPSTREAM, TASK_ID: task_id}


def group_results_ref(group_id: int) -> Dict[str, Any]:
    """Build a group-results reference dict."""
    return {REF_TYPE: GROUP_RESULTS, GROUP_ID: group_id}


def callable_ref(entrypoint: str) -> Dict[str, Any]:
    """Build a callable reference dict."""
    return {REF_TYPE: CALLABLE, ENTRYPOINT: entrypoint}


def native_value_ref(value: Any) -> Dict[str, Any]:
    """Wrap a native JSON-serializable Python value."""
    return {NATIVE_VALUE: value}


# --- Predicates --------------------------------------------------------

def is_upstream_ref(value: Any) -> bool:
    """True iff ``value`` is an upstream task reference dict."""
    return isinstance(value, dict) and value.get(REF_TYPE) == UPSTREAM


def is_persistent_object_ref(value: Any) -> bool:
    """True iff ``value`` is a serialized persistent Object reference."""
    return (
        isinstance(value, dict)
        and value.get(OBJECT_TYPE) == OBJECT
        and value.get(PERSISTENT) is True
        and isinstance(value.get(TABLE), str)
    )
