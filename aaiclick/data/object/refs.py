"""
aaiclick.data.object.refs - Task kwarg/result reference schema.

Single source of truth for the shapes used to serialize ``Task`` kwargs
and results. ``Object._serialize_ref()`` produces these shapes,
``orchestration.execution.runner`` consumes them, and
``orchestration.replay`` rewrites them during Phase 3b task-graph
replay.

The canonical producer API is a family of Pydantic ``BaseModel`` classes
— ``UpstreamRef``, ``GroupResultsRef``, ``CallableRef``, ``ObjectRef``,
``ViewRef``. Construct a model, call ``.to_dict()`` to get the exact
on-wire shape that JSON columns (``tasks.kwargs`` / ``tasks.result``)
store. That gives producers field-level validation (e.g. ``table`` must
be ``str``, ``task_id`` must be ``int``) that plain dict literals miss.

Consumers still work with the raw dict shape (the deserialization
dispatcher in ``orchestration.execution.runner`` walks nested kwargs
and dispatches on tag keys). The string constants below are the
authoritative key/value names — both producers (via the models' field
names / ``Literal`` tags) and consumers (via ``value.get(REF_TYPE)``
lookups) reference them.

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

**View** — a filtered/projected view over an Object. All view modifier
fields (``where`` / ``limit`` / ``offset`` / ``order_by`` /
``selected_fields`` / ``renamed_columns``) are always present, possibly
``None`` — the runner deserialization reads them via ``.get()`` and
feeds them to the ``View`` constructor unchanged::

    {"object_type": "view", "table": "...", "where": ..., "limit": ..., ...}

**Native Python values** — anything JSON-serializable that isn't one of
the above::

    {"native_value": <value>}

**Pydantic models** — serialized with the model's dotted class path::

    {"pydantic_type": "mod.Model", "data": {...}}

Native and pydantic shapes have no Pydantic model here because they're
terminal leaves — there's nothing the producer needs to validate beyond
"this value is JSON-serializable".
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, model_serializer


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


# --- Pydantic models ---------------------------------------------------

class _FrozenRef(BaseModel):
    """Shared config: refs are immutable value objects."""

    model_config = ConfigDict(frozen=True)


class UpstreamRef(_FrozenRef):
    """Reference to an upstream task's result."""

    ref_type: Literal["upstream"] = UPSTREAM
    task_id: int

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()


class GroupResultsRef(_FrozenRef):
    """Reference to the aggregated results of a Group's tasks."""

    ref_type: Literal["group_results"] = GROUP_RESULTS
    group_id: int

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()


class CallableRef(_FrozenRef):
    """Reference to a callable entrypoint (module.function)."""

    ref_type: Literal["callable"] = CALLABLE
    entrypoint: str

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()


def _drop_optional_metadata(wire: Dict[str, Any]) -> Dict[str, Any]:
    """Strip the optional metadata fields shared by ObjectRef / ViewRef.

    ``persistent`` is dropped unless truthy (``False`` and ``None`` are
    indistinguishable on the wire); ``job_id`` is dropped when ``None``.
    Mutates and returns ``wire``.
    """
    if not wire.get(PERSISTENT):
        wire.pop(PERSISTENT, None)
    if wire.get(JOB_ID) is None:
        wire.pop(JOB_ID, None)
    return wire


class ObjectRef(_FrozenRef):
    """Reference to an aaiclick ``Object`` — a ClickHouse table.

    ``persistent`` is omitted from the wire format when ``None`` or
    ``False`` so the on-disk shape stays minimal for ephemeral tables.
    """

    object_type: Literal["object"] = OBJECT
    table: str
    persistent: Optional[bool] = None
    job_id: Optional[int] = None

    @model_serializer(mode="wrap")
    def _to_wire(self, handler) -> Dict[str, Any]:
        return _drop_optional_metadata(handler(self))

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()


class ViewRef(_FrozenRef):
    """Reference to an aaiclick ``View`` over an ``Object``.

    Every view modifier field is emitted on the wire even when ``None``;
    the runner's View reconstruction reads them unconditionally via
    ``.get()`` and passes them to the ``View`` constructor.
    """

    object_type: Literal["view"] = VIEW
    table: str
    where: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    order_by: Optional[str] = None
    selected_fields: Optional[List[str]] = None
    renamed_columns: Optional[Dict[str, str]] = None
    persistent: Optional[bool] = None
    job_id: Optional[int] = None

    @model_serializer(mode="wrap")
    def _to_wire(self, handler) -> Dict[str, Any]:
        return _drop_optional_metadata(handler(self))

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()


# --- Constructors (thin wrappers around the models) -------------------

def upstream_ref(task_id: int) -> Dict[str, Any]:
    """Build an upstream task reference dict."""
    return UpstreamRef(task_id=task_id).to_dict()


def group_results_ref(group_id: int) -> Dict[str, Any]:
    """Build a group-results reference dict."""
    return GroupResultsRef(group_id=group_id).to_dict()


def callable_ref(entrypoint: str) -> Dict[str, Any]:
    """Build a callable reference dict."""
    return CallableRef(entrypoint=entrypoint).to_dict()


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
