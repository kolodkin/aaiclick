"""
aaiclick.data.object.refs - Task kwarg/result reference schema.

See docs/designs/task_interface.md — "Vocabulary".

Single source of truth for the tagged-dict vocabulary used to serialize
``Task`` kwargs and results. ``Object._serialize_ref()`` produces these
shapes and ``orchestration.execution.runner`` consumes them — and so can a
shim in any language: every tagged dict carries one discriminator key,
``_aaiclick_type``.

The canonical producer API is a family of Pydantic ``BaseModel`` classes
— ``UpstreamRef``, ``GroupResultsRef``, ``CallableRef``, ``ObjectRef``,
``ViewRef``. Construct a model, call ``.to_dict()`` to get the exact
on-wire shape that JSON columns (``tasks.kwargs`` / ``tasks.result``)
store. Consumers classify the raw dict with ``ref_kind()``.

``_aaiclick_type`` is reserved: a dict carrying an unknown value is an
error, never plain data. Untagged JSON values are plain data.

Reference shapes
----------------

**Upstream task result** — ``task_id`` is an int for a committed task, or
a string *handle* naming a sibling ``task`` dict in the same return value::

    {"_aaiclick_type": "upstream", "task_id": <int | handle>}

**Group result aggregation** — results of all tasks in a Group::

    {"_aaiclick_type": "group_results", "group_id": <int | handle>}

**Callable reference** — a ``@task`` wrapper passed as a value::

    {"_aaiclick_type": "callable", "entrypoint": "mod.fn"}

**Persistent / ephemeral Object** — a materialized ClickHouse table.
``persistent`` is omitted (and defaults False) for ephemeral tables::

    {"_aaiclick_type": "object", "table": "p_foo", "persistent": true}
    {"_aaiclick_type": "object", "table": "t_12345"}

**View** — a view over an Object; ``view_schema`` is the serialized
``ViewSchema``::

    {"_aaiclick_type": "view", "table": "...", "view_schema": {...}}

**Native values** — anything JSON-serializable::

    {"_aaiclick_type": "native", "value": <value>}

**Pydantic models** — serialized with the model's dotted class path::

    {"_aaiclick_type": "pydantic", "type": "mod.Model", "data": {...}}

**Returned tasks** — the JSON form of what a task function may return.
A ``task`` carries ``create_task()``'s parameters plus an optional ``id``
handle and ``depends_on`` (a list of handles); a ``group`` holds its
member tasks inline; ``task_result`` separates data from tasks::

    {"_aaiclick_type": "task", "id": "w1", "name": "worker", "entry_type": "shell",
     "command": ["node", "worker.js"], "kwargs": {"n": 1}}
    {"_aaiclick_type": "group", "id": "g", "name": "parts", "tasks": [...]}
    {"_aaiclick_type": "task_result",
     "data": {"_aaiclick_type": "upstream", "task_id": "fin"}, "tasks": [...]}
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_serializer

from aaiclick.data.models import ViewSchema

# --- Key names ---------------------------------------------------------

AAICLICK_TYPE = "_aaiclick_type"
TASK_ID = "task_id"
GROUP_ID = "group_id"
ENTRYPOINT = "entrypoint"

TABLE = "table"
PERSISTENT = "persistent"
JOB_ID = "job_id"

VALUE = "value"
PYDANTIC_CLASS = "type"
DATA = "data"
PYDANTIC_DATA = DATA

HANDLE = "id"
DEPENDS_ON = "depends_on"
TASKS = "tasks"


# --- _aaiclick_type values ---------------------------------------------

UPSTREAM = "upstream"
GROUP_RESULTS = "group_results"
CALLABLE = "callable"
OBJECT = "object"
VIEW = "view"
NATIVE = "native"
PYDANTIC = "pydantic"
TASK = "task"
GROUP = "group"
TASK_RESULT = "task_result"

RefKind = Literal[
    "upstream", "group_results", "callable", "object", "view", "native", "pydantic", "task", "group", "task_result"
]
REF_KINDS: list[RefKind] = [UPSTREAM, GROUP_RESULTS, CALLABLE, OBJECT, VIEW, NATIVE, PYDANTIC, TASK, GROUP, TASK_RESULT]


# --- Pydantic models ---------------------------------------------------


class _FrozenRef(BaseModel):
    """Shared config: refs are immutable value objects. The discriminator
    rides on the ``_aaiclick_type`` alias (a leading underscore is private to
    Pydantic as a field name)."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True)


class UpstreamRef(_FrozenRef):
    """Reference to an upstream task's result."""

    aaiclick_type: Literal["upstream"] = Field(default=UPSTREAM, alias=AAICLICK_TYPE)
    task_id: int | str


class GroupResultsRef(_FrozenRef):
    """Reference to the aggregated results of a Group's tasks."""

    aaiclick_type: Literal["group_results"] = Field(default=GROUP_RESULTS, alias=AAICLICK_TYPE)
    group_id: int | str


class CallableRef(_FrozenRef):
    """Reference to a callable entrypoint (module.function)."""

    aaiclick_type: Literal["callable"] = Field(default=CALLABLE, alias=AAICLICK_TYPE)
    entrypoint: str


def _drop_optional_metadata(wire: dict[str, Any]) -> dict[str, Any]:
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

    aaiclick_type: Literal["object"] = Field(default=OBJECT, alias=AAICLICK_TYPE)
    table: str
    persistent: bool | None = None
    job_id: int | None = None

    @model_serializer(mode="wrap")
    def _to_wire(self, handler) -> dict[str, Any]:
        return _drop_optional_metadata(handler(self))


class ViewRef(_FrozenRef):
    """Reference to an aaiclick ``View`` over an ``Object``.

    ``view_schema`` is the View's ``ViewSchema``; ``table`` stays top-level so
    ref consumers find the source table without parsing it.
    """

    aaiclick_type: Literal["view"] = Field(default=VIEW, alias=AAICLICK_TYPE)
    table: str
    view_schema: ViewSchema
    persistent: bool | None = None
    job_id: int | None = None

    @model_serializer(mode="wrap")
    def _to_wire(self, handler) -> dict[str, Any]:
        return _drop_optional_metadata(handler(self))


# --- Constructors (thin wrappers around the models) -------------------


def upstream_ref(task_id: int | str) -> dict[str, Any]:
    """Build an upstream task reference dict."""
    return UpstreamRef(task_id=task_id).to_dict()


def group_results_ref(group_id: int | str) -> dict[str, Any]:
    """Build a group-results reference dict."""
    return GroupResultsRef(group_id=group_id).to_dict()


def callable_ref(entrypoint: str) -> dict[str, Any]:
    """Build a callable reference dict."""
    return CallableRef(entrypoint=entrypoint).to_dict()


def native_ref(value: Any) -> dict[str, Any]:
    """Wrap a native JSON-serializable Python value."""
    return {AAICLICK_TYPE: NATIVE, VALUE: value}


def pydantic_ref(model: BaseModel) -> dict[str, Any]:
    """Serialize a Pydantic model with its dotted class path."""
    return {
        AAICLICK_TYPE: PYDANTIC,
        PYDANTIC_CLASS: f"{type(model).__module__}.{type(model).__qualname__}",
        PYDANTIC_DATA: model.model_dump(),
    }


# --- Predicates --------------------------------------------------------


def is_upstream_ref(value: Any) -> bool:
    """True iff ``value`` is an upstream task reference dict."""
    return isinstance(value, dict) and value.get(AAICLICK_TYPE) == UPSTREAM


def ref_kind(value: Any) -> RefKind | None:
    """Classify one serialized kwarg/result value; None for a plain value.

    Single source of truth shared by the runner's deserializer, the
    returned-task registrar and the plain-data-plane validator. Raises
    ``ValueError`` on an unknown ``_aaiclick_type`` — the key is reserved."""
    if not isinstance(value, dict) or AAICLICK_TYPE not in value:
        return None
    kind = value[AAICLICK_TYPE]
    if kind not in REF_KINDS:
        raise ValueError(f"Unknown {AAICLICK_TYPE}: {kind!r}")
    return kind


def is_persistent_object_ref(value: Any) -> bool:
    """True iff ``value`` is a serialized persistent Object reference."""
    return (
        isinstance(value, dict)
        and value.get(AAICLICK_TYPE) == OBJECT
        and value.get(PERSISTENT) is True
        and isinstance(value.get(TABLE), str)
    )
