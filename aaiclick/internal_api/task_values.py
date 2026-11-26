"""Task inputs and results as rows of a job's object listing.

See docs/designs/viewer.md — "Task Values".

A job's ``tasks.kwargs`` / ``tasks.result`` JSON, shown next to its objects:
``list_task_values`` builds one ``ObjectView`` per top-level kwarg and per
non-null result, ``read_task_value`` reads one by address. Nothing is stored.

An address is ``<task_id>.<kwarg>`` for an input and ``<task_id>`` for a
result. Object names cannot start with a digit, so the two never collide.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from typing import NamedTuple

from pydantic import JsonValue
from sqlmodel import col, select

from aaiclick.data.object.refs import (
    AAICLICK_TYPE,
    CALLABLE,
    ENTRYPOINT,
    GROUP_ID,
    GROUP_RESULTS,
    NATIVE,
    OBJECT,
    PYDANTIC,
    TABLE,
    TASK_ID,
    UPSTREAM,
    VALUE,
    VIEW,
)
from aaiclick.data.scope import JOB_PREFIX, SCOPE_JOB, name_from_table
from aaiclick.data.view_models import (
    DIRECTION_IN,
    DIRECTION_OUT,
    OBJECT_KIND_REF,
    OBJECT_KIND_VALUE,
    OWNER_JOB,
    OWNER_TASK,
    ObjectKind,
    ObjectView,
    ValueDirection,
)
from aaiclick.orchestration.models import Job, Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.snowflake import parse_snowflake_id

from .errors import NotFound

PREVIEW_CHARS = 120
MAX_VALUE_BYTES = 1024 * 1024

REF_TYPE_OBJECT = "object"
REF_TYPE_VIEW = "view"
REF_TYPE_TASK = "task"
REF_TYPE_GROUP = "group"
REF_TYPE_FUNCTION = "function"

_RESULT = "result"
# 19 digits hold any int64; a longer run is not a task id.
_ADDRESS_RE = re.compile(r"^(\d{1,19})(?:\.([A-Za-z_][A-Za-z0-9_]*))?$")


def is_address(name: str) -> bool:
    """True when ``name`` is a task-value address rather than an object name."""
    return _ADDRESS_RE.match(name) is not None


def _kwargs(stored: object) -> dict[str, JsonValue]:
    """A task's kwargs, or nothing when a hand-edited row holds something else."""
    return stored if isinstance(stored, dict) else {}


def _dump(value: JsonValue) -> str:
    return json.dumps(value, ensure_ascii=False)


class _Described(NamedTuple):
    kind: ObjectKind
    preview: str
    ref_type: str | None = None
    target: str | None = None


def _describe(
    value: JsonValue,
    task_names: dict[int, str],
    with_result: set[int],
    group_sizes: Counter[int],
    job_table_prefix: str,
) -> _Described:
    """How one stored value lists.

    Reads ``_aaiclick_type`` directly: ``ref_kind()`` raises on an unknown
    type, and a listing must show it instead.
    """
    if not isinstance(value, dict) or value.get(AAICLICK_TYPE) is None:
        return _Described(OBJECT_KIND_VALUE, _dump(value)[:PREVIEW_CHARS])
    tag = value[AAICLICK_TYPE]
    if tag == NATIVE:
        return _Described(OBJECT_KIND_VALUE, _dump(value.get(VALUE))[:PREVIEW_CHARS])
    if tag == PYDANTIC:
        text = f"{value.get('type')} {_dump(value.get('data'))}"
        return _Described(OBJECT_KIND_VALUE, text[:PREVIEW_CHARS])

    ref_type, preview, target = str(tag), str(tag), None
    if tag in (OBJECT, VIEW) and isinstance(value.get(TABLE), str):
        ref_type = REF_TYPE_OBJECT if tag == OBJECT else REF_TYPE_VIEW
        table = str(value[TABLE])
        preview = name_from_table(table)
        # Links open in this job's scope, so only this job's own tables get one.
        target = preview if table.startswith(job_table_prefix) else None
    elif tag == UPSTREAM:
        ref_type = REF_TYPE_TASK
        task_id = value.get(TASK_ID)
        name = task_names.get(task_id) if isinstance(task_id, int) else None
        # A string task_id is a handle to a sibling not committed yet: nothing to open.
        preview = f"{name}.{_RESULT}" if name else f"task {task_id}"
        # Nothing to open until that task has stored a result.
        target = str(task_id) if task_id in with_result else None
    elif tag == GROUP_RESULTS:
        ref_type = REF_TYPE_GROUP
        group_id = value.get(GROUP_ID)
        size = group_sizes[group_id] if isinstance(group_id, int) else 0
        # Direct members only; a nested group or a handle counts none, so say nothing then.
        preview = f"group {group_id} results" + (f" ({size} tasks)" if size else "")
    elif tag == CALLABLE:
        ref_type = REF_TYPE_FUNCTION
        preview = str(value.get(ENTRYPOINT))
    return _Described(OBJECT_KIND_REF, preview, ref_type, target)


async def list_task_values(job: Job) -> list[ObjectView]:
    """Every input and result of ``job``'s tasks.

    Order: the job's own kwargs, the job's result, the other tasks' results,
    then the other tasks' kwargs.
    """
    async with get_sql_session() as session:
        tasks = (
            await session.execute(
                select(Task.id, Task.name, Task.group_id, Task.kwargs, Task.result, Task.created_at)
                .where(col(Task.job_id) == job.id)
                .order_by(col(Task.id))
            )
        ).all()
    task_names = {t.id: t.name for t in tasks}
    with_result = {t.id for t in tasks if t.result is not None}
    job_table_prefix = f"{JOB_PREFIX}{job.id}_"
    group_sizes = Counter(t.group_id for t in tasks if t.group_id is not None)

    rows: list[ObjectView] = []
    for task in tasks:
        entries: list[tuple[str, str, ValueDirection, JsonValue]] = [
            (f"{task.id}.{key}", key, DIRECTION_IN, value) for key, value in sorted(_kwargs(task.kwargs).items())
        ]
        if task.result is not None:
            entries.append((str(task.id), _RESULT, DIRECTION_OUT, task.result))
        for address, key, direction, value in entries:
            rows.append(
                ObjectView(
                    name=address,
                    table="",
                    scope=SCOPE_JOB,
                    persistent=False,
                    job_id=job.id,
                    job_name=job.name,
                    created_at=task.created_at,
                    label=f"{task.name}.{key}",
                    task_id=task.id,
                    direction=direction,
                    # The entry task is named after the job (as get_job_result finds it),
                    # so its rows are the job's own inputs and result.
                    owner=OWNER_JOB if task.name == job.name else OWNER_TASK,
                    **_describe(value, task_names, with_result, group_sizes, job_table_prefix)._asdict(),
                )
            )

    # The sort is stable: sections keep task-id order, inputs by name.
    def section(row: ObjectView) -> int:
        is_input = row.direction == DIRECTION_IN
        if row.owner == OWNER_JOB:
            return 0 if is_input else 1
        return 3 if is_input else 2

    return sorted(rows, key=section)


async def read_task_value(job_id: int, address: str) -> JsonValue:
    """The value at ``address`` in job ``job_id`` as stored, ``native`` unwrapped.

    A value over ``MAX_VALUE_BYTES`` comes back as ``{"truncated": true,
    "bytes": n}``. Raises ``NotFound`` for a malformed address, another job's
    task, a missing kwarg or a task with no result yet.
    """
    match = _ADDRESS_RE.match(address)
    if match is None:
        raise NotFound(f"Object not found: {address}")
    task_id, key = parse_snowflake_id(match.group(1)), match.group(2)
    if task_id is None:
        raise NotFound(f"Object not found: {address}")
    # The address says which column holds the value: no kwarg part means the result.
    column = col(Task.result) if key is None else col(Task.kwargs)
    async with get_sql_session() as session:
        row = (await session.execute(select(column).where(col(Task.id) == task_id, col(Task.job_id) == job_id))).first()
    if row is None:
        raise NotFound(f"Object not found: {address}")
    if key is None:
        value = row[0]
        if value is None:
            raise NotFound(f"Task {task_id} has no result")
    else:
        kwargs = _kwargs(row[0])
        if key not in kwargs:
            raise NotFound(f"Object not found: {address}")
        value = kwargs[key]
    if isinstance(value, dict) and value.get(AAICLICK_TYPE) == NATIVE:
        value = value.get(VALUE)
    size = len(_dump(value).encode())
    if size > MAX_VALUE_BYTES:
        return {"truncated": True, "bytes": size}
    return value
