"""
aaiclick.data.scope - Object scope helpers (table-name prefix scheme).

Four scopes share one table-name prefix convention:

- ``"temp"``       → ``t_<snowflake_id>``         — lifetime: context/task
- ``"temp_named"`` → ``t_<name>_<snowflake_id>``  — lifetime: context/task (named)
- ``"job"``        → ``j_<job_id>_<name>``        — lifetime: owning job's TTL
- ``"global"``     → ``p_<name>``                 — lifetime: forever (user-managed)

Prefix matching is cheap and works both in Python and in SQL cleanup queries.
"""

from __future__ import annotations

import re
from typing import Literal

SCOPE_TEMP = "temp"
SCOPE_TEMP_NAMED = "temp_named"
SCOPE_JOB = "job"
SCOPE_GLOBAL = "global"

ObjectScope = Literal["temp", "temp_named", "job", "global"]
NamedScope = Literal["temp_named", "job", "global"]
PersistentScope = Literal["job", "global"]

GLOBAL_PREFIX = "p_"
TEMP_PREFIX = "t_"
JOB_SCOPED_RE = re.compile(r"^j_\d+_")
TEMP_NAMED_RE = re.compile(r"^t_[a-zA-Z_][a-zA-Z0-9_]*_\d+$")


def scope_of(table_name: str) -> ObjectScope:
    """Return the scope implied by a table name's prefix."""
    if table_name.startswith(GLOBAL_PREFIX):
        return "global"
    if JOB_SCOPED_RE.match(table_name):
        return "job"
    if TEMP_NAMED_RE.match(table_name):
        return "temp_named"
    return "temp"


def is_persistent_table(table_name: str) -> bool:
    """True for tables that survive context/task exit (``p_*`` and ``j_<id>_*``)."""
    return scope_of(table_name) in ("job", "global")


def make_scoped_table_name(
    scope: NamedScope,
    name: str,
    job_id: int | None = None,
    snowid: int | None = None,
) -> str:
    """Build the full CH table name for a scoped named object.

    Args:
        scope: ``"temp_named"``, ``"job"``, or ``"global"``.
        name: Validated persistent name (without prefix).
        job_id: Required when ``scope="job"``.
        snowid: Required when ``scope="temp_named"``.
    """
    if scope == "global":
        return f"{GLOBAL_PREFIX}{name}"
    if scope == "temp_named":
        if snowid is None:
            raise ValueError("scope='temp_named' requires a snowid")
        return f"{TEMP_PREFIX}{name}_{snowid}"
    if job_id is None:
        raise ValueError(
            "scope='job' requires a job_id; create_object_from_value(scope='job') "
            "must run inside orch_context()/task_scope(). Use scope='global' outside orch."
        )
    return f"j_{job_id}_{name}"
