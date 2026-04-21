"""
aaiclick.data.scope - Object scope helpers (table-name prefix scheme).

Three scopes share one table-name prefix convention:

- ``"temp"``   → ``t_<snowflake_id>``         — lifetime: context/task
- ``"job"``    → ``j_<job_id>_<name>``        — lifetime: owning job's TTL
- ``"global"`` → ``p_<name>``                 — lifetime: forever (user-managed)

Prefix matching is cheap and works both in Python and in SQL cleanup queries.
"""

from __future__ import annotations

import re
from typing import Literal

ObjectScope = Literal["temp", "job", "global"]
NamedScope = Literal["job", "global"]

GLOBAL_PREFIX = "p_"
TEMP_PREFIX = "t_"
JOB_SCOPED_RE = re.compile(r"^j_\d+_")


def scope_of(table_name: str) -> ObjectScope:
    """Return the scope implied by a table name's prefix."""
    if table_name.startswith(GLOBAL_PREFIX):
        return "global"
    if JOB_SCOPED_RE.match(table_name):
        return "job"
    return "temp"


def is_persistent_table(table_name: str) -> bool:
    """True for tables that survive context/task exit (``p_*`` and ``j_<id>_*``)."""
    return scope_of(table_name) != "temp"


def make_persistent_table_name(scope: NamedScope, name: str, job_id: int | None = None) -> str:
    """Build the full CH table name for a scoped named object.

    Args:
        scope: ``"job"`` or ``"global"``.
        name: Validated persistent name (without prefix).
        job_id: Required when ``scope="job"``.
    """
    if scope == "global":
        return f"{GLOBAL_PREFIX}{name}"
    if job_id is None:
        raise ValueError(
            "scope='job' requires a job_id; create_object_from_value(scope='job') "
            "must run inside orch_context()/task_scope(). Use scope='global' outside orch."
        )
    return f"j_{job_id}_{name}"
