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

from aaiclick.tenancy import DEFAULT_TENANT_ID

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
# Unambiguous because persistent names may not start with a digit
# (_validate_persistent_name), so no default-tenant object matches this.
GLOBAL_TENANT_RE = re.compile(rf"^{GLOBAL_PREFIX}\d+_")
TEMP_NAMED_RE = re.compile(r"^t_[a-zA-Z_][a-zA-Z0-9_]*_\d+$")


def scope_of(table_name: str) -> ObjectScope:
    """Return the scope implied by a table name's prefix."""
    if table_name.startswith(GLOBAL_PREFIX):
        return SCOPE_GLOBAL
    if JOB_SCOPED_RE.match(table_name):
        return SCOPE_JOB
    if TEMP_NAMED_RE.match(table_name):
        return SCOPE_TEMP_NAMED
    return SCOPE_TEMP


def is_persistent_table(table_name: str) -> bool:
    """True for tables that survive context/task exit (``p_*`` and ``j_<id>_*``)."""
    return scope_of(table_name) in (SCOPE_JOB, SCOPE_GLOBAL)


def name_from_table(table_name: str) -> str:
    """Strip the scope prefix to recover the user-visible name.

    - ``p_<name>``                → ``<name>``
    - ``j_<job_id>_<name>``       → ``<name>``
    - ``t_<name>_<snowflake>``    → ``<name>``
    - ``t_<snowflake>`` (unnamed) → the table name itself
    """
    scope = scope_of(table_name)
    if scope == SCOPE_GLOBAL:
        match = GLOBAL_TENANT_RE.match(table_name)
        return table_name[match.end() if match else len(GLOBAL_PREFIX) :]
    if scope == SCOPE_JOB:
        return table_name.split("_", 2)[2]
    if scope == SCOPE_TEMP_NAMED:
        return table_name[len(TEMP_PREFIX) :].rsplit("_", 1)[0]
    return table_name


def make_scoped_table_name(
    scope: NamedScope,
    name: str,
    job_id: int | None = None,
    snowid: int | None = None,
    tenant_id: int = DEFAULT_TENANT_ID,
) -> str:
    """Build the full CH table name for a scoped named object.

    Args:
        scope: ``"temp_named"``, ``"job"``, or ``"global"``.
        name: Validated persistent name (without prefix).
        job_id: Required when ``scope="job"``.
        snowid: Required when ``scope="temp_named"``.
        tenant_id: Owning tenant. Only ``scope="global"`` encodes it; the
            default tenant keeps the bare ``p_<name>`` form. Job- and
            temp-scoped tables reach their tenant through the owning job.
    """
    if scope == SCOPE_GLOBAL:
        if tenant_id == DEFAULT_TENANT_ID:
            return f"{GLOBAL_PREFIX}{name}"
        return f"{GLOBAL_PREFIX}{tenant_id}_{name}"
    if scope == SCOPE_TEMP_NAMED:
        if snowid is None:
            raise ValueError("scope='temp_named' requires a snowid")
        return f"{TEMP_PREFIX}{name}_{snowid}"
    if job_id is None:
        raise ValueError(
            "scope='job' requires a job_id; create_object_from_value(scope='job') "
            "must run inside orch_context()/task_scope(). Use scope='global' outside orch."
        )
    return f"j_{job_id}_{name}"
