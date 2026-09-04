"""
aaiclick.data.scope - Object scope helpers (table-name prefix scheme).

Four scopes share one table-name prefix convention:

- ``"temp"``       → ``t_<snowflake_id>``         — lifetime: context/task
- ``"temp_named"`` → ``t_<name>_<snowflake_id>``  — lifetime: context/task (named)
- ``"job"``        → ``j_<job_id>_<name>``        — lifetime: owning job's TTL
- ``"global"``     → ``p_<snowflake_id>``         — lifetime: forever (user-managed)

The prefix only encodes the *scope*; it is cheap to match both in Python and
in SQL cleanup queries. A global object's user-visible name is not part of
its table name — it lives in SQL ``table_registry.name`` (unique per tenant),
which is the sole name → table mapping.
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
OBJECT_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


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


def make_scoped_table_name(
    scope: NamedScope,
    name: str,
    job_id: int | None = None,
    snowid: int | None = None,
) -> str:
    """Build the full CH table name for a scoped named object.

    Args:
        scope: ``"temp_named"``, ``"job"``, or ``"global"``.
        name: Validated object name. Encoded into ``temp_named`` and ``job``
            table names only; a ``global`` table is ``p_<snowid>`` and the
            name is recorded in ``table_registry`` instead.
        job_id: Required when ``scope="job"``.
        snowid: Required when ``scope="temp_named"`` or ``scope="global"``.
    """
    if scope == SCOPE_GLOBAL:
        if snowid is None:
            raise ValueError("scope='global' requires a snowid")
        return f"{GLOBAL_PREFIX}{snowid}"
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


def legacy_global_name(table_name: str, tenant_id: int, default_tenant_id: int) -> str | None:
    """Recover the object name from a pre-registry global table name.

    Before names moved into ``table_registry``, global tables were
    ``p_<name>`` for the default tenant and ``p_<tenant_id>_<name>``
    otherwise. Returns ``None`` for anything that does not parse as such
    a table — including today's opaque ``p_<snowflake>`` names.
    """
    if not table_name.startswith(GLOBAL_PREFIX):
        return None
    tenant_prefix = f"{GLOBAL_PREFIX}{tenant_id}_"
    if tenant_id != default_tenant_id and table_name.startswith(tenant_prefix):
        name = table_name[len(tenant_prefix) :]
    else:
        name = table_name[len(GLOBAL_PREFIX) :]
    return name if OBJECT_NAME_RE.match(name) else None
