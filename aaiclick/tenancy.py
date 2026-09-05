"""Active-tenant context shared by the server, CLI, and orchestration.

A neutral module (no auth or orchestration imports) so both sides can read
the active tenant without an import cycle. The server's ``require_tenant``
dependency and the CLI set it; ``internal_api`` query filters and the job
factories read it. Unset means the default tenant — local mode and existing
single-tenant deployments keep working with zero configuration.
See ``docs/designs/tenant_rbac.md``.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

# 1 << 62: the same 19-digit width as every snowflake tenant id, so global
# object tables (p_<tenant_id>_<name>) line up with j_<job_id>_<name>, yet a
# value no generator can mint — its snowflake timestamp field decodes to
# November 2004, and ids are stamped with the current wall-clock time.
DEFAULT_TENANT_ID = 1 << 62
DEFAULT_TENANT_SLUG = "aaiclick"

_active_tenant_id: ContextVar[int] = ContextVar("active_tenant_id", default=DEFAULT_TENANT_ID)


def get_active_tenant_id() -> int:
    """Tenant id all tenant-scoped reads/writes are filtered by."""
    return _active_tenant_id.get()


@contextmanager
def active_tenant(tenant_id: int) -> Iterator[None]:
    """Set the active tenant for the duration of the block."""
    token = _active_tenant_id.set(tenant_id)
    try:
        yield
    finally:
        _active_tenant_id.reset(token)
