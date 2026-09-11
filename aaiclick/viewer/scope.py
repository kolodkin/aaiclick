"""Scope keys: the string form of "which objects" a viewer request means."""

from __future__ import annotations

from typing import NamedTuple

SCOPE_PERSISTENT = "persistent"
_JOB_PREFIX = "job:"


class ScopeRef(NamedTuple):
    job: str | None  # a job id or name (resolved by ``jobs_api.resolve_job``); None for persistent


def parse_scope(key: str) -> ScopeRef:
    """``"persistent"`` or ``"job:<id|name>"`` → ``ScopeRef``; ``ValueError`` otherwise."""
    if key == SCOPE_PERSISTENT:
        return ScopeRef(None)
    if key.startswith(_JOB_PREFIX) and len(key) > len(_JOB_PREFIX):
        return ScopeRef(key[len(_JOB_PREFIX) :])
    raise ValueError(f"scope must be 'persistent' or 'job:<id|name>', got {key!r}")
