"""Scope keys: the string form of "which objects" a viewer request means."""

from __future__ import annotations

from typing import Literal, NamedTuple

from aaiclick.view_models import RefId

SCOPE_PERSISTENT = "persistent"
SCOPE_JOB_KIND = "job"
ScopeKind = Literal["persistent", "job"]


class ScopeRef(NamedTuple):
    kind: ScopeKind
    job: RefId | None  # id, or name resolving to the latest run; None for persistent


def parse_scope(key: str) -> ScopeRef:
    """``"persistent"`` or ``"job:<id|name>"`` → ``ScopeRef``; ``ValueError`` otherwise."""
    if key == SCOPE_PERSISTENT:
        return ScopeRef(SCOPE_PERSISTENT, None)
    prefix = f"{SCOPE_JOB_KIND}:"
    if key.startswith(prefix) and len(key) > len(prefix):
        ref = key[len(prefix) :]
        return ScopeRef(SCOPE_JOB_KIND, int(ref) if ref.isdigit() else ref)
    raise ValueError(f"scope must be 'persistent' or 'job:<id|name>', got {key!r}")


def scope_key(ref: ScopeRef) -> str:
    return SCOPE_PERSISTENT if ref.kind == SCOPE_PERSISTENT else f"{SCOPE_JOB_KIND}:{ref.job}"
