"""Audit-log policy — which requests the middleware records."""

from __future__ import annotations

import os
from typing import Literal, cast

ENV_AUDIT_LOG = "AAICLICK_AUDIT_LOG"

AUDIT_WRITES = "writes"
AUDIT_ALL = "all"
AUDIT_OFF = "off"
AuditPolicy = Literal["writes", "all", "off"]
AUDIT_POLICIES: tuple[AuditPolicy, ...] = (AUDIT_WRITES, AUDIT_ALL, AUDIT_OFF)


def audit_policy() -> AuditPolicy:
    """Unknown values fall back to ``writes``."""
    value = os.getenv(ENV_AUDIT_LOG, AUDIT_WRITES).lower()
    return cast(AuditPolicy, value) if value in AUDIT_POLICIES else AUDIT_WRITES
