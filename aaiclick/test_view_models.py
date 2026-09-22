from __future__ import annotations

import subprocess
import sys

import pytest
from pydantic import ValidationError

from aaiclick.audit.view_models import AuditListFilter
from aaiclick.auth.view_models import UserListFilter
from aaiclick.view_models import ExecutionWorkerFilter, JobListFilter, ObjectFilter, RegisteredJobFilter
from aaiclick.viewer.view_models import SavedQueryFilter


def test_import_order_independent_of_orchestration():
    """``view_models`` must be importable before ``orchestration``.

    Regression for the old ``view_models`` ↔ ``orchestration`` cycle: any
    import path that reached ``view_models`` first (e.g. the AI stack) died
    with a partially-initialized-module ImportError. Each ordering runs in a
    fresh interpreter because import-order bugs are invisible once the modules
    are cached in this process.
    """
    for stmt in (
        "import aaiclick.view_models",
        "import aaiclick.ai.ollama, aaiclick.view_models",
    ):
        proc = subprocess.run([sys.executable, "-c", stmt], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr


@pytest.mark.parametrize(
    "model",
    [
        JobListFilter,
        RegisteredJobFilter,
        ExecutionWorkerFilter,
        ObjectFilter,
        UserListFilter,
        AuditListFilter,
        SavedQueryFilter,
    ],
)
def test_every_list_filter_bounds_its_limit(model):
    """Unbounded limits returned whole tables; ``limit=0`` and huge values are 422s at the boundary."""
    with pytest.raises(ValidationError):
        model(limit=0)
    with pytest.raises(ValidationError):
        model(limit=10_000)


@pytest.mark.parametrize(
    "model", [JobListFilter, RegisteredJobFilter, ExecutionWorkerFilter, UserListFilter, AuditListFilter]
)
def test_every_offset_filter_rejects_negative_offset(model):
    """A negative OFFSET is a Postgres error (500) — refuse it on input."""
    with pytest.raises(ValidationError):
        model(offset=-1)
