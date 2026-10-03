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

    Regression for the old ``view_models`` ↔ ``orchestration`` cycle: an
    import path that reached ``view_models`` first died with a
    partially-initialized-module ImportError. Runs in a fresh interpreter
    because import-order bugs are invisible once the modules are cached.
    """
    proc = subprocess.run([sys.executable, "-c", "import aaiclick.view_models"], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr


@pytest.mark.parametrize(
    "model",
    [
        pytest.param(JobListFilter, id="job-list"),
        pytest.param(RegisteredJobFilter, id="registered-job"),
        pytest.param(ExecutionWorkerFilter, id="execution-worker"),
        pytest.param(ObjectFilter, id="object"),
        pytest.param(UserListFilter, id="user-list"),
        pytest.param(AuditListFilter, id="audit-list"),
        pytest.param(SavedQueryFilter, id="saved-query"),
    ],
)
def test_every_list_filter_bounds_its_limit(model):
    """Unbounded limits returned whole tables; ``limit=0`` and huge values are 422s at the boundary."""
    with pytest.raises(ValidationError):
        model(limit=0)
    with pytest.raises(ValidationError):
        model(limit=10_000)


@pytest.mark.parametrize(
    "model",
    [
        pytest.param(JobListFilter, id="job-list"),
        pytest.param(RegisteredJobFilter, id="registered-job"),
        pytest.param(ExecutionWorkerFilter, id="execution-worker"),
        pytest.param(UserListFilter, id="user-list"),
        pytest.param(AuditListFilter, id="audit-list"),
        pytest.param(SavedQueryFilter, id="saved-query"),
    ],
)
def test_every_offset_filter_rejects_negative_offset(model):
    """A negative OFFSET is a Postgres error (500) — refuse it on input."""
    with pytest.raises(ValidationError):
        model(offset=-1)
