"""Bare ``data_context()`` rejects persistent named objects.

Persistent tables (``scope="global"``/``"job"``, ``open_object()``) live
in orchestration — they need the SQL ``table_registry`` row that only
``OrchLifecycleHandler`` writes. The behavioural coverage of persistent
tables under ``orch_context()`` lives in
``aaiclick/orchestration/test_persistent.py``; this file just pins the
data-context-side rejection.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context


async def test_named_object_in_bare_data_context_raises():
    async with data_context():
        with pytest.raises(RuntimeError, match="orch_context"):
            await create_object_from_value([1, 2, 3], name="should_not_persist")


async def test_scope_global_in_bare_data_context_raises():
    async with data_context():
        with pytest.raises(RuntimeError, match="orch_context"):
            await create_object_from_value([1, 2, 3], name="x", scope="global")


async def test_scope_job_in_bare_data_context_raises():
    async with data_context():
        with pytest.raises(RuntimeError, match="orch_context"):
            await create_object_from_value([1, 2, 3], name="x", scope="job")
