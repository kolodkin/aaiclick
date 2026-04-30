"""Persistent named objects under ``orch_context()``.

Covers both persistence tiers:

- ``scope="job"`` → ``j_<job_id>_<name>``
- ``scope="global"`` → ``p_<name>`` (user-managed, survives the job)

Plus the regex validation of names, the ``open``/``delete`` round trip,
and the API-misuse paths (``scope`` without ``name``, ``delete_persistent_objects``
without a time filter).

Default-scope behaviour (``name`` set, no ``scope``) → ``"temp_named"`` is
covered alongside the unnamed-temp default in
``aaiclick/data/test_context.py``.
"""

import pytest

from aaiclick import create_object_from_value
from aaiclick.data.data_context import (
    delete_persistent_object,
    delete_persistent_objects,
    get_data_lifecycle,
    open_object,
)
from aaiclick.data.data_context.data_context import _validate_persistent_name


async def test_scope_default_is_temp_named_when_name_set(orch_ctx):
    """No ``scope=`` with ``name=`` defaults to ``"temp_named"`` even inside orch."""
    obj = await create_object_from_value([1, 2, 3], name="default_scope_named")
    assert obj.scope == "temp_named"
    assert obj.persistent is False
    assert obj.table.startswith("t_default_scope_named_")


async def test_scope_job_explicit(orch_ctx):
    """``scope='job'`` yields ``j_<job_id>_<name>``."""
    lifecycle = get_data_lifecycle()
    assert lifecycle is not None
    job_id = lifecycle.current_job_id()
    assert job_id is not None

    obj = await create_object_from_value([1, 2, 3], name="explicit_job", scope="job")
    assert obj.table == f"j_{job_id}_explicit_job"
    assert obj.scope == "job"
    assert obj.persistent is True


async def test_scope_global_explicit(orch_ctx):
    """``scope='global'`` yields ``p_<name>`` and survives until explicit delete."""
    obj = await create_object_from_value([10, 20, 30], name="explicit_global", scope="global")
    try:
        assert obj.table == "p_explicit_global"
        assert obj.scope == "global"
        assert obj.persistent is True
        assert await obj.data() == [10, 20, 30]
    finally:
        await delete_persistent_object("explicit_global", scope="global")


async def test_open_object_round_trip_global(orch_ctx):
    """``open_object`` rehydrates schema and data for a ``scope='global'`` table."""
    await create_object_from_value(
        {"x": [1, 2, 3], "y": [4, 5, 6]},
        name="open_round_trip",
        scope="global",
    )
    try:
        opened = await open_object("open_round_trip", scope="global")
        assert opened.table == "p_open_round_trip"
        assert opened.persistent is True
        data = await opened.data()
        assert data["x"] == [1, 2, 3]
        assert data["y"] == [4, 5, 6]
    finally:
        await delete_persistent_object("open_round_trip", scope="global")


async def test_open_object_round_trip_job(orch_ctx):
    """``open_object`` rehydrates schema and data for a ``scope='job'`` table."""
    lifecycle = get_data_lifecycle()
    assert lifecycle is not None
    job_id = lifecycle.current_job_id()
    assert job_id is not None

    await create_object_from_value([7, 8, 9], name="open_job_trip", scope="job")
    opened = await open_object("open_job_trip", scope="job")
    assert opened.table == f"j_{job_id}_open_job_trip"
    assert await opened.data() == [7, 8, 9]


async def test_delete_then_open_raises_global(orch_ctx):
    await create_object_from_value([1], name="delete_then_open", scope="global")
    await delete_persistent_object("delete_then_open", scope="global")

    with pytest.raises(RuntimeError, match="does not exist"):
        await open_object("delete_then_open", scope="global")


async def test_open_nonexistent_raises(orch_ctx):
    with pytest.raises(RuntimeError, match="does not exist"):
        await open_object("does_not_exist_xyz", scope="global")


async def test_delete_persistent_objects_requires_time_filter(orch_ctx):
    with pytest.raises(ValueError, match="At least one of"):
        await delete_persistent_objects()


async def test_scope_without_name_raises(orch_ctx):
    """Passing ``scope`` without a name is API misuse."""
    with pytest.raises(ValueError, match="scope can only be set together with name"):
        await create_object_from_value([1, 2, 3], scope="global")


async def test_unnamed_object_is_temp_in_orch_context(orch_ctx):
    """No ``name=`` → temp ``t_*`` table even inside an orch context."""
    obj = await create_object_from_value([1, 2, 3])
    assert obj.scope == "temp"
    assert obj.persistent is False
    assert obj.table.startswith("t_")


async def test_persistent_name_validation():
    with pytest.raises(ValueError, match="Invalid persistent name"):
        _validate_persistent_name("123bad")
    with pytest.raises(ValueError, match="Invalid persistent name"):
        _validate_persistent_name("has space")
    with pytest.raises(ValueError, match="Invalid persistent name"):
        _validate_persistent_name("has-dash")
    _validate_persistent_name("valid_name")
    _validate_persistent_name("_underscore")
    _validate_persistent_name("CamelCase")
