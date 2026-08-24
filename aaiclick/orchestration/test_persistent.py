"""Persistent named objects under ``orch_context()``.

Covers both persistence tiers:

- ``scope="job"`` → ``j_<job_id>_<name>``
- ``scope="global"`` → ``p_<name>`` (user-managed, survives the job)

Plus the regex validation of names, the ``open``/``delete`` round trip,
and the API-misuse paths (``scope`` without ``name``, ``delete_persistent_objects``
without a time filter).

Default-scope behaviour (``name`` set, no ``scope``) → ``"temp_named"`` is
covered alongside the unnamed-temp default in
``aaiclick/data/data_context/test_persistent.py``.
"""

from datetime import datetime

import pytest
from sqlmodel import select

from aaiclick import create_object_from_value
from aaiclick.data.data_context import (
    delete_persistent_object,
    delete_persistent_objects,
    get_data_lifecycle,
    list_persistent_objects,
    open_object,
)
from aaiclick.data.data_context.data_context import MAX_PERSISTENT_NAME_LEN, _validate_persistent_name
from aaiclick.data.errors import ObjectNotFoundError
from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
from aaiclick.orchestration.sql_context import get_sql_session
from aaiclick.tenancy import active_tenant


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
    """Rejecting a leading digit is load-bearing beyond tidiness.

    Tenant-prefixed object naming (``p_<tenant_id>_<name>``, see
    ``docs/designs/tenant_rbac.md``) is only unambiguous while no
    default-tenant object can produce a ``p_<digits>_`` prefix.
    """
    with pytest.raises(ValueError, match="Invalid persistent name"):
        _validate_persistent_name("123bad")
    with pytest.raises(ValueError, match="Invalid persistent name"):
        _validate_persistent_name("has space")
    with pytest.raises(ValueError, match="Invalid persistent name"):
        _validate_persistent_name("has-dash")
    _validate_persistent_name("valid_name")
    _validate_persistent_name("_underscore")
    _validate_persistent_name("CamelCase")


async def test_persistent_name_length_is_capped():
    """An over-long name must fail here, not deep inside ClickHouse.

    ClickHouse caps table names near ``213 - len(database)`` characters and
    raises an opaque filesystem error past 251, so the boundary check keeps
    the failure a ``ValueError`` at the API boundary.
    """
    _validate_persistent_name("a" * MAX_PERSISTENT_NAME_LEN)
    with pytest.raises(ValueError, match="Invalid persistent name"):
        _validate_persistent_name("a" * (MAX_PERSISTENT_NAME_LEN + 1))


async def test_register_table_stamps_the_active_tenant(orch_ctx):
    """A registry row records who owns the table, for query scoping.

    Looks the row up *by tenant* rather than by table name: the stamp is
    what this covers, and the name gains its tenant prefix separately.
    """
    with active_tenant(7):
        await create_object_from_value([1, 2, 3], name="owned", scope="global")

    async with get_sql_session() as session:
        result = await session.execute(select(TableRegistry.table_name).where(TableRegistry.tenant_id == 7))
        assert result.scalar_one().endswith("owned")


async def test_two_tenants_hold_distinct_objects_of_one_name(orch_ctx):
    """Same name, different tenants — separate tables, separate data."""
    with active_tenant(7):
        await create_object_from_value([1, 2, 3], name="shared", scope="global")
    with active_tenant(8):
        await create_object_from_value([9], name="shared", scope="global")

    with active_tenant(7):
        assert await (await open_object("shared", scope="global")).data() == [1, 2, 3]
    with active_tenant(8):
        assert await (await open_object("shared", scope="global")).data() == [9]


async def test_open_object_does_not_cross_tenants(orch_ctx):
    """Another tenant's object is missing, not forbidden — no existence leak."""
    with active_tenant(7):
        await create_object_from_value([1, 2, 3], name="private", scope="global")

    with active_tenant(8):
        with pytest.raises(ObjectNotFoundError):
            await open_object("private", scope="global")


async def test_listing_shows_only_the_active_tenants_objects(orch_ctx):
    with active_tenant(7):
        await create_object_from_value([1], name="seven_only", scope="global")
    with active_tenant(8):
        await create_object_from_value([2], name="eight_only", scope="global")

    with active_tenant(7):
        assert await list_persistent_objects() == ["seven_only"]
    with active_tenant(8):
        assert await list_persistent_objects() == ["eight_only"]


async def test_purge_leaves_other_tenants_objects_alone(orch_ctx):
    """A purge is scoped to the caller's tenant, not the whole database."""
    with active_tenant(7):
        await create_object_from_value([1], name="mine", scope="global")
    with active_tenant(8):
        await create_object_from_value([2], name="theirs", scope="global")

    with active_tenant(7):
        # ``before`` (not ``after``): chdb reports metadata_modification_time
        # as the epoch, so an after-window can never match on that backend.
        deleted = await delete_persistent_objects(before=datetime(2100, 1, 1))
        assert deleted == ["mine"]
    with active_tenant(8):
        assert await list_persistent_objects() == ["theirs"]


async def test_delete_clears_the_registry_row(orch_ctx):
    """A dropped object must vanish from registry-backed listing too."""
    await create_object_from_value([1], name="short_lived", scope="global")
    await delete_persistent_object("short_lived", scope="global")
    assert "short_lived" not in await list_persistent_objects()
