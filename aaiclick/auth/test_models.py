import pytest
from sqlmodel import select

from aaiclick.auth.models import (
    ROLE_ADMIN,
    ROLE_MEMBER,
    ROLE_SCOPES,
    ROLE_VIEWER,
    ROLES,
    SCOPE_ADMIN,
    SCOPE_LEVELS,
    SCOPE_READ,
    SCOPE_WRITE,
    ApiToken,
    RefreshToken,
    User,
    scope_admits,
)
from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id


async def test_user_round_trips(orch_ctx):
    uid = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(User(id=uid, username="alice", password_hash="x", role=ROLE_ADMIN))
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(User).where(User.username == "alice"))).scalar_one()
        assert row.id == uid
        assert row.role == ROLE_ADMIN
        assert row.disabled is False


async def test_user_role_defaults_to_viewer(orch_ctx):
    """Least privilege: a user created without an explicit role only reads."""
    uid = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(User(id=uid, username="bob", password_hash="x"))
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(User).where(User.id == uid))).scalar_one()
        assert row.role == ROLE_VIEWER


async def test_refresh_token_round_trips(orch_ctx):
    uid = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(User(id=uid, username="carol", password_hash="x"))
        # No ORM relationship links the two models, so flush to guarantee the
        # user row exists before the FK'd token insert (Postgres enforces it).
        await session.flush()
        session.add(RefreshToken(id=get_snowflake_id(), user_id=uid, token_hash="h", expires_at=utc_now()))
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(RefreshToken).where(RefreshToken.user_id == uid))).scalar_one()
        assert row.token_hash == "h"


@pytest.mark.parametrize(
    "held, required, expected",
    [
        pytest.param(SCOPE_READ, SCOPE_READ, True, id="read-admits-read"),
        pytest.param(SCOPE_READ, SCOPE_WRITE, False, id="read-refuses-write"),
        pytest.param(SCOPE_WRITE, SCOPE_READ, True, id="write-admits-read"),
        pytest.param(SCOPE_WRITE, SCOPE_ADMIN, False, id="write-refuses-admin"),
        pytest.param(SCOPE_ADMIN, SCOPE_WRITE, True, id="admin-admits-write"),
        pytest.param(SCOPE_ADMIN, SCOPE_ADMIN, True, id="admin-admits-admin"),
    ],
)
def test_scope_admits(held, required, expected):
    assert scope_admits(held, required) is expected


def test_scope_levels_are_ordered_low_to_high():
    """The index is the comparison — three rungs, nothing above admin."""
    assert SCOPE_LEVELS == (SCOPE_READ, SCOPE_WRITE, SCOPE_ADMIN)
    assert ROLES == (ROLE_VIEWER, ROLE_MEMBER, ROLE_ADMIN)


async def test_api_token_carries_its_scope(orch_ctx):
    uid = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(User(id=uid, username="tok", password_hash="x"))
        await session.flush()
        session.add(
            ApiToken(id=get_snowflake_id(), user_id=uid, name="ci", prefix="aaic_abc", token_hash="h", scope=SCOPE_ADMIN)
        )
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(ApiToken).where(ApiToken.user_id == uid))).scalar_one()
        assert row.scope == SCOPE_ADMIN


@pytest.mark.parametrize(
    "role, scope",
    [
        pytest.param(ROLE_VIEWER, SCOPE_READ, id="viewer-reads"),
        pytest.param(ROLE_MEMBER, SCOPE_WRITE, id="member-writes"),
        pytest.param(ROLE_ADMIN, SCOPE_ADMIN, id="admin-admins"),
    ],
)
def test_every_role_maps_to_one_scope(role, scope):
    """The single bridge between the vocabularies — authorization compares scopes."""
    assert ROLE_SCOPES[role] == scope
