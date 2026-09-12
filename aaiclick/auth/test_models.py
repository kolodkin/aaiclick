import pytest
from sqlmodel import select

from aaiclick.auth.models import (
    SCOPE_ADMIN,
    SCOPE_LEVELS,
    SCOPE_READ,
    SCOPE_SUPERADMIN,
    SCOPE_WRITE,
    ApiToken,
    RefreshToken,
    User,
    scope_admits,
)
from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id
from aaiclick.tenancy import DEFAULT_TENANT_ID


async def test_user_round_trips(orch_ctx):
    uid = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(User(id=uid, username="alice", password_hash="x", superadmin=True))
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(User).where(User.username == "alice"))).scalar_one()
        assert row.id == uid
        assert row.superadmin is True
        assert row.disabled is False


async def test_refresh_token_round_trips(orch_ctx):
    uid = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(User(id=uid, username="bob", password_hash="x"))
        # No ORM relationship links the two models, so flush to guarantee the
        # user row exists before the FK'd token insert (Postgres enforces it).
        await session.flush()
        session.add(RefreshToken(id=get_snowflake_id(), user_id=uid, token_hash="h", expires_at=utc_now()))
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(RefreshToken).where(RefreshToken.user_id == uid))).scalar_one()
        assert row.token_hash == "h"
        assert row.rotated_at is None


@pytest.mark.parametrize(
    "held, required, expected",
    [
        pytest.param(SCOPE_READ, SCOPE_READ, True, id="read-admits-read"),
        pytest.param(SCOPE_READ, SCOPE_WRITE, False, id="read-refuses-write"),
        pytest.param(SCOPE_WRITE, SCOPE_READ, True, id="write-admits-read"),
        pytest.param(SCOPE_WRITE, SCOPE_ADMIN, False, id="write-refuses-admin"),
        pytest.param(SCOPE_ADMIN, SCOPE_WRITE, True, id="admin-admits-write"),
        pytest.param(SCOPE_ADMIN, SCOPE_SUPERADMIN, False, id="admin-refuses-superadmin"),
        pytest.param(SCOPE_SUPERADMIN, SCOPE_ADMIN, True, id="superadmin-admits-admin"),
    ],
)
def test_scope_admits(held, required, expected):
    assert scope_admits(held, required) is expected


def test_scope_levels_are_ordered_low_to_high():
    """The tuple order *is* the comparison — a reordering silently changes every gate."""
    assert SCOPE_LEVELS == (SCOPE_READ, SCOPE_WRITE, SCOPE_ADMIN, SCOPE_SUPERADMIN)


async def test_api_token_carries_its_tenant(orch_ctx):
    uid = get_snowflake_id()
    async with get_sql_session() as session:
        session.add(User(id=uid, username="tok", password_hash="x"))
        await session.flush()
        session.add(
            ApiToken(
                id=get_snowflake_id(),
                user_id=uid,
                name="ci",
                prefix="aaic_abc",
                token_hash="h",
                scope=SCOPE_ADMIN,
                tenant_id=DEFAULT_TENANT_ID,
            )
        )
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(ApiToken).where(ApiToken.user_id == uid))).scalar_one()
        assert row.tenant_id == DEFAULT_TENANT_ID and row.scope == SCOPE_ADMIN
