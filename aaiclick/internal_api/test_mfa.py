import pytest

from aaiclick.auth import security, store
from aaiclick.auth.view_models import (
    CreateUserRequest,
    LoginRequest,
    MfaDisableRequest,
    MfaEnableRequest,
    RefreshRequest,
)
from aaiclick.internal_api import auth, users
from aaiclick.internal_api.errors import Conflict, Invalid, MfaRequired, Unauthorized

SECRET = "internal-api-mfa-test-secret-key-32-plus-bytes"


async def _enrolled(username="alice"):
    """A user with MFA enabled; returns (view, totp secret)."""
    view = await users.create_user(CreateUserRequest(username=username, password="pw"))
    setup = await auth.mfa_setup(view.id)
    await auth.mfa_enable(view.id, MfaEnableRequest(code=security.totp_code(setup.secret)))
    return view, setup.secret


async def test_login_demands_code_once_enabled(orch_ctx):
    view, secret = await _enrolled()
    with pytest.raises(MfaRequired):
        await auth.login(LoginRequest(username="alice", password="pw"), secret=SECRET)
    with pytest.raises(Unauthorized):
        await auth.login(LoginRequest(username="alice", password="pw", totp_code="000000"), secret=SECRET)
    pair = await auth.login(
        LoginRequest(username="alice", password="pw", totp_code=security.totp_code(secret)), secret=SECRET
    )
    assert security.decode_access_token(pair.access_token, SECRET).user_id == view.id


async def test_wrong_password_never_reveals_mfa(orch_ctx):
    """A bad password is a plain 401 — ``mfa_required`` must not leak that the
    password was right."""
    await _enrolled()
    with pytest.raises(Unauthorized) as excinfo:
        await auth.login(LoginRequest(username="alice", password="nope"), secret=SECRET)
    assert not isinstance(excinfo.value, MfaRequired)


async def test_setup_is_pending_until_enabled(orch_ctx):
    view = await users.create_user(CreateUserRequest(username="pending", password="pw"))
    setup = await auth.mfa_setup(view.id)
    assert setup.otpauth_uri.startswith("otpauth://totp/")
    assert await auth.login(LoginRequest(username="pending", password="pw"), secret=SECRET)  # not yet enforced
    with pytest.raises(Unauthorized):
        await auth.mfa_enable(view.id, MfaEnableRequest(code="123456"))
    assert (await users.get_user(view.id)).mfa_enabled is False


async def test_enable_revokes_existing_sessions(orch_ctx):
    view = await users.create_user(CreateUserRequest(username="sessions", password="pw"))
    pair = await auth.login(LoginRequest(username="sessions", password="pw"), secret=SECRET)
    setup = await auth.mfa_setup(view.id)
    await auth.mfa_enable(view.id, MfaEnableRequest(code=security.totp_code(setup.secret)))
    with pytest.raises(Unauthorized):
        await auth.refresh(RefreshRequest(refresh_token=pair.refresh_token), secret=SECRET)


async def test_setup_and_enable_guards(orch_ctx):
    view, _secret = await _enrolled("guarded")
    with pytest.raises(Conflict):
        await auth.mfa_setup(view.id)
    with pytest.raises(Conflict):
        await auth.mfa_enable(view.id, MfaEnableRequest(code="123456"))
    fresh = await users.create_user(CreateUserRequest(username="fresh", password="pw"))
    with pytest.raises(Invalid):
        await auth.mfa_enable(fresh.id, MfaEnableRequest(code="123456"))


async def test_disable_needs_both_factors(orch_ctx):
    view, secret = await _enrolled("off")
    with pytest.raises(Unauthorized):
        await auth.mfa_disable(view.id, MfaDisableRequest(password="wrong", code=security.totp_code(secret)))
    with pytest.raises(Unauthorized):
        await auth.mfa_disable(view.id, MfaDisableRequest(password="pw", code="000000"))
    await auth.mfa_disable(view.id, MfaDisableRequest(password="pw", code=security.totp_code(secret)))
    assert await auth.login(LoginRequest(username="off", password="pw"), secret=SECRET)
    with pytest.raises(Conflict):
        await auth.mfa_disable(view.id, MfaDisableRequest(password="pw", code="000000"))


async def test_admin_reset_clears_mfa(orch_ctx):
    view, _secret = await _enrolled("lost")
    reset = await users.reset_mfa(view.id)
    assert reset.mfa_enabled is False
    row = await store.get_user_by_id(view.id)
    assert row is not None and row.totp_secret is None
    assert await auth.login(LoginRequest(username="lost", password="pw"), secret=SECRET)
