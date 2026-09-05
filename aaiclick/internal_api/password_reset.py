"""Internal API for the password-reset lifecycle: a superadmin (or the CLI)
mints a one-time link, the user redeems it without a session."""

from __future__ import annotations

from aaiclick.auth import config, security, store
from aaiclick.auth.view_models import PasswordResetLinkView, PasswordResetRedeem

from . import users
from .errors import Unauthorized


async def create(user_id: int) -> PasswordResetLinkView:
    """Mint a one-time reset token; ``url`` is set when the public URL is known."""
    await users.get_user(user_id)
    token = security.generate_secret()
    row = await store.create_password_reset(
        user_id=user_id, token_hash=security.sha256_hex(token), ttl=config.password_reset_ttl()
    )
    url = config.spa_url(f"reset {token}") if config.public_url() else None
    return PasswordResetLinkView(token=token, expires_at=row.expires_at, url=url)


async def redeem(request: PasswordResetRedeem) -> None:
    """Consume a reset token and set the password; sessions are revoked like an
    admin reset."""
    row = await store.consume_password_reset(security.sha256_hex(request.token))
    if row is None:
        raise Unauthorized("unknown, expired, or already used reset token")
    await users.set_password(row.user_id, request.new_password)
