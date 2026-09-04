"""Internal API for the password-reset lifecycle: mint (superadmin / CLI),
request (self-service mail), redeem (public)."""

from __future__ import annotations

import logging

from aaiclick.auth import config, mail, security, store
from aaiclick.auth.view_models import PasswordResetLinkView, PasswordResetRedeem, PasswordResetRequest

from . import users
from .errors import Unauthorized

logger = logging.getLogger(__name__)


async def create(user_id: int) -> PasswordResetLinkView:
    """Mint a one-time reset token; ``url`` is set when the public URL is known."""
    await users.get_user(user_id)
    token = security.generate_secret()
    row = await store.create_password_reset(
        user_id=user_id, token_hash=security.sha256_hex(token), ttl=config.password_reset_ttl()
    )
    url = config.spa_url(f"reset {token}") if config.public_url() else None
    return PasswordResetLinkView(token=token, expires_at=row.expires_at, url=url)


async def request(request: PasswordResetRequest) -> None:
    """Self-service reset: mail a link when SMTP is configured and the user has
    an email. Silent otherwise — the caller learns nothing about the account."""
    settings = config.smtp_settings()
    user = await store.get_user_by_username(request.username)
    if user is None or user.disabled or user.email is None:
        logger.info("password reset requested for %r: no deliverable account", request.username)
        return
    if settings is None:
        logger.warning("password reset requested for %r but mail is not configured", request.username)
        return
    link = await create(user.id)
    body = (
        f"A password reset was requested for your aaiclick account '{user.username}'.\n\n"
        f"Open this link to choose a new password (valid until {link.expires_at:%Y-%m-%d %H:%M} UTC):\n\n"
        f"{link.url or link.token}\n\n"
        "If you did not request this, ignore this message."
    )
    await mail.send_mail(settings, to=user.email, subject="aaiclick password reset", body=body)


async def redeem(request: PasswordResetRedeem) -> None:
    """Consume a reset token and set the password; sessions are revoked like an
    admin reset."""
    row = await store.consume_password_reset(security.sha256_hex(request.token))
    if row is None:
        raise Unauthorized("unknown, expired, or already used reset token")
    await users.set_password(row.user_id, request.new_password)
