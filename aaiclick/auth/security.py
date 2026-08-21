"""Pure auth crypto: password hashing, token secrets, and access-JWT codec.

No DB, no contextvars, no env reads — callers pass secrets/TTLs explicitly.
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

import bcrypt
import jwt

TOKEN_TYPE_ACCESS = "access"


class TokenError(Exception):
    """Access token is missing, malformed, expired, or wrong type."""


class AccessClaims(NamedTuple):
    user_id: int
    superadmin: bool
    tenants: dict[int, str]
    """Membership map ``tenant_id -> role`` at mint time."""


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode(), password_hash.encode())


def generate_secret() -> str:
    """Opaque URL-safe secret for refresh tokens."""
    return secrets.token_urlsafe(32)


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def encode_access_token(*, user_id: int, superadmin: bool, tenants: dict[int, str], secret: str, ttl: int) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "superadmin": superadmin,
        # JSON object keys must be strings; decode converts back to int.
        "tenants": {str(tenant_id): role for tenant_id, role in tenants.items()},
        "type": TOKEN_TYPE_ACCESS,
        "iat": now,
        "exp": now + timedelta(seconds=ttl),
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def decode_access_token(token: str, secret: str) -> AccessClaims:
    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
    except jwt.PyJWTError as exc:
        raise TokenError(str(exc)) from exc
    if payload.get("type") != TOKEN_TYPE_ACCESS:
        raise TokenError("not an access token")
    try:
        return AccessClaims(
            user_id=int(payload["sub"]),
            superadmin=bool(payload.get("superadmin", False)),
            tenants={int(tenant_id): role for tenant_id, role in payload.get("tenants", {}).items()},
        )
    except (KeyError, ValueError) as exc:
        raise TokenError("malformed claims") from exc
