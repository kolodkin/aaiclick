"""Pure auth crypto: password hashing, token secrets, and access-JWT codec.

No DB, no contextvars, no env reads — callers pass secrets/TTLs explicitly.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import struct
import time
from datetime import datetime, timedelta, timezone
from typing import NamedTuple
from urllib.parse import quote

import bcrypt
import jwt

TOKEN_TYPE_ACCESS = "access"
API_TOKEN_PREFIX = "aaic_"
API_TOKEN_DISPLAY_CHARS = 12
"""How many leading characters of an API token are kept for display."""

TOTP_STEP_SECONDS = 30
TOTP_DIGITS = 6
TOTP_DRIFT_STEPS = 1
"""Accept codes from this many steps either side of now — clock skew tolerance."""
TOTP_ISSUER = "aaiclick"


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


def generate_api_token() -> str:
    """Opaque API token. The fixed prefix lets the resolver route it without a
    JWT parse attempt and lets secret scanners recognise it."""
    return API_TOKEN_PREFIX + secrets.token_urlsafe(32)


def is_api_token(credential: str) -> bool:
    return credential.startswith(API_TOKEN_PREFIX)


def api_token_display_prefix(token: str) -> str:
    return token[:API_TOKEN_DISPLAY_CHARS]


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


# --- TOTP (RFC 6238 over HOTP / RFC 4226) ---------------------------------
# Standard-library only: HMAC-SHA1, 30 s steps, 6 digits — what every
# authenticator app expects from an ``otpauth://`` URI.


def generate_totp_secret() -> str:
    """Base32 seed (160 bits), the format authenticator apps accept by hand."""
    return base64.b32encode(secrets.token_bytes(20)).decode()


def totp_code(secret: str, at: float | None = None) -> str:
    counter = int((time.time() if at is None else at) // TOTP_STEP_SECONDS)
    key = base64.b32decode(secret, casefold=True)
    digest = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    number = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(number % (10**TOTP_DIGITS)).zfill(TOTP_DIGITS)


def verify_totp(secret: str, code: str, at: float | None = None) -> bool:
    """Constant-time check of ``code`` against the current step ± drift."""
    now = time.time() if at is None else at
    candidate = code.strip().replace(" ", "")
    return any(
        hmac.compare_digest(totp_code(secret, now + step * TOTP_STEP_SECONDS), candidate)
        for step in range(-TOTP_DRIFT_STEPS, TOTP_DRIFT_STEPS + 1)
    )


def totp_uri(secret: str, username: str, issuer: str = TOTP_ISSUER) -> str:
    """``otpauth://`` URI for authenticator apps (rendered as a QR code or typed)."""
    label = quote(f"{issuer}:{username}")
    return f"otpauth://totp/{label}?secret={secret}&issuer={quote(issuer)}&algorithm=SHA1&digits={TOTP_DIGITS}&period={TOTP_STEP_SECONDS}"
