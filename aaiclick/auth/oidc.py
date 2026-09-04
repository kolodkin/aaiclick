"""OpenID Connect client primitives: discovery, PKCE, the code exchange, and
``id_token`` validation. Pure functions over an ``httpx.AsyncClient`` — no DB,
no contextvars — so ``internal_api.auth`` owns the login-state bookkeeping and
tests can drive everything through ``httpx.MockTransport``.
"""

from __future__ import annotations

import base64
import hashlib
import secrets
from typing import Any, NamedTuple
from urllib.parse import urlencode

import httpx
import jwt

DISCOVERY_PATH = "/.well-known/openid-configuration"
ID_TOKEN_ALGORITHMS = ["RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512"]
HTTP_TIMEOUT = 10.0


class OidcError(Exception):
    """The provider rejected the request or returned something unusable."""


class ProviderMetadata(NamedTuple):
    authorization_endpoint: str
    token_endpoint: str
    jwks_uri: str


class Pkce(NamedTuple):
    verifier: str
    challenge: str


class IdentityClaims(NamedTuple):
    subject: str
    username: str | None
    email: str | None


def http_client() -> httpx.AsyncClient:
    """The client every provider call goes through — tests monkeypatch this."""
    return httpx.AsyncClient(timeout=HTTP_TIMEOUT)


def generate_pkce() -> Pkce:
    verifier = secrets.token_urlsafe(48)
    digest = hashlib.sha256(verifier.encode()).digest()
    return Pkce(verifier=verifier, challenge=base64.urlsafe_b64encode(digest).rstrip(b"=").decode())


def generate_state() -> str:
    return secrets.token_urlsafe(32)


def generate_nonce() -> str:
    return secrets.token_urlsafe(32)


async def discover(client: httpx.AsyncClient, issuer: str) -> ProviderMetadata:
    response = await client.get(issuer + DISCOVERY_PATH)
    if response.status_code != 200:
        raise OidcError(f"discovery failed: HTTP {response.status_code}")
    doc = response.json()
    try:
        return ProviderMetadata(
            authorization_endpoint=doc["authorization_endpoint"],
            token_endpoint=doc["token_endpoint"],
            jwks_uri=doc["jwks_uri"],
        )
    except KeyError as exc:
        raise OidcError(f"discovery document lacks {exc}") from exc


def authorization_url(
    metadata: ProviderMetadata,
    *,
    client_id: str,
    redirect_uri: str,
    scopes: str,
    state: str,
    nonce: str,
    pkce: Pkce,
) -> str:
    query = urlencode(
        {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": scopes,
            "state": state,
            "nonce": nonce,
            "code_challenge": pkce.challenge,
            "code_challenge_method": "S256",
        }
    )
    separator = "&" if "?" in metadata.authorization_endpoint else "?"
    return f"{metadata.authorization_endpoint}{separator}{query}"


async def exchange_code(
    client: httpx.AsyncClient,
    metadata: ProviderMetadata,
    *,
    code: str,
    redirect_uri: str,
    client_id: str,
    client_secret: str | None,
    code_verifier: str,
) -> str:
    """Trade the authorization code for the ``id_token`` (raw JWT)."""
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "code_verifier": code_verifier,
    }
    if client_secret is not None:
        form["client_secret"] = client_secret
    response = await client.post(metadata.token_endpoint, data=form)
    if response.status_code != 200:
        raise OidcError(f"token exchange failed: HTTP {response.status_code}")
    id_token = response.json().get("id_token")
    if not id_token:
        raise OidcError("token response lacks id_token")
    return id_token


async def fetch_jwks(client: httpx.AsyncClient, metadata: ProviderMetadata) -> dict[str, Any]:
    response = await client.get(metadata.jwks_uri)
    if response.status_code != 200:
        raise OidcError(f"jwks fetch failed: HTTP {response.status_code}")
    return response.json()


def validate_id_token(
    id_token: str,
    jwks: dict[str, Any],
    *,
    issuer: str,
    client_id: str,
    nonce: str,
    username_claim: str,
) -> IdentityClaims:
    """Verify signature (by ``kid`` against the JWKS), ``iss``, ``aud``, ``exp``,
    and ``nonce``; return the identity claims aaiclick maps to a user."""
    try:
        kid = jwt.get_unverified_header(id_token).get("kid")
        key_set = jwt.PyJWKSet.from_dict(jwks)
        key = key_set[kid] if kid is not None else key_set.keys[0]
        claims = jwt.decode(
            id_token,
            key=key.key,
            algorithms=ID_TOKEN_ALGORITHMS,
            audience=client_id,
            issuer=issuer,
            options={"require": ["exp", "iat", "sub"]},
        )
    except (jwt.PyJWTError, KeyError, IndexError) as exc:
        raise OidcError(f"invalid id_token: {exc}") from exc
    if claims.get("nonce") != nonce:
        raise OidcError("id_token nonce mismatch")
    username = claims.get(username_claim) or claims.get("email")
    return IdentityClaims(subject=str(claims["sub"]), username=username, email=claims.get("email"))


def subject_key(issuer: str, subject: str) -> str:
    """The value stored in ``users.oidc_subject`` — issuer-qualified so two
    providers' subjects can never collide."""
    return f"{issuer}|{subject}"
