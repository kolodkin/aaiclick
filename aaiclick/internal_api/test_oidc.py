"""OIDC login flow against a fake provider served by ``httpx.MockTransport``.

The provider signs ``id_token``s with an RSA key generated per module and
publishes it through the JWKS endpoint, so validation runs the real path.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from aaiclick.auth import oidc, security, store
from aaiclick.auth.view_models import CreateUserRequest, OidcCallbackRequest
from aaiclick.internal_api import auth, users
from aaiclick.internal_api.errors import Invalid, Unauthorized

SECRET = "internal-api-oidc-test-secret-key-32-plus-bytes"
ISSUER = "https://idp.example.com"
CLIENT_ID = "aaiclick-client"
KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
KID = "test-key"


class FakeProvider:
    """Records what the callback sends and mints id_tokens for the claims it is told to."""

    def __init__(self) -> None:
        self.claims: dict = {}
        self.token_requests: list[dict[str, list[str]]] = []
        self.nonce_override: str | None = None

    def id_token(self, nonce: str) -> str:
        now = datetime.now(timezone.utc)
        payload = {
            "iss": ISSUER,
            "aud": CLIENT_ID,
            "sub": "subject-1",
            "iat": now,
            "exp": now + timedelta(minutes=5),
            "nonce": self.nonce_override or nonce,
            **self.claims,
        }
        return jwt.encode(payload, KEY, algorithm="RS256", headers={"kid": KID})

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/.well-known/openid-configuration":
            return httpx.Response(
                200,
                json={
                    "authorization_endpoint": f"{ISSUER}/authorize",
                    "token_endpoint": f"{ISSUER}/token",
                    "jwks_uri": f"{ISSUER}/jwks",
                },
            )
        if path == "/jwks":
            public = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(KEY.public_key()))
            return httpx.Response(200, json={"keys": [{**public, "kid": KID, "use": "sig", "alg": "RS256"}]})
        if path == "/token":
            form = parse_qs(request.content.decode())
            self.token_requests.append(form)
            # The nonce the client expects travels in the login state; the
            # test hands it over through ``last_nonce`` before the callback.
            return httpx.Response(200, json={"id_token": self.id_token(self.last_nonce)})
        return httpx.Response(404)

    last_nonce = ""


@pytest.fixture
def provider(monkeypatch):
    fake = FakeProvider()
    monkeypatch.setenv("AAICLICK_OIDC_ISSUER", ISSUER)
    monkeypatch.setenv("AAICLICK_OIDC_CLIENT_ID", CLIENT_ID)
    monkeypatch.setenv("AAICLICK_OIDC_CLIENT_SECRET", "s3cret")
    monkeypatch.setenv("AAICLICK_PUBLIC_URL", "https://aaiclick.example.com/")
    monkeypatch.delenv("AAICLICK_OIDC_AUTO_PROVISION", raising=False)
    monkeypatch.setattr(oidc, "http_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(fake.handle)))
    return fake


async def _start(provider: FakeProvider) -> str:
    """Run ``oidc_start`` and return the ``state`` the browser would echo back."""
    view = await auth.oidc_start()
    query = parse_qs(urlparse(view.authorization_url).query)
    assert query["code_challenge_method"] == ["S256"] and query["redirect_uri"] == ["https://aaiclick.example.com/"]
    state = query["state"][0]
    provider.last_nonce = query["nonce"][0]
    return state


def test_oidc_config_reflects_settings(monkeypatch):
    monkeypatch.delenv("AAICLICK_OIDC_ISSUER", raising=False)
    assert auth.oidc_config().enabled is False
    monkeypatch.setenv("AAICLICK_OIDC_ISSUER", ISSUER)
    monkeypatch.setenv("AAICLICK_OIDC_CLIENT_ID", CLIENT_ID)
    monkeypatch.setenv("AAICLICK_OIDC_LABEL", "Okta")
    assert auth.oidc_config() == auth.OidcConfigView(enabled=True, label="Okta")


async def test_start_requires_configuration(orch_ctx, monkeypatch):
    monkeypatch.delenv("AAICLICK_OIDC_ISSUER", raising=False)
    with pytest.raises(Invalid):
        await auth.oidc_start()


async def test_callback_provisions_and_links_user(orch_ctx, provider):
    provider.claims = {"preferred_username": "sso_sam", "email": "sam@example.com"}
    state = await _start(provider)

    pair = await auth.oidc_callback(OidcCallbackRequest(code="abc", state=state), secret=SECRET)

    claims = security.decode_access_token(pair.access_token, SECRET)
    user = await store.get_user_by_id(claims.user_id)
    assert user is not None and user.username == "sso_sam" and user.email == "sam@example.com"
    assert user.oidc_subject == f"{ISSUER}|subject-1" and user.password_hash is None
    form = provider.token_requests[-1]
    assert form["code"] == ["abc"] and form["client_secret"] == ["s3cret"] and "code_verifier" in form


async def test_callback_links_existing_password_user_by_username(orch_ctx, provider):
    existing = await users.create_user(CreateUserRequest(username="alice", password="pw"))
    provider.claims = {"preferred_username": "alice"}
    state = await _start(provider)

    pair = await auth.oidc_callback(OidcCallbackRequest(code="abc", state=state), secret=SECRET)

    assert security.decode_access_token(pair.access_token, SECRET).user_id == existing.id
    linked = await store.get_user_by_id(existing.id)
    assert linked is not None and linked.oidc_subject == f"{ISSUER}|subject-1"


async def test_state_is_single_use(orch_ctx, provider):
    provider.claims = {"preferred_username": "once"}
    state = await _start(provider)
    await auth.oidc_callback(OidcCallbackRequest(code="abc", state=state), secret=SECRET)
    with pytest.raises(Unauthorized):
        await auth.oidc_callback(OidcCallbackRequest(code="abc", state=state), secret=SECRET)
    with pytest.raises(Unauthorized):
        await auth.oidc_callback(OidcCallbackRequest(code="abc", state="never-issued"), secret=SECRET)


async def test_nonce_mismatch_rejected(orch_ctx, provider):
    provider.claims = {"preferred_username": "evil"}
    provider.nonce_override = "replayed"
    state = await _start(provider)
    with pytest.raises(Unauthorized, match="nonce"):
        await auth.oidc_callback(OidcCallbackRequest(code="abc", state=state), secret=SECRET)


async def test_auto_provision_off_rejects_unknown(orch_ctx, provider, monkeypatch):
    monkeypatch.setenv("AAICLICK_OIDC_AUTO_PROVISION", "0")
    provider.claims = {"preferred_username": "stranger"}
    state = await _start(provider)
    with pytest.raises(Unauthorized):
        await auth.oidc_callback(OidcCallbackRequest(code="abc", state=state), secret=SECRET)
    assert await store.get_user_by_username("stranger") is None


async def test_disabled_user_rejected(orch_ctx, provider):
    view = await users.create_user(CreateUserRequest(username="off", password="pw"))
    await users.disable_user(view.id, True)
    provider.claims = {"preferred_username": "off"}
    state = await _start(provider)
    with pytest.raises(Unauthorized):
        await auth.oidc_callback(OidcCallbackRequest(code="abc", state=state), secret=SECRET)
