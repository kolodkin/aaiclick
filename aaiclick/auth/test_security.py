import jwt
import pytest

from aaiclick.auth import security

SECRET = "unit-test-secret-key-at-least-32-bytes-long"


def test_password_round_trip():
    h = security.hash_password("hunter2")
    assert h != "hunter2"
    assert security.verify_password("hunter2", h) is True
    assert security.verify_password("wrong", h) is False


def test_sha256_hex_stable():
    assert security.sha256_hex("abc") == security.sha256_hex("abc")
    assert security.sha256_hex("abc") != security.sha256_hex("abd")


def test_generate_secret_unique():
    assert security.generate_secret() != security.generate_secret()


def test_access_token_round_trip():
    token = security.encode_access_token(user_id=42, superadmin=True, tenants={5: "admin", 6: "viewer"}, secret=SECRET, ttl=60)
    claims = security.decode_access_token(token, SECRET)
    assert claims.user_id == 42
    assert claims.superadmin is True
    assert claims.tenants == {5: "admin", 6: "viewer"}


def test_access_token_defaults_missing_claims():
    """Tokens minted without the new claims decode to no-access defaults."""
    token = jwt.encode({"sub": "1", "type": "access"}, SECRET, algorithm="HS256")
    claims = security.decode_access_token(token, SECRET)
    assert claims.superadmin is False and claims.tenants == {}


def test_decode_rejects_bad_signature():
    token = security.encode_access_token(user_id=1, superadmin=False, tenants={}, secret=SECRET, ttl=60)
    with pytest.raises(security.TokenError):
        security.decode_access_token(token, "a-different-secret-also-32-plus-bytes-long")


def test_decode_rejects_expired():
    token = security.encode_access_token(user_id=1, superadmin=False, tenants={}, secret=SECRET, ttl=-1)
    with pytest.raises(security.TokenError):
        security.decode_access_token(token, SECRET)


def test_decode_rejects_wrong_type():
    bad = jwt.encode({"sub": "1", "type": "refresh"}, SECRET, algorithm="HS256")
    with pytest.raises(security.TokenError):
        security.decode_access_token(bad, SECRET)
