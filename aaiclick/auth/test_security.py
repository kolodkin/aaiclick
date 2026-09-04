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
    token = security.encode_access_token(
        user_id=42, superadmin=True, tenants={5: "admin", 6: "viewer"}, secret=SECRET, ttl=60
    )
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


def test_api_token_format():
    token = security.generate_api_token()
    assert security.is_api_token(token)
    assert not security.is_api_token("eyJhbGciOi")
    assert security.api_token_display_prefix(token) == token[:12]
    assert security.generate_api_token() != token


def test_totp_matches_rfc6238_vector():
    """RFC 6238 Appendix B, SHA-1, 8 digits truncated to our 6: at T=59 the
    code is 94287082 → last six digits 287082."""
    secret = security.base64.b32encode(b"12345678901234567890").decode()
    assert security.totp_code(secret, at=59) == "287082"


def test_verify_totp_accepts_drift_and_rejects_stale():
    secret = security.generate_totp_secret()
    now = 1_700_000_000.0
    code = security.totp_code(secret, at=now)
    assert security.verify_totp(secret, code, at=now)
    assert security.verify_totp(secret, f"{code[:3]} {code[3:]}", at=now + 30)  # one step later, spaces ok
    assert not security.verify_totp(secret, code, at=now + 120)
    assert not security.verify_totp(secret, "000000" if code != "000000" else "111111", at=now)


def test_totp_uri_shape():
    uri = security.totp_uri("ABC234", "al ice")
    assert uri.startswith("otpauth://totp/aaiclick%3Aal%20ice?secret=ABC234&issuer=aaiclick")
