"""Tests for GitHub App build credentials.

``git_auth_env`` is a pure function of the worker env, the remote and
GitHub's replies: the returned overlay *is* the contract, so GitHub is
stubbed at ``_api`` and git is exercised for real in one test.
"""

from __future__ import annotations

import io
import os
import shutil
import subprocess
import urllib.error
import urllib.request

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from . import github_app
from .github_app import git_auth_env

TOKEN = "ghs_secret_tok"
HELPER_KEY = "credential.https://github.com.helper"


class _Reply(io.BytesIO):
    """A 200 response body, as ``urlopen`` returns it."""

    status = 200


@pytest.fixture(scope="module")
def private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def app_env(monkeypatch, tmp_path, private_key):
    """Both settings set, pointing at a real key; no inherited git config count."""
    pem = tmp_path / "app.pem"
    pem.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
    )
    monkeypatch.setenv("AAICLICK_GITHUB_APP_ID", "123")
    monkeypatch.setenv("AAICLICK_GITHUB_APP_PEM_FILE", str(pem))
    monkeypatch.delenv("GIT_CONFIG_COUNT", raising=False)
    return pem


@pytest.fixture
def api(monkeypatch):
    """Stub GitHub: record calls, answer from ``replies`` keyed by method."""
    calls: list[tuple[str, str, str, dict | None]] = []
    replies = {"GET": (200, {"id": 42}), "POST": (201, {"token": TOKEN})}

    def fake(method, path, jwt_token, body=None):
        calls.append((method, path, jwt_token, body))
        return replies[method]

    monkeypatch.setattr(github_app, "_api", fake)
    fake.calls = calls
    fake.replies = replies
    return fake


def _config(env: dict[str, str]) -> list[tuple[str, str]]:
    """The overlay's own git config entries, in order."""
    count = int(env["GIT_CONFIG_COUNT"])
    return [
        (env[f"GIT_CONFIG_KEY_{i}"], env[f"GIT_CONFIG_VALUE_{i}"]) for i in range(count) if f"GIT_CONFIG_KEY_{i}" in env
    ]


async def test_unset_settings_return_none(monkeypatch, api):
    monkeypatch.delenv("AAICLICK_GITHUB_APP_ID", raising=False)
    monkeypatch.delenv("AAICLICK_GITHUB_APP_PEM_FILE", raising=False)

    assert await git_auth_env("https://github.com/o/r.git") is None
    assert api.calls == []


@pytest.mark.parametrize(
    "present, missing",
    [
        pytest.param("AAICLICK_GITHUB_APP_ID", "AAICLICK_GITHUB_APP_PEM_FILE", id="pem-missing"),
        pytest.param("AAICLICK_GITHUB_APP_PEM_FILE", "AAICLICK_GITHUB_APP_ID", id="id-missing"),
    ],
)
async def test_half_configured_raises_whatever_the_remote(monkeypatch, api, present, missing):
    monkeypatch.setenv(present, "x")
    monkeypatch.delenv(missing, raising=False)

    with pytest.raises(RuntimeError, match=missing):
        await git_auth_env("https://gitlab.com/o/r.git")


@pytest.mark.parametrize(
    "remote, repo_path",
    [
        pytest.param("https://github.com/o/r", "o/r", id="https"),
        pytest.param("https://github.com/o/r.git", "o/r", id="https-dot-git"),
        pytest.param("https://GitHub.com/o/r/", "o/r", id="mixed-case-trailing-slash"),
        pytest.param("https://user@github.com/o/r.git", "o/r", id="https-userinfo"),
        pytest.param("git@github.com:o/r.git", "o/r", id="scp-style"),
        pytest.param("ssh://git@github.com/o/r", "o/r", id="ssh-url"),
        pytest.param("https://github.com/o/my.repo.git", "o/my.repo", id="dots-in-name"),
    ],
)
async def test_github_remotes_are_recognised(app_env, api, remote, repo_path):
    assert await git_auth_env(remote) is not None
    assert api.calls[0][:2] == ("GET", f"/repos/{repo_path}/installation")


@pytest.mark.parametrize(
    "remote",
    [
        pytest.param("https://github.com.evil.test/o/r", id="lookalike-suffix"),
        pytest.param("https://notgithub.com/o/r", id="lookalike-prefix"),
        pytest.param("https://github.com@evil.test/o/r", id="lookalike-userinfo"),
        pytest.param("https://gitlab.com/o/r.git", id="other-host"),
        pytest.param("https://github.com/o", id="no-repo"),
        pytest.param("http://github.com/o/r", id="plain-http"),
    ],
)
async def test_other_remotes_get_no_token(app_env, api, remote):
    assert await git_auth_env(remote) is None
    assert api.calls == []


async def test_token_request_is_scoped_to_the_repo_read_only(app_env, api, private_key):
    await git_auth_env("https://github.com/o/r.git")

    (_, _, signed, _), (method, path, signed_again, body) = api.calls
    assert (method, path) == ("POST", "/app/installations/42/access_tokens")
    assert body == {"repositories": ["r"], "permissions": {"contents": "read"}}
    assert signed == signed_again
    claims = jwt.decode(signed, private_key.public_key(), algorithms=["RS256"])
    assert claims["iss"] == "123"
    assert claims["exp"] - claims["iat"] == 600


async def test_overlay_for_https_remote(app_env, api):
    env = await git_auth_env("https://github.com/o/r")

    assert env["GIT_TERMINAL_PROMPT"] == "0"
    assert env["AAICLICK_GIT_TOKEN"] == TOKEN
    (reset_key, reset), (helper_key, helper) = _config(env)
    assert (reset_key, reset) == (HELPER_KEY, "")
    assert helper_key == HELPER_KEY
    assert helper.startswith("!") and "AAICLICK_GIT_TOKEN" in helper and TOKEN not in helper


async def test_overlay_rewrites_a_non_canonical_remote_to_https(app_env, api):
    remote = "git@github.com:o/r.git"

    env = await git_auth_env(remote)

    assert _config(env)[2] == ("url.https://github.com/o/r.insteadOf", remote)
    assert env["GIT_CONFIG_COUNT"] == "3"


@pytest.mark.parametrize(
    "existing, first_index",
    [
        pytest.param("2", 2, id="continues"),
        pytest.param("abc", 0, id="non-numeric-counts-as-zero"),
    ],
)
async def test_overlay_numbers_after_existing_git_config(monkeypatch, app_env, api, existing, first_index):
    monkeypatch.setenv("GIT_CONFIG_COUNT", existing)

    env = await git_auth_env("https://github.com/o/r")

    assert env["GIT_CONFIG_COUNT"] == str(first_index + 2)
    assert env[f"GIT_CONFIG_KEY_{first_index}"] == HELPER_KEY
    assert f"GIT_CONFIG_KEY_{first_index + 2}" not in env


async def test_app_not_installed_warns_and_clones_anonymously(app_env, api, capsys):
    api.replies["GET"] = (404, {"message": "Not Found"})

    assert await git_auth_env("https://github.com/o/r") is None

    assert [c[0] for c in api.calls] == ["GET"]
    assert "o/r" in capsys.readouterr().err


def _break_pem_missing(pem, api):
    pem.unlink()


def _break_pem_is_directory(pem, api):
    pem.unlink()
    pem.mkdir()


def _break_pem_is_garbage(pem, api):
    pem.write_text("not a key")


def _break_installation_lookup(pem, api):
    api.replies["GET"] = (500, {"message": "boom-lookup"})


def _break_token_request(pem, api):
    api.replies["POST"] = (403, {"message": "boom-token"})


@pytest.mark.parametrize(
    "break_it, match",
    [
        pytest.param(_break_pem_missing, "AAICLICK_GITHUB_APP_PEM_FILE", id="pem-missing"),
        pytest.param(_break_pem_is_directory, "not a file", id="pem-is-directory"),
        pytest.param(_break_pem_is_garbage, "private key", id="pem-is-garbage"),
        pytest.param(_break_installation_lookup, "500.*boom-lookup", id="installation-500"),
        pytest.param(_break_token_request, "403.*boom-token", id="token-403"),
    ],
)
async def test_failures_raise_without_leaking_secrets(app_env, api, break_it, match):
    break_it(app_env, api)

    with pytest.raises(RuntimeError, match=match) as excinfo:
        await git_auth_env("https://github.com/o/r")

    message = str(excinfo.value)
    assert TOKEN not in message
    assert all(call[2] not in message for call in api.calls)


@pytest.mark.parametrize(
    "urlopen_result",
    [
        pytest.param(urllib.error.URLError("no route"), id="network-down"),
        pytest.param(_Reply(b"<html>not json</html>"), id="non-json-reply"),
    ],
)
def test_api_wraps_transport_failures(monkeypatch, urlopen_result):
    def fake_urlopen(request, timeout):
        if isinstance(urlopen_result, Exception):
            raise urlopen_result
        return urlopen_result

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(RuntimeError, match="api.github.com") as excinfo:
        github_app._api("GET", "/repos/o/r/installation", "signed.jwt.value")

    assert "signed.jwt.value" not in str(excinfo.value)


@pytest.mark.skipif(shutil.which("git") is None, reason="needs the git binary")
async def test_git_accepts_the_helper_over_one_already_configured(monkeypatch, app_env, api):
    """Real git, no network: our helper answers, and a helper the worker
    already had (here via env config) is neither asked first nor kept."""
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "credential.helper")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "!echo username=stale; echo password=stale; :")

    env = await git_auth_env("https://github.com/o/r")

    out = subprocess.run(
        ["git", "credential", "fill"],
        input="protocol=https\nhost=github.com\n\n",
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert "username=x-access-token" in out
    assert f"password={TOKEN}" in out
    assert "stale" not in out
