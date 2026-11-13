"""GitHub App credentials for the build task's clone.

See docs/designs/orchestration.md — "Image source".

With ``AAICLICK_GITHUB_APP_ID`` and ``AAICLICK_GITHUB_APP_PEM_FILE`` set, a
build of a ``github.com`` repo mints an installation token limited to that
repo, read-only, and hands it to git through its environment — never through
argv, the remote URL or a config file.
"""

from __future__ import annotations

import asyncio
import http.client
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import jwt

APP_ID_ENV = "AAICLICK_GITHUB_APP_ID"
PEM_FILE_ENV = "AAICLICK_GITHUB_APP_PEM_FILE"
TOKEN_ENV = "AAICLICK_GIT_TOKEN"

_API = "https://api.github.com"
_HELPER_KEY = "credential.https://github.com.helper"
# git runs a "!"-prefixed helper through the shell with the action appended;
# the token is read from the environment, so it never appears in git's config.
_HELPER = f'!f() {{ test "$1" = get && printf "username=x-access-token\\npassword=%s\\n" "${TOKEN_ENV}"; }}; f'

# https (optional userinfo), ssh:// and scp-style remotes on github.com. The
# host is followed by its separator, so "github.com.evil.test" cannot match.
# Owner and repo are GitHub's name alphabet only: both land in an API path.
_REMOTE_RE = re.compile(
    r"^(?:https://(?:[^@/]+@)?github\.com/|ssh://git@github\.com/|git@github\.com:)"
    r"(?P<owner>[A-Za-z0-9_.-]+)/(?P<repo>[A-Za-z0-9_.-]+?)(?:\.git)?/?\Z",
    re.IGNORECASE,
)


def _api(
    method: str, path: str, jwt_token: str, body: dict[str, object] | None = None
) -> tuple[int, dict[str, object]]:
    """One GitHub API call authenticated as the App: ``(status, json)``.

    HTTP error statuses are returned, not raised; a transport failure or a
    non-JSON reply raises ``RuntimeError`` (which never carries the JWT)."""
    request = urllib.request.Request(
        _API + path,
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "aaiclick",
        },
    )
    try:
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as e:
            status, raw = e.code, e.read()
    except (OSError, http.client.HTTPException) as e:
        reason = e.reason if isinstance(e, urllib.error.URLError) else type(e).__name__
        raise RuntimeError(f"GitHub App: {method} api.github.com{path} failed: {reason}") from None
    try:
        data = json.loads(raw)
    except ValueError:
        data = None
    if isinstance(data, dict):
        return status, data
    if status >= 400:
        return status, {}  # a proxy's HTML error page: the status is the message
    raise RuntimeError(f"GitHub App: {method} api.github.com{path} failed: reply was not a JSON object")


def _sign(app_id: str, pem_file: str) -> str:
    """A 10-minute App JWT, backdated a minute against clock drift."""
    path = Path(pem_file)
    if path.exists() and not path.is_file():
        raise RuntimeError(
            f"{PEM_FILE_ENV}={pem_file} is not a file. Compose creates a directory when the "
            f"mounted host file is missing: put the key there and recreate the worker."
        )
    try:
        key = path.read_bytes()
    except OSError as e:
        raise RuntimeError(f"{PEM_FILE_ENV}={pem_file} cannot be read: {e.strerror}") from None
    now = int(time.time())
    try:
        return jwt.encode({"iat": now - 60, "exp": now + 540, "iss": app_id}, key, algorithm="RS256")
    except (ValueError, TypeError, jwt.PyJWTError):
        raise RuntimeError(
            f"{PEM_FILE_ENV}={pem_file} is not an RSA private key in PEM format; "
            f"use the .pem GitHub generated for the App."
        ) from None


def _expect(status: int, wanted: int, data: dict[str, object], field: str, what: str) -> object:
    """``data[field]`` from a ``wanted`` reply; anything else raises."""
    if status != wanted:
        raise RuntimeError(f"GitHub App: {what} returned {status}: {data.get('message', '')}")
    if not data.get(field):
        raise RuntimeError(f"GitHub App: {what} returned no {field}")
    return data[field]


def _git_env(remote: str, repo_path: str, token: str) -> dict[str, str]:
    """Env overlay registering a one-shot credential helper for github.com."""
    entries = [
        # An empty value resets the helper list, so a helper from the worker's
        # gitconfig is neither asked first nor handed the token to store.
        (_HELPER_KEY, ""),
        (_HELPER_KEY, _HELPER),
    ]
    canonical = f"https://github.com/{repo_path}"
    if remote != canonical:
        entries.append((f"url.{canonical}.insteadOf", remote))
    existing = os.environ.get("GIT_CONFIG_COUNT", "")
    first = int(existing) if existing.isdecimal() else 0
    env = {"GIT_TERMINAL_PROMPT": "0", TOKEN_ENV: token, "GIT_CONFIG_COUNT": str(first + len(entries))}
    for i, (key, value) in enumerate(entries, start=first):
        env[f"GIT_CONFIG_KEY_{i}"] = key
        env[f"GIT_CONFIG_VALUE_{i}"] = value
    return env


async def git_auth_env(remote: str) -> dict[str, str] | None:
    """Env overlay that lets git authenticate to ``remote``, or None.

    None when the App is not configured, ``remote`` is not a ``github.com``
    repo, or the App is not installed on it (the clone then proceeds
    anonymously). A half-configured App raises whatever the remote."""
    app_id = os.environ.get(APP_ID_ENV)
    pem_file = os.environ.get(PEM_FILE_ENV)
    if not app_id and not pem_file:
        return None
    if not app_id or not pem_file:
        missing, present = (APP_ID_ENV, PEM_FILE_ENV) if not app_id else (PEM_FILE_ENV, APP_ID_ENV)
        raise RuntimeError(f"{present} is set but {missing} is not; GitHub App builds need both.")
    match = _REMOTE_RE.match(remote)
    if match is None:
        return None
    repo = match["repo"]
    if repo in (".", "..") or match["owner"] in (".", ".."):
        return None
    repo_path = f"{match['owner']}/{repo}"

    signed = _sign(app_id, pem_file)
    status, data = await asyncio.to_thread(_api, "GET", f"/repos/{repo_path}/installation", signed)
    if status == 404:
        print(
            f"GitHub App {app_id} is not installed on {repo_path}; cloning without credentials.",
            file=sys.stderr,
        )
        return None
    installation = _expect(status, 200, data, "id", f"installation lookup for {repo_path}")
    status, data = await asyncio.to_thread(
        _api,
        "POST",
        f"/app/installations/{installation}/access_tokens",
        signed,
        {"repositories": [repo], "permissions": {"contents": "read"}},
    )
    token = _expect(status, 201, data, "token", f"token request for {repo_path}")
    return _git_env(remote, repo_path, str(token))
