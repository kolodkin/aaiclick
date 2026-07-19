"""
aaiclick.ai.ollama - Local Ollama server probing and model bootstrap.

Stdlib + pydantic only (no litellm, no orchestration imports) so it is
importable from anywhere — ``ai_available()`` uses it to gate AI steps, and
``aaiclick setup --ai`` pulls the configured model via
:func:`bootstrap_ollama`.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Literal, NamedTuple

from pydantic import BaseModel

OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_SHOW_TIMEOUT_S = 5
OLLAMA_PULL_TIMEOUT_S = 600
DEFAULT_OLLAMA_MODEL = "ollama/llama3.1:8b"

PROBE_OK = "ok"
PROBE_UNREACHABLE = "unreachable"
PROBE_MISSING = "missing"
PROBE_ERROR = "error"
OllamaProbeStatus = Literal["ok", "unreachable", "missing", "error"]


class OllamaProbe(NamedTuple):
    status: OllamaProbeStatus
    detail: str


OLLAMA_ALREADY_PRESENT = "already_present"
OLLAMA_PULLED = "pulled"
OLLAMA_SERVER_UNREACHABLE = "server_unreachable"
OLLAMA_NOT_OLLAMA = "not_ollama"
OLLAMA_FAILED = "failed"
OllamaBootstrapStatus = Literal[
    "already_present",
    "pulled",
    "server_unreachable",
    "not_ollama",
    "failed",
]
"""Outcome of :func:`bootstrap_ollama`."""


class OllamaBootstrapResult(BaseModel):
    """Response from :func:`bootstrap_ollama`."""

    model: str
    server_url: str
    status: OllamaBootstrapStatus
    detail: str | None = None


def get_configured_model() -> str:
    """Return the active model string: ``AAICLICK_AI_MODEL``, else the Ollama default.

    An empty environment value counts as unset.
    """
    return os.environ.get("AAICLICK_AI_MODEL") or DEFAULT_OLLAMA_MODEL


def probe_ollama_model(model_name: str, *, base_url: str = OLLAMA_BASE_URL) -> OllamaProbe:
    """Check that the local Ollama server has ``model_name`` downloaded.

    A single ``POST /api/show`` distinguishes every case: connection failure
    means the server is down, 404 means the model is not downloaded. Never
    triggers a pull.
    """
    show_req = urllib.request.Request(  # noqa: S310
        f"{base_url}/api/show",
        data=json.dumps({"model": model_name}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(show_req, timeout=OLLAMA_SHOW_TIMEOUT_S)  # noqa: S310
    except urllib.error.HTTPError as exc:
        # exc.close() releases the response stream (Python 3.14's tempfile-backed
        # body would raise ResourceWarning on gc, which filterwarnings=["error"]
        # escalates to a failure).
        code = exc.code
        detail = f"model lookup failed: {exc}"
        exc.close()
        if code == 404:
            return OllamaProbe(PROBE_MISSING, f"model '{model_name}' not downloaded")
        return OllamaProbe(PROBE_ERROR, detail)
    except (urllib.error.URLError, OSError) as exc:
        return OllamaProbe(PROBE_UNREACHABLE, f"ollama server not reachable: {exc}")
    return OllamaProbe(PROBE_OK, f"model '{model_name}' already downloaded")


def ollama_model_available(model: str, *, base_url: str = OLLAMA_BASE_URL) -> bool:
    """Return True when ``model`` is an Ollama model ready to serve requests.

    Ready means the local server is reachable and the model is already
    downloaded. Never triggers a pull — use :func:`bootstrap_ollama` for that.
    """
    if not model.startswith("ollama/"):
        return False
    model_name = model.removeprefix("ollama/")
    return probe_ollama_model(model_name, base_url=base_url).status == PROBE_OK


def bootstrap_ollama(model: str, *, base_url: str = OLLAMA_BASE_URL) -> OllamaBootstrapResult:
    """Ensure the named Ollama model is available locally.

    Non-Ollama models (anything not prefixed ``ollama/``) short-circuit to
    ``NOT_OLLAMA``. If the server is unreachable, returns
    ``SERVER_UNREACHABLE``; if the model is already downloaded, returns
    ``ALREADY_PRESENT``; otherwise triggers a pull and returns ``PULLED``.
    """
    if not model.startswith("ollama/"):
        return OllamaBootstrapResult(
            model=model,
            server_url=base_url,
            status=OLLAMA_NOT_OLLAMA,
            detail="not an Ollama model — nothing to pull",
        )

    model_name = model.removeprefix("ollama/")

    probe = probe_ollama_model(model_name, base_url=base_url)
    if probe.status == PROBE_UNREACHABLE:
        return OllamaBootstrapResult(
            model=model,
            server_url=base_url,
            status=OLLAMA_SERVER_UNREACHABLE,
            detail=probe.detail,
        )
    if probe.status == PROBE_OK:
        return OllamaBootstrapResult(
            model=model,
            server_url=base_url,
            status=OLLAMA_ALREADY_PRESENT,
            detail=probe.detail,
        )
    if probe.status != PROBE_MISSING:
        return OllamaBootstrapResult(
            model=model,
            server_url=base_url,
            status=OLLAMA_FAILED,
            detail=probe.detail,
        )

    pull_req = urllib.request.Request(  # noqa: S310
        f"{base_url}/api/pull",
        data=json.dumps({"model": model_name, "stream": False}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(pull_req, timeout=OLLAMA_PULL_TIMEOUT_S) as resp:  # noqa: S310
            payload = json.loads(resp.read())
    except (urllib.error.URLError, OSError) as exc:
        return OllamaBootstrapResult(
            model=model,
            server_url=base_url,
            status=OLLAMA_FAILED,
            detail=f"pull failed: {exc}",
        )

    if payload.get("status") == "success":
        return OllamaBootstrapResult(
            model=model,
            server_url=base_url,
            status=OLLAMA_PULLED,
            detail=f"model '{model_name}' pulled",
        )
    return OllamaBootstrapResult(
        model=model,
        server_url=base_url,
        status=OLLAMA_FAILED,
        detail=f"unexpected pull response: {payload}",
    )
