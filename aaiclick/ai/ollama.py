"""
aaiclick.ai.ollama - Local Ollama server probing.

Stdlib-only (no litellm, no view models) so it is importable from anywhere —
``ai_available()`` uses it to gate AI steps, and ``aaiclick setup --ai``
builds its model bootstrap on the same probe.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Literal, NamedTuple

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
    downloaded. Never triggers a pull — use
    ``aaiclick.internal_api.setup.bootstrap_ollama()`` for that.
    """
    if not model.startswith("ollama/"):
        return False
    model_name = model.removeprefix("ollama/")
    return probe_ollama_model(model_name, base_url=base_url).status == PROBE_OK
