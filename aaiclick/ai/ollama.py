"""
aaiclick.ai.ollama - Local Ollama server probing.

Stdlib-only (no litellm, no view models) so it is importable from anywhere —
``ai_available()`` uses it to gate AI steps, and ``aaiclick setup --ai``
builds its model bootstrap on the same constants and request helper.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request

OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_PING_TIMEOUT_S = 2
OLLAMA_SHOW_TIMEOUT_S = 5
OLLAMA_PULL_TIMEOUT_S = 600
DEFAULT_OLLAMA_MODEL = "ollama/llama3.1:8b"


def show_model_request(model_name: str, base_url: str) -> urllib.request.Request:
    """Build the ``POST /api/show`` request that checks a model is downloaded."""
    return urllib.request.Request(  # noqa: S310
        f"{base_url}/api/show",
        data=json.dumps({"model": model_name}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )


def ollama_model_available(model: str, *, base_url: str = OLLAMA_BASE_URL) -> bool:
    """Return True when ``model`` is an Ollama model ready to serve requests.

    Ready means the local server is reachable and the model is already
    downloaded. Never triggers a pull — use
    ``aaiclick.internal_api.setup.bootstrap_ollama()`` for that.
    """
    if not model.startswith("ollama/"):
        return False

    model_name = model.removeprefix("ollama/")

    try:
        urllib.request.urlopen(base_url, timeout=OLLAMA_PING_TIMEOUT_S)  # noqa: S310
    except (urllib.error.URLError, OSError):
        return False

    try:
        urllib.request.urlopen(show_model_request(model_name, base_url), timeout=OLLAMA_SHOW_TIMEOUT_S)  # noqa: S310
    except urllib.error.HTTPError as exc:
        # exc.close() releases the response stream (Python 3.14's tempfile-backed
        # body would raise ResourceWarning on gc, which filterwarnings=["error"]
        # escalates to a failure).
        exc.close()
        return False
    except (urllib.error.URLError, OSError):
        return False
    return True
