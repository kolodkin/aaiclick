"""
aaiclick.ai.config - Configuration from environment variables.
"""

import os

from aaiclick.ai.ollama import get_configured_model, ollama_model_available
from aaiclick.ai.provider import AIProvider


def get_ai_provider() -> AIProvider:
    """Return an AIProvider configured from environment variables.

    Reads:
      AAICLICK_AI_MODEL   — LiteLLM model string (default: ollama/llama3.1:8b)
      AAICLICK_AI_API_KEY — API key for remote providers (optional)
    """
    return AIProvider(model=get_configured_model(), api_key=os.environ.get("AAICLICK_AI_API_KEY"))


def ai_available() -> bool:
    """Return True when the configured model can serve AI queries.

    Ollama models are available when the local server is reachable and the
    model is downloaded — run ``python -m aaiclick setup --ai`` to pull it.
    Remote models are available when ``AAICLICK_AI_API_KEY`` is set.
    """
    model = get_configured_model()
    if model.startswith("ollama/"):
        return ollama_model_available(model)
    return bool(os.environ.get("AAICLICK_AI_API_KEY"))
