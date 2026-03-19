"""
aaiclick.ai.config - Configuration from environment variables.
"""

import os

from aaiclick.ai.provider import AIProvider


def get_ai_provider() -> AIProvider:
    """Return an AIProvider configured from environment variables.

    Reads:
      AAICLICK_AI_MODEL   — LiteLLM model string (default: ollama/llama3.1:8b)
      AAICLICK_AI_API_KEY — API key for remote providers (optional)
    """
    model = os.environ.get("AAICLICK_AI_MODEL", "ollama/llama3.1:8b")
    api_key = os.environ.get("AAICLICK_AI_API_KEY")
    return AIProvider(model=model, api_key=api_key)
