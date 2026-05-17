"""
Tests for aaiclick.ai.config — env-var parsing for get_ai_provider().
"""

from __future__ import annotations

import pytest

from aaiclick.ai.config import get_ai_provider


def test_get_ai_provider_strips_whitespace_from_env(monkeypatch: pytest.MonkeyPatch):
    """All whitespace (including interior) is removed from the API key.

    aiohttp rejects any header containing ``\\n`` or ``\\r`` as a header-injection
    guard, so a stray newline in ``AAICLICK_AI_API_KEY`` would crash every
    request. Edges aren't enough — pasting a long secret into the GH Actions
    UI sometimes wraps it with an interior newline. Stripping all whitespace
    is safe for API keys, which never legitimately contain whitespace.
    """
    monkeypatch.setenv("AAICLICK_AI_MODEL", "  nvidia_nim/meta/llama-3.3-70b-instruct\n")
    monkeypatch.setenv("AAICLICK_AI_API_KEY", " nvapi-xx\nxx\r\nx ")

    provider = get_ai_provider()

    assert provider.model == "nvidia_nim/meta/llama-3.3-70b-instruct"
    assert provider._api_key == "nvapi-xxxxx"


def test_get_ai_provider_missing_api_key_stays_none(monkeypatch: pytest.MonkeyPatch):
    """Unset API key remains ``None`` rather than becoming an empty string."""
    monkeypatch.delenv("AAICLICK_AI_API_KEY", raising=False)

    provider = get_ai_provider()

    assert provider._api_key is None
