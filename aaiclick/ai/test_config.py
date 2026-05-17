"""
Tests for aaiclick.ai.config — env-var parsing for get_ai_provider().
"""

from __future__ import annotations

import pytest

from aaiclick.ai.config import get_ai_provider


def test_get_ai_provider_strips_whitespace_from_env(monkeypatch: pytest.MonkeyPatch):
    """Trailing/leading whitespace in env vars is stripped.

    aiohttp rejects any header containing ``\\n`` or ``\\r`` as a header-injection
    guard, so a stray newline in ``AAICLICK_AI_API_KEY`` (common with secrets
    pasted into env files or returned from CI secret stores) would crash every
    request. Strip at the boundary.
    """
    monkeypatch.setenv("AAICLICK_AI_MODEL", "  nvidia_nim/meta/llama-3.3-70b-instruct\n")
    monkeypatch.setenv("AAICLICK_AI_API_KEY", "nvapi-xxxxx\n")

    provider = get_ai_provider()

    assert provider.model == "nvidia_nim/meta/llama-3.3-70b-instruct"
    assert provider._api_key == "nvapi-xxxxx"


def test_get_ai_provider_missing_api_key_stays_none(monkeypatch: pytest.MonkeyPatch):
    """Unset API key remains ``None`` rather than becoming an empty string."""
    monkeypatch.delenv("AAICLICK_AI_API_KEY", raising=False)

    provider = get_ai_provider()

    assert provider._api_key is None
