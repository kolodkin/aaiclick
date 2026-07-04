"""Tests for ``aaiclick.ai.ollama``."""

from __future__ import annotations

import urllib.error

from . import ollama


def test_ollama_model_available_non_ollama_model():
    assert ollama.ollama_model_available("openai/gpt-4") is False


def test_ollama_model_available_server_unreachable():
    assert ollama.ollama_model_available("ollama/llama3.1:8b", base_url="http://127.0.0.1:1") is False


def test_ollama_model_available_model_present(monkeypatch):
    monkeypatch.setattr(ollama.urllib.request, "urlopen", lambda *a, **k: _StubResponse(b""))

    assert ollama.ollama_model_available("ollama/llama3.1:8b") is True


def test_ollama_model_available_model_missing_does_not_pull(monkeypatch):
    urls: list[str] = []

    def fake_urlopen(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        urls.append(url)
        if url.endswith("/api/show"):
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
        return _StubResponse(b"")

    monkeypatch.setattr(ollama.urllib.request, "urlopen", fake_urlopen)

    assert ollama.ollama_model_available("ollama/llama3.1:8b") is False
    assert not any(url.endswith("/api/pull") for url in urls)


class _StubResponse:
    """Minimal ``urlopen`` stand-in that supports ``read()`` + context manager."""

    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False
