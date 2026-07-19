"""Tests for ``aaiclick.ai.ollama``."""

from __future__ import annotations

import json
import urllib.error

from . import ollama


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


def test_get_configured_model_treats_empty_env_as_unset(monkeypatch):
    monkeypatch.setenv("AAICLICK_AI_MODEL", "")

    assert ollama.get_configured_model() == ollama.DEFAULT_OLLAMA_MODEL


def test_ollama_model_available_non_ollama_model():
    assert ollama.ollama_model_available("openai/gpt-4") is False


def test_ollama_model_available_server_unreachable():
    assert ollama.ollama_model_available("ollama/llama3.1:8b", base_url="http://127.0.0.1:1") is False


def test_ollama_model_available_model_present(monkeypatch):
    monkeypatch.setattr(ollama.urllib.request, "urlopen", lambda *a, **k: None)

    assert ollama.ollama_model_available("ollama/llama3.1:8b") is True


def test_ollama_model_available_model_missing_does_not_pull(monkeypatch):
    urls: list[str] = []

    def fake_urlopen(req, timeout=None):
        urls.append(req.full_url)
        raise urllib.error.HTTPError(req.full_url, 404, "Not Found", {}, None)

    monkeypatch.setattr(ollama.urllib.request, "urlopen", fake_urlopen)

    assert ollama.ollama_model_available("ollama/llama3.1:8b") is False
    assert urls == ["http://localhost:11434/api/show"]


def test_bootstrap_ollama_non_ollama_model():
    result = ollama.bootstrap_ollama("openai/gpt-4")

    assert isinstance(result, ollama.OllamaBootstrapResult)
    assert result.status == ollama.OLLAMA_NOT_OLLAMA
    assert result.model == "openai/gpt-4"


def test_bootstrap_ollama_server_unreachable():
    result = ollama.bootstrap_ollama("ollama/llama3.1:8b", base_url="http://127.0.0.1:1")

    assert result.status == ollama.OLLAMA_SERVER_UNREACHABLE
    assert "not reachable" in (result.detail or "")


def test_bootstrap_ollama_already_present(monkeypatch):
    monkeypatch.setattr(ollama.urllib.request, "urlopen", lambda *a, **k: _StubResponse(b""))

    result = ollama.bootstrap_ollama("ollama/llama3.1:8b")

    assert result.status == ollama.OLLAMA_ALREADY_PRESENT


def test_bootstrap_ollama_pulled(monkeypatch):
    def fake_urlopen(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if url.endswith("/api/show"):
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
        return _StubResponse(b'{"status": "success"}')

    monkeypatch.setattr(ollama.urllib.request, "urlopen", fake_urlopen)

    result = ollama.bootstrap_ollama("ollama/llama3.1:8b")

    assert result.status == ollama.OLLAMA_PULLED


def test_bootstrap_ollama_show_5xx_returns_failed(monkeypatch):
    def fake_urlopen(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if url.endswith("/api/show"):
            raise urllib.error.HTTPError(url, 500, "Internal Server Error", {}, None)
        return _StubResponse(b"")

    monkeypatch.setattr(ollama.urllib.request, "urlopen", fake_urlopen)

    result = ollama.bootstrap_ollama("ollama/llama3.1:8b")

    assert result.status == ollama.OLLAMA_FAILED
    assert "model lookup failed" in (result.detail or "")


def test_bootstrap_ollama_pull_unexpected_response(monkeypatch):
    def fake_urlopen(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if url.endswith("/api/show"):
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
        return _StubResponse(json.dumps({"status": "error"}).encode())

    monkeypatch.setattr(ollama.urllib.request, "urlopen", fake_urlopen)

    result = ollama.bootstrap_ollama("ollama/llama3.1:8b")

    assert result.status == ollama.OLLAMA_FAILED
