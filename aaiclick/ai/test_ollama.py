"""Tests for ``aaiclick.ai.ollama``."""

from __future__ import annotations

import urllib.error

from . import ollama


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
