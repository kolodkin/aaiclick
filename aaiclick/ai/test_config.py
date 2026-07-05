"""Tests for ``aaiclick.ai.config``."""

from __future__ import annotations

from . import config
from .ollama import DEFAULT_OLLAMA_MODEL


def test_ai_available_remote_model_with_api_key(monkeypatch):
    monkeypatch.setenv("AAICLICK_AI_API_KEY", "secret")
    monkeypatch.setenv("AAICLICK_AI_MODEL", "nvidia_nim/meta/llama-3.3-70b-instruct")

    assert config.ai_available() is True


def test_ai_available_remote_model_without_key(monkeypatch):
    monkeypatch.delenv("AAICLICK_AI_API_KEY", raising=False)
    monkeypatch.setenv("AAICLICK_AI_MODEL", "openai/gpt-4")

    assert config.ai_available() is False


def test_ai_available_probes_configured_ollama_model(monkeypatch):
    probed: list[str] = []

    def fake_probe(model):
        probed.append(model)
        return True

    monkeypatch.delenv("AAICLICK_AI_API_KEY", raising=False)
    monkeypatch.setenv("AAICLICK_AI_MODEL", "ollama/llama3.2:3b")
    monkeypatch.setattr(config, "ollama_model_available", fake_probe)

    assert config.ai_available() is True
    assert probed == ["ollama/llama3.2:3b"]


def test_ai_available_api_key_does_not_shortcut_ollama_probe(monkeypatch):
    monkeypatch.setenv("AAICLICK_AI_API_KEY", "secret")
    monkeypatch.setenv("AAICLICK_AI_MODEL", "ollama/llama3.1:8b")
    monkeypatch.setattr(config, "ollama_model_available", lambda model: False)

    assert config.ai_available() is False


def test_ai_available_defaults_to_ollama_model(monkeypatch):
    monkeypatch.delenv("AAICLICK_AI_API_KEY", raising=False)
    monkeypatch.delenv("AAICLICK_AI_MODEL", raising=False)
    monkeypatch.setattr(config, "ollama_model_available", lambda model: model == DEFAULT_OLLAMA_MODEL)

    assert config.ai_available() is True
