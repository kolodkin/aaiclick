"""
aaiclick.ai.provider - Unified AI provider via LiteLLM.
"""

from __future__ import annotations

import json
import os
from typing import Any

from litellm import acompletion


class AIProvider:
    """Unified AI provider via LiteLLM. Works with any model string."""

    def __init__(self, model: str = "ollama/llama3.1:8b", api_key: str | None = None) -> None:
        self.model = model
        self._api_key = api_key

    async def query(self, prompt: str, context: str = "", system: str = "") -> str:
        """Single-turn query. Returns the model's text response."""
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        content = f"Context:\n{context}\n\n{prompt}" if context else prompt
        messages.append({"role": "user", "content": content})
        kwargs: dict[str, Any] = {"model": self.model, "messages": messages}
        if self._api_key:
            kwargs["api_key"] = self._api_key
        response = await acompletion(**kwargs)
        return response.choices[0].message.content or ""

    async def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
    ) -> Any:
        """Low-level completion with full message history.

        Returns the raw litellm response. Use this for multi-turn agentic
        loops that manage their own message list.
        """
        kwargs: dict[str, Any] = {"model": self.model, "messages": messages}
        if tools:
            kwargs["tools"] = tools
        if self._api_key:
            kwargs["api_key"] = self._api_key
        return await acompletion(**kwargs)

    async def query_with_tools(
        self,
        prompt: str,
        tools: list[dict[str, Any]],
        context: str = "",
    ) -> dict[str, Any]:
        """Single-round query with tool-calling support.

        Returns a dict with keys:
          - content: str | None — model text (None when finish_reason is tool_calls)
          - tool_calls: list[dict] — each with id, name, arguments (dict)
          - finish_reason: str
        """
        content = f"Context:\n{context}\n\n{prompt}" if context else prompt
        messages: list[dict[str, Any]] = [{"role": "user", "content": content}]
        kwargs: dict[str, Any] = {"model": self.model, "messages": messages, "tools": tools}
        if self._api_key:
            kwargs["api_key"] = self._api_key
        response = await acompletion(**kwargs)
        message = response.choices[0].message
        return {
            "content": message.content,
            "tool_calls": [
                {
                    "id": tc.id,
                    "name": tc.function.name,
                    "arguments": json.loads(tc.function.arguments),
                }
                for tc in (message.tool_calls or [])
            ],
            "finish_reason": response.choices[0].finish_reason,
        }
