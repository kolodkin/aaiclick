"""
aaiclick.ai - Optional AI-powered lineage querying and debugging.

Requires: pip install aaiclick[ai]

This package's submodules pull in litellm and other LLM dependencies. Importing
``aaiclick.ai`` itself is intentionally lightweight - import submodules
explicitly (``from aaiclick.ai.config import get_ai_provider``) so callers
without the ``ai`` extra can still import sibling packages that share helpers
from ``aaiclick.ai.agents.lineage_tools``.
"""
