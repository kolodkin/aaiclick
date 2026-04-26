"""
aaiclick.ai.agents - LLM-powered agents for lineage and debugging.

Subpackage ``__init__`` is intentionally empty - import the agents directly
(``from aaiclick.ai.agents.debug_agent import debug_result``) so callers
without the ``ai`` extra can still import ``lineage_tools`` (which has no
LLM dependencies and is reused by ``aaiclick.internal_api.lineage``).
"""
