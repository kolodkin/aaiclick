"""
aaiclick.ai.agents.strategy_agent - Translate a question + lineage graph
into a ``SamplingStrategy`` (``dict[str, str]``) suitable for Phase 3 replay.

The agent takes a natural-language question and an ``OplogGraph`` and asks
the LLM for a JSON object mapping table names to ClickHouse WHERE clauses.
Each emitted clause is dry-run against ClickHouse so syntax errors and
unknown columns are caught before the strategy is used for replay.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

from aaiclick.ai.agents.prompts import STRATEGY_SYSTEM_PROMPT
from aaiclick.ai.agents.tools import get_schemas_for_nodes
from aaiclick.ai.config import get_ai_provider
from aaiclick.data.data_context import get_ch_client
from aaiclick.oplog.lineage import OplogGraph
from aaiclick.oplog.sampling import SamplingStrategy

logger = logging.getLogger(__name__)

_MAX_RETRIES = 1


async def produce_strategy(
    question: str,
    graph: OplogGraph,
    *,
    schemas: str | None = None,
) -> SamplingStrategy:
    """Ask the LLM to translate ``question`` into a ``SamplingStrategy``.

    The strategy keys must be tables that appear in ``graph``. Each WHERE
    clause is validated against ClickHouse (via a ``LIMIT 0`` dry run) so
    malformed output is caught before the strategy is used downstream.

    Args:
        question: Natural-language question from the user.
        graph: ``OplogGraph`` built by ``oplog_subgraph()`` for the target
            table. Used to constrain valid keys and to load column schemas.
        schemas: Pre-rendered schema block for the graph's tables. Pass
            the result of ``get_schemas_for_nodes(graph.nodes)`` to reuse
            an existing fetch; left as ``None`` the agent fetches its own.

    Returns:
        ``SamplingStrategy`` — a (possibly empty) dict of table → WHERE
        clause. An empty dict is a valid result and means "the question
        does not map to a row-level filter".

    Raises:
        ValueError: When the model's output is not valid JSON, references
            a table outside the graph, or fails validation even after a
            retry.
    """
    known_tables = graph.tables
    if schemas is None:
        schemas = await get_schemas_for_nodes(graph.nodes)
    context = f"{graph.to_prompt_context()}\n\n{schemas}".rstrip()

    provider = get_ai_provider()
    base_prompt = f"Question: {question}"
    retry_error: ValueError | None = None

    for attempt in range(_MAX_RETRIES + 1):
        prompt = (
            base_prompt
            if retry_error is None
            else f"Your previous output failed validation: {retry_error}\nReturn a corrected JSON object. {base_prompt}"
        )
        raw = await provider.query(prompt, context=context, system=STRATEGY_SYSTEM_PROMPT)
        try:
            strategy = _parse(raw, known_tables)
            if strategy:
                await _dry_run(strategy)
            return strategy
        except ValueError as exc:
            logger.warning("strategy agent output invalid: %s", exc)
            if attempt == _MAX_RETRIES:
                raise
            retry_error = exc

    # Unreachable: the last iteration either returns or re-raises.
    raise AssertionError("produce_strategy exited the retry loop without a result")


def format_strategy(strategy: SamplingStrategy) -> str:
    """Render a strategy as a context block for the debug agent.

    Returns an empty string for an empty strategy so callers can always
    append the result unconditionally.
    """
    if not strategy:
        return ""
    body = "\n".join(f"  {table}: {clause}" for table, clause in strategy.items())
    return f"\n\nSampling strategy (proposed row filter):\n{body}"


def _parse(raw: str, known_tables: set[str]) -> SamplingStrategy:
    """Parse the model output into a validated ``SamplingStrategy``."""
    payload = _extract_json(raw)
    try:
        decoded: Any = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"output is not valid JSON: {exc}") from exc

    if not isinstance(decoded, dict):
        raise ValueError(f"expected a JSON object, got {type(decoded).__name__}")

    strategy: SamplingStrategy = {}
    for key, value in decoded.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError(f"strategy entries must be string→string, got {key!r}→{value!r}")
        if key not in known_tables:
            raise ValueError(f"table {key!r} is not in the lineage graph (known: {sorted(known_tables)})")
        clause = value.strip()
        if not clause:
            raise ValueError(f"empty WHERE clause for table {key!r}")
        strategy[key] = clause
    return strategy


_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json(raw: str) -> str:
    """Return the substring that looks like a JSON object.

    Tolerates leading/trailing prose or markdown fences; the tolerance
    cost is tiny and the cost of rejecting legitimate-but-wrapped output
    is a full retry.
    """
    stripped = raw.strip()
    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        if stripped.lower().startswith("json"):
            stripped = stripped[4:].lstrip()
    match = _JSON_OBJECT_RE.search(stripped)
    if not match:
        raise ValueError("no JSON object found in output")
    return match.group(0)


async def _dry_run(strategy: SamplingStrategy) -> None:
    """Run every clause as a ``LIMIT 0`` query in parallel to surface syntax errors."""
    ch_client = get_ch_client()

    async def _check(table: str, clause: str) -> tuple[str, BaseException | None]:
        try:
            await ch_client.query(f"SELECT aai_id FROM {table} WHERE {clause} LIMIT 0")
            return table, None
        except Exception as exc:
            return table, exc

    results = await asyncio.gather(*(_check(table, clause) for table, clause in strategy.items()))
    for table, exc in results:
        if exc is not None:
            raise ValueError(f"clause for {table!r} failed validation: {exc}") from exc
