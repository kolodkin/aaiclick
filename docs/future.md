Future Plans
---

Unimplemented features and planned work across aaiclick, ordered by priority.

---

# High Priority

## `join()` Operator

Distributed join of two Objects on a key column:

```python
basics.join(ratings, on="tconst", how="left")
```

Core data operation — table-stakes for any data framework.

## Insert Advisory Lock for Concurrent Workers

In orchestration mode, multiple workers may insert into the same persistent Object concurrently.
After the `_insert_source()` fix (generating fresh Snowflake IDs instead of preserving source
IDs), concurrent INSERTs within the same millisecond could produce interleaved IDs, mixing
rows from different logical inserts.

Use PostgreSQL advisory locks (`SELECT pg_advisory_lock(table_hash)`) to serialize inserts
per-table in distributed mode. SQLite mode is single-process and needs no lock.

## Progressive Tutorial

7-page tutorial using named snippets (`pymdownx.snippets` section markers) from existing
example files — 6 of 7 pages need zero new code. Pages: Your First Object, Operations,
Aggregations, Multi-Column Data, Views & Filters, Persistence, Orchestration. Add
`# --8<-- [start:name]` / `# --8<-- [end:name]` markers to example `.py` files, then
include specific sections in tutorial `.md` pages via snippet syntax.

Add "See Also" footers and cross-page links alongside the tutorial.

---

# Medium Priority

## Schema-Aware Agent Context ✅ IMPLEMENTED

**Implementation**: `aaiclick/ai/agents/tools.py` — see `get_schemas_for_nodes()` and `get_column_stats()`

Both `debug_agent` and `lineage_agent` now include table schemas in their initial context via
`get_schemas_for_nodes()`, which fetches `DESCRIBE TABLE` for every node in the lineage graph.
The `get_stats(table, column)` tool has been replaced by `get_column_stats(table)`, which discovers
columns automatically and returns stats for all of them in a single round-trip.

## Lineage: aai_id Uniqueness Awareness ✅ IMPLEMENTED

**Implementation**: `aaiclick/oplog/lineage.py` — see `OplogGraph.to_prompt_context()`;
`aaiclick/ai/agents/debug_agent.py` and `aaiclick/ai/agents/lineage_agent.py` — see system prompts

Both agents' system prompts now include guidance that `insert` and `concat` operations generate
fresh aai_id values. `OplogGraph.to_prompt_context()` emits per-node warnings for these operations,
instructing the LLM to use data-value matching or oplog provenance metadata instead of aai_id
equality for row-level tracing across insert/concat boundaries.


---

# Deferred

Items deferred until preconditions are met.

## Comparison Page

`docs/comparison.md` — feature matrix comparing aaiclick vs Pandas, Spark, and Dask. Defer until the project has enough real-world usage to make meaningful claims.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
