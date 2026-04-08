Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# High Priority

## `join()` Operator

Distributed join of two Objects on a key column:

```python
basics.join(ratings, on="tconst", how="left")
```

Core data operation — table-stakes for any data framework.

## Insert Advisory Lock for Concurrent Workers

Concurrent workers inserting into the same persistent Object can produce interleaved Snowflake IDs within the same millisecond.

Serialize via PostgreSQL advisory locks (`pg_advisory_lock(table_hash)`) per-table. SQLite mode is single-process and needs no lock.

## Progressive Tutorial

7-page tutorial using named snippets (`pymdownx.snippets` section markers) from existing
example files — 6 of 7 pages need zero new code. Pages: Your First Object, Operations,
Aggregations, Multi-Column Data, Views & Filters, Persistence, Orchestration. Add
`# --8<-- [start:name]` / `# --8<-- [end:name]` markers to example `.py` files, then
include specific sections in tutorial `.md` pages via snippet syntax.

Add "See Also" footers and cross-page links alongside the tutorial.

---

# Medium Priority

## Schema-Aware Agent Context

Both `debug_agent` and `lineage_agent` build context from the oplog graph but never include table schemas, causing the LLM to hallucinate column names. Two improvements:

1. **Schema injection** — fetch `DESCRIBE TABLE` for every node before the agentic loop and include in the initial context message.
2. **`get_column_stats` tool** — replace `get_stats(table, column)` with a schema-first tool that returns stats for all columns without requiring the LLM to know column names upfront.

## Lineage: aai_id Uniqueness Awareness

`insert()` and `concat()` generate fresh Snowflake IDs, so `aai_id` values differ between source and target. Row-level tracing across these boundaries needs data-value matching or oplog provenance metadata instead of `aai_id` matching.


---

# Deferred

Items deferred until preconditions are met.

## Comparison Page

`docs/comparison.md` — feature matrix comparing aaiclick vs Pandas, Spark, and Dask. Defer until the project has enough real-world usage to make meaningful claims.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
