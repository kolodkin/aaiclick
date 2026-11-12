---
name: aaiclick
description: How to use the aaiclick MCP tools, including the lineage triage method for "why does table X look wrong?".
---

# aaiclick MCP tools

Tools mirror aaiclick's CLI verbs (the lineage tools have none) and run against
the same backends as the REST surface under `/api/v0`. Tool names and pydantic
schemas match the REST payloads.

# Lineage triage

For "why does table X look wrong?", use the lineage tools in this order:

1. `lineage_graph` — read each node's `sql_template` and form a hypothesis before querying.
2. `lineage_nodes` — a table with `live=false` cannot be queried: say so and stop,
   or re-run the job with full preservation (`run_job`) and retry.
3. `lineage_schema` — before querying any table; use only the column names it returns.
4. `lineage_query` — for evidence; cite the rows it returns.
