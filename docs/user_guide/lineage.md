Lineage via MCP
---

The MCP server exposes the [operation log](oplog.md) and a read-only SQL
sandbox over it, so your coding agent — Claude Code, Codex, any MCP client —
can answer *how did this value get here?* against the live tables.

# Tools

| Tool                                  | Purpose                                                                                      |
|---------------------------------------|----------------------------------------------------------------------------------------------|
| `oplog_subgraph(target_table)`        | The operation graph behind a table — each node carries its rendered `sql_template`           |
| `list_graph_nodes(target_table)`      | Every table in that graph with its kind (input / intermediate / target) and whether it is live |
| `get_table_schema(table, target_table)` | Columns and types for one table in the graph                                               |
| `query_table(sql, target_table)`      | A read-only, row-capped `SELECT` scoped to the graph                                         |

All four take `direction` (`"backward"` default, or `"forward"`) and
`max_depth`. The server's instructions give the agent the triage order:
graph SQL first, `list_graph_nodes` for dropped tables, `get_table_schema`
before any query, `query_table` rows as evidence.

!!! warning "`query_table` reaches only the target's lineage"
    The scope is resolved server-side from `target_table`; a SQL string
    naming any other table, a table function, or a `SETTINGS` clause is
    rejected. Write a CTE as a subquery in `FROM`, and use
    `has(column, value)` rather than `IN` for an array column.

A table that `list_graph_nodes` reports as `live: false` was cleaned up
after the run. Re-run the job with `preservation_mode="FULL"` (`run_job`,
an admin tool — the read-scoped token below cannot call it) and query the
new run's tables.

**Implementation**: `aaiclick/server/mcp.py` (tools), `aaiclick/internal_api/lineage.py` (scope lookup), `aaiclick/oplog/query_sandbox.py` (sandbox).

# Connect an agent

Start the server, then register `http://127.0.0.1:5255/mcp` with the agent.
Local mode needs no credential:

```bash
pip install "aaiclick[server]"
python -m aaiclick local start
```

=== "Claude Code"

    ```bash
    claude mcp add --transport http aaiclick http://127.0.0.1:5255/mcp
    ```

=== "Codex"

    ```toml
    # ~/.codex/config.toml
    [mcp_servers.aaiclick]
    url = "http://127.0.0.1:5255/mcp"
    ```

In distributed mode the server requires a bearer token. Mint one and pass it
as a header (`claude mcp add … --header "Authorization: Bearer aaic_…"`, or
`http_headers` in the Codex entry):

```bash
python -m aaiclick token create <username> --name claude-code --scope read
```

Then ask the agent, for example: *"Why is `p_revenue` negative for vendor
MS-001? Use the aaiclick lineage tools."*
