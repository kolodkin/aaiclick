Operation Log (oplog)
---

Storage and cleanup data model: `docs/architecture/data_model.md`.

**Implementation**: `aaiclick/oplog/`

Operation provenance captured inside ClickHouse, with zero AI dependencies:
every object created or transformed is recorded for lineage tracing and
debugging.

# Recording

**Implementation**: `aaiclick/oplog/oplog_api.py`

<!-- req: recording -->
Recording delegates to the active lifecycle handler. Under a bare
`object_context()` (local mode) it is a no-op. Inside a task, the
orchestration handler enqueues each record on the same FIFO queue as
incref/decref, and the queue's consumer writes it straight to
`operation_log` — one best-effort row per message, no buffer. Table creation
enqueues the `table_registry` row the same way.

# Instrumentation Points

<!-- req: instrumentation-points -->
| Operation                           | `operation`              | `kwargs`                                                     |
|-------------------------------------|--------------------------|--------------------------------------------------------------|
| `create_object()`                   | (none — registry only)   | —                                                            |
| `create_object_from_value()`        | `"create_from_value"`    | `{}`                                                         |
| Binary operators                    | `"+"`, `"-"`, etc.       | `{"left": ..., "right": ...}`; a literal operand is left out |
| Aggregations, `nunique`, `group_by` | `"sum"`, `"group_by"`, … | `{"source": ...}`                                            |
| `isin`                              | `"isin"`                 | `{"source": ..., "other": ...}`                              |
| `join`                              | `"join"`                 | `left`, `right`, `left_on`, `right_on`, `how`                |
| `concat`                            | `"concat"`               | `{"source_0": ..., "source_1": ..., ...}`                    |
| `insert`                            | `"insert"`               | `{"source": ..., "target": ...}`                             |
| `copy()`                            | `"copy"`                 | `{"source": ...}`                                            |

<!-- req: registry-only-create -->
`create_object()` only registers the table, never records an operation, to
avoid double-counting — the higher-level function records the operation that
produced the table.

# Graph Queries

<!-- req: graph-traversal -->
Graph traversal over `operation_log` by recursive CTE traces upstream
lineage; the MCP server exposes the graph to agents — see
[Lineage via MCP](lineage.md).
