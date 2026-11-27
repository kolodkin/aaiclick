ObjectContext
---

Schema storage: `docs/architecture/data_model.md`.

**Implementation**: `aaiclick/data/object_context/`

Internals behind the [ObjectContext guide](../user_guide/object_context.md).

# Managed Resources

<!-- req: managed-resources -->
`object_context()` sets four per-resource ContextVars for the duration of the
block and resets each by token on exit, so nested calls are safe. Each
resource lives in its own module, so it can be read without importing
`object_context`:

| Resource          | Holds                         | Module              |
|-------------------|-------------------------------|---------------------|
| ClickHouse client | the chdb or remote client     | `ch_client.py`      |
| Table lifecycle   | lifecycle handler, or none    | `lifecycle.py`      |
| Table engine      | default engine for new tables | `object_context.py` |
| Object registry   | weak refs to live Objects     | `object_context.py` |

Oplog recording rides the lifecycle handler (`docs/designs/oplog.md` —
"Recording").

# Arrow Ingest

<!-- req: arrow-ingest -->
Ingest inserts are Arrow-native: `create_object_from_value()` builds a
`pa.Table` and hands it to the client — chdb inserts it through pyarrow's
`Python()` table function, clickhouse-connect through its native
`insert_arrow`. Type mapping: `arrow_types.py`.
