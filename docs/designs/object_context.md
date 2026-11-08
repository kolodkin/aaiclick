ObjectContext
---

**Implementation**: `aaiclick/data/object_context/`

Internals behind the [ObjectContext guide](../user_guide/object_context.md).

# Managed Resources

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

Ingest inserts are Arrow-native: `create_object_from_value()` builds a
`pa.Table` and hands it to the client — chdb inserts it through pyarrow's
`Python()` table function, clickhouse-connect through its native
`insert_arrow`. Type mapping: `arrow_types.py`.

# Schema Storage

Per-table fieldtype and column metadata live as a serialised `Schema`
(Pydantic JSON) in the SQL `table_registry.schema_doc` column, read back to
hydrate the in-memory `Schema`. There is no per-column ClickHouse `COMMENT`
storage — DDL emits user columns only.

`Schema` = "what a table is" and `ViewSchema` = "how to read it", and the
registry can never see the latter: `create_object()` is the registry's only
writer and rejects a `ViewSchema`. View operators (`where`, `rename`,
`with_columns`, `explode`, column selection) never create a table or a
registry row — only `copy()` does.
