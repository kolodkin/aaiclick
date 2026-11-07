ObjectContext
---

**Implementation**: `aaiclick/data/object_context/`

Internals behind the [ObjectContext guide](../user_guide/object_context.md).

# Managed Resources

`object_context()` owns and sets five per-resource ContextVars for the duration of the block.
Each resource lives in its own module so it can be accessed without importing `object_context`:

| Resource             | Type                          | ContextVar          | Module               | Accessor              |
|----------------------|-------------------------------|---------------------|----------------------|-----------------------|
| ClickHouse client    | `ChClient`                    | `_ch_client_var`    | `ch_client.py`       | `get_ch_client()`     |
| Table lifecycle      | `LifecycleHandler \| None`    | `_lifecycle_var`    | `lifecycle.py`       | `get_data_lifecycle()`|
| Table engine         | `EngineType`                  | `_engine_var`       | `object_context.py`  | `get_engine()`        |
| Object registry      | `dict[int, weakref.ref]`      | `_objects_var`      | `object_context.py`  | internal              |
| Oplog collector      | `OplogCollector \| None`      | `_oplog_collector`  | `oplog/collector.py` | `get_oplog_collector()`|

Each ContextVar is reset (via token) on context exit, so nested `object_context()` calls are safe.

# Arrow Ingest

Ingest inserts are arrow-native: `create_object_from_value()` builds a `pa.Table` and calls `ChClient.insert_arrow()` — chdb inserts it via pyarrow's `Python()` table function, clickhouse-connect via its native `insert_arrow`. Type mapping: `ch_type_to_pa()` in `arrow_types.py`.

# Schema Storage

aaiclick stores per-table fieldtype + column metadata as a serialised `Schema` (Pydantic JSON) in the SQL `table_registry.schema_doc` column. `read_table_schema(table)` reads that row to hydrate the in-memory `Schema`. There is no per-column ClickHouse `COMMENT` storage — DDL emits user columns only.

`Schema` = "what a table is" and `ViewSchema` = "how to read it", and the registry can never see the latter: `create_object()` is the registry's only writer and rejects a `ViewSchema`. View operators (`where`, `rename`, `with_columns`, `explode`, column selection) never create a table or a registry row — only `copy()` does.
