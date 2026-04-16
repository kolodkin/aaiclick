"""
aaiclick - A Python framework that translates Python code into ClickHouse operations.

This framework converts Python computational logic into a flow of ClickHouse database
operations, enabling execution of Python-equivalent computations at scale.
"""

try:
    from importlib.metadata import version as _version

    __version__ = _version("aaiclick")
except Exception:
    __version__ = "0.0.0"

# Import context manager (primary API) and factory functions
# Import core objects
from .data import (
    DATE_TYPES,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    FIELDTYPE_SCALAR,
    ORIENT_DICT,
    ORIENT_RECORDS,
    ColumnInfo,
    ColumnMeta,
    ColumnType,
    DataResult,
    LifecycleHandler,
    LocalLifecycleHandler,
    Object,
    QueryInfo,
    Schema,
    ValueListType,
    ValueScalarType,
    ValueType,
    View,
    ViewSchema,
    create_object,
    create_object_from_url,
    create_object_from_value,
    data_context,
    delete_object,
    delete_persistent_object,
    delete_persistent_objects,
    get_ch_client,
    list_persistent_objects,
    open_object,
)

# Import Snowflake ID generation
from .snowflake_id import get_snowflake_id, get_snowflake_ids

# Note: Ingest functions (copy_db, concat_objects_db, insert_objects_db) are internal.
# Use Object.copy(), Object.concat(), Object.insert() methods instead.


async def explain(target_table: str, question: str | None = None) -> str:
    """Trace and explain how target_table was produced using AI lineage analysis.

    Requires: pip install aaiclick[ai]
    """
    try:
        from aaiclick.ai.agents.lineage_agent import explain_lineage
    except ImportError as err:
        raise ImportError("AI features require the aaiclick[ai] extra. Install with: pip install aaiclick[ai]") from err
    return await explain_lineage(target_table, question)
