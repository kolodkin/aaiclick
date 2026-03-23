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
from .data import (
    data_context,
    get_ch_client,
    delete_object,
    delete_persistent_object,
    delete_persistent_objects,
    create_object,
    create_object_from_url,
    create_object_from_value,
    list_persistent_objects,
    open_object,
    LifecycleHandler,
    LocalLifecycleHandler,
)

# Import core objects
from .data import Object, View, DataResult
from .data import (
    Schema,
    ColumnInfo,
    ColumnMeta,
    ColumnType,
    ViewSchema,
    QueryInfo,
    DATE_TYPES,
    FIELDTYPE_SCALAR,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    ORIENT_DICT,
    ORIENT_RECORDS,
    ValueScalarType,
    ValueListType,
    ValueType,
)

# Import Snowflake ID generation
from .snowflake_id import get_snowflake_id, get_snowflake_ids

# Note: Ingest functions (copy_db, concat_objects_db, insert_objects_db) are internal.
# Use Object.copy(), Object.concat(), Object.insert() methods instead.


async def explain(target_table: str, question: str | None = None) -> str:
    """Trace and explain how a table was produced using AI lineage analysis.

    Walks the operation log to reconstruct the lineage of `target_table` and
    returns a human-readable explanation. An optional `question` focuses the
    analysis (e.g. "why does this column contain nulls?").

    Args:
        target_table: ClickHouse table name to explain.
        question: Optional natural-language question to focus the analysis.

    Returns:
        Human-readable explanation string describing how the table was produced.

    Raises:
        ImportError: If `aaiclick[ai]` is not installed.

    Note:
        Requires ``pip install "aaiclick[ai]"``.
    """
    try:
        from aaiclick.ai.agents.lineage_agent import explain_lineage
    except ImportError:
        raise ImportError(
            "AI features require the aaiclick[ai] extra. "
            "Install with: pip install aaiclick[ai]"
        )
    return await explain_lineage(target_table, question)

