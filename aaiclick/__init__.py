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

# Context manager (primary API)
# Factory functions
# Core types
# Helper functions
# Schema definition
# Field type and orientation constants
# Value type aliases
# Persistent object management
from .data import (
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    FIELDTYPE_SCALAR,
    ORIENT_DICT,
    ORIENT_RECORDS,
    ColumnInfo,
    ColumnType,
    DataResult,
    FieldSpec,
    Object,
    Schema,
    ValueListType,
    ValueScalarType,
    ValueType,
    View,
    cast,
    create_object,
    create_object_from_url,
    create_object_from_value,
    data_context,
    delete_persistent_object,
    delete_persistent_objects,
    list_persistent_objects,
    literal,
    open_object,
    split_by_char,
)


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
    except ImportError as err:
        raise ImportError("AI features require the aaiclick[ai] extra. Install with: pip install aaiclick[ai]") from err
    return await explain_lineage(target_table, question)
