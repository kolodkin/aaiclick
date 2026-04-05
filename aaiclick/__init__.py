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
from .data import data_context

# Factory functions
from .data import create_object, create_object_from_value, create_object_from_url

# Core types
from .data import Object, View, DataResult

# Helper functions
from .data import cast, literal, split_by_char

# Schema definition
from .data import Schema, ColumnInfo, ColumnType

# Field type and orientation constants
from .data import (
    FIELDTYPE_SCALAR,
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    ORIENT_DICT,
    ORIENT_RECORDS,
)

# Value type aliases
from .data import ValueScalarType, ValueListType, ValueType

# Persistent object management
from .data import (
    list_persistent_objects,
    open_object,
    delete_persistent_object,
    delete_persistent_objects,
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
    except ImportError:
        raise ImportError(
            "AI features require the aaiclick[ai] extra. "
            "Install with: pip install aaiclick[ai]"
        )
    return await explain_lineage(target_table, question)
