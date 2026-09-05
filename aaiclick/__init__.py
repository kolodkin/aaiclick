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
from .ai.importing import import_ai_module
from .data import (
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    FIELDTYPE_SCALAR,
    ORIENT_DICT,
    ORIENT_RECORDS,
    ColumnInfo,
    ColumnType,
    FieldSpec,
    LazyOperator,
    Object,
    ObjectNotFoundError,
    QueryStats,
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
from .view_models import LineageAnswer


async def explain(target_table: str, question: str | None = None) -> LineageAnswer:
    """Trace and explain how a table was produced using AI lineage analysis.

    Same path as ``python -m aaiclick explain``: walks the operation log to
    reconstruct the lineage of `target_table` and returns the agent's answer.
    An optional `question` focuses the analysis (e.g. "why does this column
    contain nulls?").

    Args:
        target_table: ClickHouse table name to explain.
        question: Optional natural-language question to focus the analysis.

    Returns:
        ``LineageAnswer`` — read ``.answer`` for the explanation text.

    Raises:
        ImportError: If `aaiclick[ai]` is not installed.

    Note:
        Requires ``pip install "aaiclick[ai]"``.
    """
    lineage_ai = import_ai_module("aaiclick.internal_api.lineage_ai")
    return await lineage_ai.explain_lineage(target_table, question=question)
