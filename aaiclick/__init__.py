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
