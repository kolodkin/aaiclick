"""
aaiclick - A Python framework that translates Python code into ClickHouse operations.

This framework converts Python computational logic into a flow of ClickHouse database
operations, enabling execution of Python-equivalent computations at scale.
"""

__version__ = "0.1.0"

# Import context manager (primary API) and context accessor
from .context import Context, get_context

# Import core objects
from .object import Object, DataResult
from .models import (
    Schema,
    ColumnMeta,
    ColumnType,
    QueryInfo,
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
from .snowflake import get_snowflake_id, get_snowflake_ids

# Note: Factory functions (create_object, create_object_from_value) are internal.
# Use Context.create_object() and Context.create_object_from_value() instead.
# Note: Ingest functions (copy, concat, insert) are db-level in ingest.py.
# Use Object.copy(), Object.concat(), Object.insert() methods instead.

