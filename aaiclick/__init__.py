"""
aaiclick - A Python framework that translates Python code into ClickHouse operations.

This framework converts Python computational logic into a flow of ClickHouse database
operations, enabling execution of Python-equivalent computations at scale.
"""

__version__ = "0.1.0"

# Import context manager (primary API), context accessor, and factory functions
from .data_context import DataContext, get_context, create_object, create_object_from_value

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

# Note: Ingest functions (copy_db, concat_objects_db, insert_objects_db) are internal.
# Use Object.copy(), Object.concat(), Object.insert() methods instead.

