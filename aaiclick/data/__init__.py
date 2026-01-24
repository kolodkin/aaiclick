"""
aaiclick.data - Data context and object management for ClickHouse operations.

This module provides the core data management capabilities for aaiclick,
including context management, object creation, and database operations.
"""

from .data_context import (
    DataContext,
    create_object,
    create_object_from_value,
    get_ch_client,
    get_context,
    get_pool,
)
from .models import (
    FIELDTYPE_ARRAY,
    FIELDTYPE_DICT,
    FIELDTYPE_SCALAR,
    ORIENT_DICT,
    ORIENT_RECORDS,
    ColumnMeta,
    ColumnType,
    QueryInfo,
    Schema,
    ValueListType,
    ValueScalarType,
    ValueType,
)
from .object import DataResult, Object
from .snowflake import get_snowflake_id, get_snowflake_ids
