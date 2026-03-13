"""
aaiclick.data.models - Data models and type definitions for the aaiclick framework.

This module provides dataclasses, type literals, and constants used throughout the framework.
"""

from datetime import datetime
from typing import Optional, Dict, Union, Literal, List, NamedTuple
from dataclasses import dataclass, field

import yaml


# ClickHouse column type literals
ColumnType = Literal[
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Int8", "Int16", "Int32", "Int64",
    "Float32", "Float64",
    "String", "FixedString",
    "Date", "DateTime", "DateTime64",
    "Bool", "UUID",
    "Array", "Tuple", "Map", "Nested"
]

# Type category sets for runtime type checking
INT_TYPES = frozenset({"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"})
FLOAT_TYPES = frozenset({"Float32", "Float64"})
DATE_TYPES = frozenset({"DateTime64(3, 'UTC')"})
NUMERIC_TYPES = INT_TYPES | FLOAT_TYPES


@dataclass(frozen=True)
class ColumnInfo:
    """Column definition including base type, nullability, array wrapping, and cardinality.

    Attributes:
        type: Base ClickHouse element type (e.g., 'Int64', 'String')
        nullable: Whether the column allows NULL values
        array: Nesting depth of Array wrapping. ``False``/``0`` means plain column,
               ``True``/``1`` means ``Array(T)``, ``2`` means ``Array(Array(T))``, etc.
        low_cardinality: Whether to use LowCardinality encoding
    """

    type: str
    nullable: bool = False
    array: int = False
    low_cardinality: bool = False
    description: str = ""

    def ch_type(self) -> str:
        """Return the ClickHouse DDL type string.

        Examples:
            ColumnInfo('Int64').ch_type()                          -> 'Int64'
            ColumnInfo('Int64', nullable=True).ch_type()           -> 'Nullable(Int64)'
            ColumnInfo('Int64', array=True).ch_type()              -> 'Array(Int64)'
            ColumnInfo('Int64', array=2).ch_type()                 -> 'Array(Array(Int64))'
            ColumnInfo('Int64', nullable=True, array=True).ch_type() -> 'Array(Nullable(Int64))'
            ColumnInfo('String', low_cardinality=True).ch_type()   -> 'LowCardinality(String)'
            ColumnInfo('String', nullable=True, low_cardinality=True).ch_type()
                -> 'Array(LowCardinality(Nullable(String)))' if array else 'LowCardinality(Nullable(String))'
        """
        base = self.type
        if self.nullable:
            base = f"Nullable({base})"
        if self.low_cardinality:
            base = f"LowCardinality({base})"
        depth = int(self.array)
        for _ in range(depth):
            base = f"Array({base})"
        return base



class Computed(NamedTuple):
    """A computed column definition: ClickHouse type + SQL expression."""

    type: str
    expression: str


def parse_ch_type(type_str: str) -> "ColumnInfo":
    """Parse a ClickHouse type string into a ColumnInfo.

    Handles plain types ('Int64'), nullable ('Nullable(Int64)'),
    array ('Array(Int64)'), nested arrays ('Array(Array(Int64))'),
    low cardinality ('LowCardinality(String)'),
    and combinations ('Array(LowCardinality(Nullable(String)))').

    Args:
        type_str: ClickHouse type string from system.columns

    Returns:
        ColumnInfo with extracted base type, nullable, array depth, and low_cardinality flags
    """
    array = 0
    while type_str.startswith("Array(") and type_str.endswith(")"):
        array += 1
        type_str = type_str[6:-1]

    low_cardinality = False
    if type_str.startswith("LowCardinality(") and type_str.endswith(")"):
        low_cardinality = True
        type_str = type_str[15:-1]

    nullable = False
    if type_str.startswith("Nullable(") and type_str.endswith(")"):
        nullable = True
        type_str = type_str[9:-1]

    return ColumnInfo(type=type_str, nullable=nullable, array=array, low_cardinality=low_cardinality)


# Fieldtype constants
FIELDTYPE_SCALAR = "s"
FIELDTYPE_ARRAY = "a"
FIELDTYPE_DICT = "d"

# ClickHouse engine constants
ENGINE_MERGE_TREE = "MergeTree"
ENGINE_AGGREGATING_MERGE_TREE = "AggregatingMergeTree"
ENGINE_MEMORY = "Memory"
ENGINES = [ENGINE_MERGE_TREE, ENGINE_AGGREGATING_MERGE_TREE, ENGINE_MEMORY]
EngineType = Literal["MergeTree", "AggregatingMergeTree", "Memory"]
ENGINE_DEFAULT = ENGINE_MERGE_TREE

# Orient constants for data() method
ORIENT_DICT = "dict"
ORIENT_RECORDS = "records"

# GroupBy aggregation operator constants
GB_SUM = "sum"
GB_MEAN = "mean"
GB_MIN = "min"
GB_MAX = "max"
GB_COUNT = "count"
GB_STD = "std"
GB_VAR = "var"
GB_ANY = "any"
GroupByOpType = Literal["sum", "mean", "min", "max", "count", "std", "var", "any"]

# Value type aliases for factory functions
ValueScalarType = Union[int, float, bool, str, datetime]
ValueListType = Union[List[int], List[float], List[bool], List[str], List[datetime]]
ValueDictType = Dict[str, Union[ValueScalarType, ValueListType]]
ValueRecordType = List[ValueDictType]
ValueType = Union[ValueScalarType, ValueListType, ValueDictType, ValueRecordType]


@dataclass
class QueryInfo:
    """
    Query information for database operations.

    Couples the data source (which may be a subquery) with the base table name
    and schema metadata. This makes it easier to pass all required values together
    in operator operations without querying system tables.

    Attributes:
        source: Data source - either a table name or a wrapped subquery like "(SELECT ...)"
        base_table: Base table name (always a simple table name)
        value_column: Column name containing the value ("value" or a dict field name)
        fieldtype: Fieldtype of the value column ('s' for scalar, 'a' for array)
        value_type: ClickHouse type of the value column (e.g., 'Int64', 'Float64')
    """
    source: str
    base_table: str
    value_column: str
    fieldtype: str
    value_type: str
    nullable: bool = False


@dataclass
class IngestQueryInfo(QueryInfo):
    """
    Extended QueryInfo carrying full column schema for concat/insert operations.

    Adds column metadata so concat_objects_db and insert_objects_db can validate
    schemas without querying system.columns.
    """
    columns: Dict[str, "ColumnInfo"] = field(default_factory=dict)


@dataclass
class CopyInfo:
    """
    Info for copy operations at database level.

    Attributes:
        source_query: Data source - table name or subquery "(SELECT ...)"
        fieldtype: Overall fieldtype - 's' for scalar, 'a' for array, 'd' for dict
        columns: Column name to ClickHouse type mapping (from cached schema)
        selected_fields: Fields to select from dict (None for base copy)
        is_single_field: True if single field selection
    """

    source_query: str
    fieldtype: str
    columns: Dict[str, "ColumnInfo"]
    selected_fields: Optional[List[str]] = None
    is_single_field: bool = False


@dataclass
class Schema:
    """
    Schema definition for Object tables. Also serves as Object metadata
    when table is set.

    Attributes:
        fieldtype: Overall fieldtype - 's' for scalar, 'a' for array, 'd' for dict
        columns: Dict mapping column names to ColumnInfo
        table: ClickHouse table name (empty for blueprints, set for realized objects)
        col_fieldtype: Per-column fieldtype for ClickHouse COMMENT. Defaults to fieldtype.
                       For dict schemas, distinguishes array data ('a') from scalar data ('s').
        engine: ClickHouse table engine. If None, uses context's engine setting.
        order_by: ORDER BY key for MergeTree-family engines. If None, defaults to tuple().
    """

    fieldtype: str
    columns: Dict[str, "ColumnInfo"]
    table: Optional[str] = None
    col_fieldtype: Optional[str] = None
    engine: Optional["EngineType"] = None
    order_by: Optional[str] = None


@dataclass
class ViewSchema(Schema):
    """
    Metadata for a View. Inherits fieldtype, columns, and table from Schema.

    Attributes:
        where: WHERE clause constraint (or None)
        limit: LIMIT constraint (or None)
        offset: OFFSET constraint (or None)
        order_by: ORDER BY clause (or None)
        selected_fields: List of selected column names (single-field=[name], multi-field=[...])
    """

    where: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    order_by: Optional[str] = None
    selected_fields: Optional[List[str]] = None
    computed_columns: Optional[Dict[str, "Computed"]] = None


@dataclass
class GroupByInfo:
    """
    Info for group_by operations at database level.

    Attributes:
        source: Data source - table name or subquery "(SELECT ...)"
        base_table: Base table name (always a simple table name)
        group_keys: Column names to group by
        columns: All source columns {name: ClickHouse type}
        fieldtype: Source fieldtype ('a' for array, 'd' for dict)
    """

    source: str
    base_table: str
    group_keys: List[str]
    columns: Dict[str, str]
    fieldtype: str
    having: Optional[str] = None


@dataclass
class ColumnMeta:
    """
    Metadata for a column parsed from YAML comment.

    Attributes:
        fieldtype: 's' for scalar, 'a' for array
    """

    fieldtype: Optional[str] = None

    def to_yaml(self) -> str:
        """
        Convert metadata to single-line YAML format for column comment.

        Returns:
            str: YAML string like "{fieldtype: a}"
        """
        if self.fieldtype is None:
            return ""

        return yaml.dump({"fieldtype": self.fieldtype}, default_flow_style=True).strip()

    @classmethod
    def from_yaml(cls, comment: str) -> "ColumnMeta":
        """
        Parse YAML from column comment string.

        Args:
            comment: Column comment string containing YAML

        Returns:
            ColumnMeta: Parsed metadata
        """
        if not comment or not comment.strip():
            return cls()

        try:
            data = yaml.safe_load(comment)
            if not isinstance(data, dict):
                return cls()

            return cls(fieldtype=data.get("fieldtype"))
        except yaml.YAMLError:
            return cls()
