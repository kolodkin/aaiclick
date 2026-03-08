"""
aaiclick.data.models - Data models and type definitions for the aaiclick framework.

This module provides dataclasses, type literals, and constants used throughout the framework.
"""

from typing import Optional, Dict, Union, Literal, List
from dataclasses import dataclass, field

import yaml


@dataclass(frozen=True)
class ClickHouseCreds:
    """ClickHouse connection credentials."""

    host: str = "localhost"
    port: int = 8123
    user: str = "default"
    password: str = ""
    database: str = "default"


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
NUMERIC_TYPES = INT_TYPES | FLOAT_TYPES


@dataclass(frozen=True)
class ColumnDef:
    """Definition for a single column including base type and nullability.

    Attributes:
        type: Base ClickHouse type (e.g., 'Int64', 'String')
        nullable: Whether the column allows NULL values
    """

    type: str
    nullable: bool = False

    def ch_type(self) -> str:
        """Return the ClickHouse DDL type string.

        Returns 'Nullable(Int64)' when nullable=True, 'Int64' otherwise.
        """
        return f"Nullable({self.type})" if self.nullable else self.type


def parse_ch_type(type_str: str) -> ColumnDef:
    """Parse a ClickHouse type string into a ColumnDef.

    Handles both plain types ('Int64') and nullable types ('Nullable(Int64)').

    Args:
        type_str: ClickHouse type string from system.columns

    Returns:
        ColumnDef with extracted base type and nullable flag
    """
    if type_str.startswith("Nullable(") and type_str.endswith(")"):
        return ColumnDef(type=type_str[9:-1], nullable=True)
    return ColumnDef(type=type_str, nullable=False)


# Fieldtype constants
FIELDTYPE_SCALAR = "s"
FIELDTYPE_ARRAY = "a"
FIELDTYPE_DICT = "d"

# ClickHouse engine constants
ENGINE_MERGE_TREE = "MergeTree"
ENGINE_MEMORY = "Memory"
ENGINES = [ENGINE_MERGE_TREE, ENGINE_MEMORY]
EngineType = Literal["MergeTree", "Memory"]
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
GroupByOpType = Literal["sum", "mean", "min", "max", "count", "std", "var"]

# Value type aliases for factory functions
ValueScalarType = Union[int, float, bool, str]
ValueListType = Union[List[int], List[float], List[bool], List[str]]
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
    columns: Dict[str, "ColumnDef"] = field(default_factory=dict)


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
    columns: Dict[str, "ColumnDef"]
    selected_fields: Optional[List[str]] = None
    is_single_field: bool = False


@dataclass
class Schema:
    """
    Schema definition for creating Object tables.

    Attributes:
        fieldtype: Overall fieldtype - 's' for scalar, 'a' for array, 'd' for dict
        columns: Dict mapping column names to ClickHouse column types
    """

    fieldtype: str
    columns: Dict[str, "ColumnDef"]


@dataclass
class ColumnInfo:
    """
    Information about a single column including type and metadata.

    Attributes:
        name: Column name
        type: ClickHouse data type (e.g., 'Int64', 'Float64', 'String')
        fieldtype: 's' for scalar, 'a' for array, or None if not set
    """

    name: str
    type: str
    fieldtype: Optional[str] = None
    nullable: bool = False


@dataclass
class ObjectMetadata:
    """
    Metadata for an Object including table name, fieldtype, and column information.

    Attributes:
        table: ClickHouse table name
        fieldtype: Overall object fieldtype - 's' for scalar, 'a' for array, 'd' for dict
        columns: Dict mapping column name to ColumnInfo
    """

    table: str
    fieldtype: str
    columns: Dict[str, ColumnInfo]


@dataclass
class ViewMetadata:
    """
    Metadata for a View including table info and view constraints.

    Attributes:
        table: ClickHouse table name (from source Object)
        fieldtype: Overall object fieldtype - 's' for scalar, 'a' for array, 'd' for dict
        columns: Dict mapping column name to ColumnInfo
        where: WHERE clause constraint (or None)
        limit: LIMIT constraint (or None)
        offset: OFFSET constraint (or None)
        order_by: ORDER BY clause (or None)
        selected_fields: List of selected column names (single-field=[name], multi-field=[...])
    """

    table: str
    fieldtype: str
    columns: Dict[str, ColumnInfo]
    where: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    order_by: Optional[str] = None
    selected_fields: Optional[List[str]] = None


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
