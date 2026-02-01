"""
aaiclick.data.models - Data models and type definitions for the aaiclick framework.

This module provides dataclasses, type literals, and constants used throughout the framework.
"""

from typing import Optional, Dict, Union, Literal, List, NamedTuple
from dataclasses import dataclass

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

# Value type aliases for factory functions
ValueScalarType = Union[int, float, bool, str]
ValueListType = Union[List[int], List[float], List[bool], List[str]]
ValueType = Union[ValueScalarType, ValueListType, Dict[str, Union[ValueScalarType, ValueListType]]]


class QueryInfo(NamedTuple):
    """
    Query information for database operations.

    Couples the data source (which may be a subquery) with the base table name
    (used for metadata queries). This makes it easier to pass both values together
    in operator and concat operations.

    Attributes:
        source: Data source - either a table name or a wrapped subquery like "(SELECT ...)"
        base_table: Base table name for metadata queries (always a simple table name)
    """
    source: str
    base_table: str


@dataclass
class Schema:
    """
    Schema definition for creating Object tables.

    Attributes:
        fieldtype: Overall fieldtype - 's' for scalar, 'a' for array, 'd' for dict
        columns: Dict mapping column names to ClickHouse column types
    """

    fieldtype: str
    columns: Dict[str, Union[ColumnType, str]]


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
