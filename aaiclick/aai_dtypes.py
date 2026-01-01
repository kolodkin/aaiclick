"""
aaiclick.aai_dtypes - Datatype and column metadata handling.

This module provides YAML-based parsing for column comments that contain
datatype and fieldtype information following numpy dtype conventions.

References:
- NumPy Array Interface: https://numpy.org/doc/stable/reference/arrays.interface.html
- NumPy Structured Arrays: https://numpy.org/doc/stable/user/basics.rec.html
"""

from typing import Optional
from dataclasses import dataclass
import yaml


# Fieldtype constants
FIELDTYPE_SCALAR = "s"
FIELDTYPE_ARRAY = "a"


@dataclass
class ColumnMeta:
    """
    Metadata for a column parsed from YAML comment.

    Attributes:
        datatype: NumPy-style dtype string (e.g., 'i4', 'f8', 'U10')
        fieldtype: 's' for scalar, 'a' for array
    """

    datatype: Optional[str] = None
    fieldtype: Optional[str] = None

    def to_yaml(self) -> str:
        """
        Convert metadata to single-line YAML format for column comment.

        Returns:
            str: YAML string like "{datatype: i4, fieldtype: a}"
        """
        parts = {}
        if self.datatype is not None:
            parts["datatype"] = self.datatype
        if self.fieldtype is not None:
            parts["fieldtype"] = self.fieldtype

        if not parts:
            return ""

        return yaml.dump(parts, default_flow_style=True).strip()

    @classmethod
    def from_yaml(cls, comment: str) -> "ColumnMeta":
        """
        Parse YAML from column comment string.

        Args:
            comment: Column comment string containing YAML

        Returns:
            ColumnMeta: Parsed metadata

        Examples:
            >>> ColumnMeta.from_yaml("{datatype: i4, fieldtype: a}")
            ColumnMeta(datatype='i4', fieldtype='a')
            >>> ColumnMeta.from_yaml("{datatype: f8, fieldtype: s}")
            ColumnMeta(datatype='f8', fieldtype='s')
        """
        if not comment or not comment.strip():
            return cls()

        try:
            data = yaml.safe_load(comment)
            if not isinstance(data, dict):
                return cls()

            return cls(
                datatype=data.get("datatype"),
                fieldtype=data.get("fieldtype"),
            )
        except yaml.YAMLError:
            return cls()


def clickhouse_type_to_dtype(ch_type: str) -> str:
    """
    Convert ClickHouse type to numpy dtype string.

    Args:
        ch_type: ClickHouse type string

    Returns:
        str: NumPy dtype string

    Examples:
        >>> clickhouse_type_to_dtype("Int64")
        'i8'
        >>> clickhouse_type_to_dtype("Float64")
        'f8'
    """
    type_map = {
        "Int8": "i1",
        "Int16": "i2",
        "Int32": "i4",
        "Int64": "i8",
        "UInt8": "u1",
        "UInt16": "u2",
        "UInt32": "u4",
        "UInt64": "u8",
        "Float32": "f4",
        "Float64": "f8",
        "String": "O",  # Python object (string)
    }
    return type_map.get(ch_type, "O")


def dtype_to_clickhouse_type(dtype: str) -> str:
    """
    Convert numpy dtype string to ClickHouse type.

    Args:
        dtype: NumPy dtype string

    Returns:
        str: ClickHouse type string

    Examples:
        >>> dtype_to_clickhouse_type("i8")
        'Int64'
        >>> dtype_to_clickhouse_type("f8")
        'Float64'
    """
    type_map = {
        "i1": "Int8",
        "i2": "Int16",
        "i4": "Int32",
        "i8": "Int64",
        "u1": "UInt8",
        "u2": "UInt16",
        "u4": "UInt32",
        "u8": "UInt64",
        "f4": "Float32",
        "f8": "Float64",
        "O": "String",
        # Common aliases
        "int8": "Int8",
        "int16": "Int16",
        "int32": "Int32",
        "int64": "Int64",
        "uint8": "UInt8",
        "uint16": "UInt16",
        "uint32": "UInt32",
        "uint64": "UInt64",
        "float32": "Float32",
        "float64": "Float64",
    }
    return type_map.get(dtype, "String")
