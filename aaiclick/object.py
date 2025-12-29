"""
aaiclick.object - Base object classes for the aaiclick framework.

This module provides core object classes that represent Python objects
and their translations to ClickHouse operations.
"""

from typing import Any, Dict, Optional, List
from abc import ABC, abstractmethod


class ClickHouseObject(ABC):
    """
    Base class for all aaiclick objects that can be translated to ClickHouse operations.
    """

    def __init__(self, name: Optional[str] = None):
        self.name = name
        self._metadata: Dict[str, Any] = {}

    @abstractmethod
    def to_clickhouse(self) -> str:
        """
        Convert the object to a ClickHouse SQL expression.

        Returns:
            str: ClickHouse SQL expression
        """
        pass

    def set_metadata(self, key: str, value: Any) -> None:
        """Set metadata for this object."""
        self._metadata[key] = value

    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata for this object."""
        return self._metadata.get(key, default)


class DataFrameObject(ClickHouseObject):
    """
    Represents a DataFrame-like object that maps to a ClickHouse table or query.
    """

    def __init__(self, table_name: Optional[str] = None, query: Optional[str] = None):
        super().__init__(name=table_name)
        self.table_name = table_name
        self.query = query
        self.columns: List[str] = []
        self.filters: List[str] = []
        self.transformations: List[str] = []

    def to_clickhouse(self) -> str:
        """
        Generate the ClickHouse query for this DataFrame object.

        Returns:
            str: Complete ClickHouse SQL query
        """
        if self.query:
            return self.query

        if not self.table_name:
            raise ValueError("Either table_name or query must be provided")

        # Build SELECT clause
        select_clause = "*" if not self.columns else ", ".join(self.columns)

        # Build WHERE clause
        where_clause = ""
        if self.filters:
            where_clause = f" WHERE {' AND '.join(self.filters)}"

        return f"SELECT {select_clause} FROM {self.table_name}{where_clause}"

    def select(self, *columns: str) -> 'DataFrameObject':
        """Select specific columns."""
        self.columns = list(columns)
        return self

    def filter(self, condition: str) -> 'DataFrameObject':
        """Add a filter condition."""
        self.filters.append(condition)
        return self


class ColumnObject(ClickHouseObject):
    """
    Represents a column in a ClickHouse table.
    """

    def __init__(self, name: str, dtype: Optional[str] = None):
        super().__init__(name=name)
        self.dtype = dtype

    def to_clickhouse(self) -> str:
        """
        Convert column to ClickHouse column reference.

        Returns:
            str: Column name for use in ClickHouse queries
        """
        return self.name


class ExpressionObject(ClickHouseObject):
    """
    Represents an expression that can be evaluated in ClickHouse.
    """

    def __init__(self, expression: str, name: Optional[str] = None):
        super().__init__(name=name)
        self.expression = expression

    def to_clickhouse(self) -> str:
        """
        Convert expression to ClickHouse SQL.

        Returns:
            str: ClickHouse SQL expression
        """
        return self.expression


class AggregationObject(ClickHouseObject):
    """
    Represents an aggregation operation.
    """

    def __init__(self, function: str, column: str, name: Optional[str] = None):
        super().__init__(name=name)
        self.function = function
        self.column = column

    def to_clickhouse(self) -> str:
        """
        Convert aggregation to ClickHouse SQL.

        Returns:
            str: ClickHouse aggregation expression
        """
        return f"{self.function}({self.column})"
