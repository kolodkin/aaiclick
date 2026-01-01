"""
aaiclick.object - Core Object class for the aaiclick framework.

This module provides the Object class that represents data in ClickHouse tables
and supports operations through operator overloading.
"""

from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass
import yaml
from .client import get_client
from .snowflake import generate_snowflake_id


# Fieldtype constants
FIELDTYPE_SCALAR = "s"
FIELDTYPE_ARRAY = "a"


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


@dataclass
class DataResult:
    """
    Result container for Object.data() that includes both rows and column metadata.

    Attributes:
        rows: List of tuples containing row data
        columns: Dict mapping column name to ColumnMeta with datatype/fieldtype info
    """

    rows: List[Tuple[Any, ...]]
    columns: Dict[str, ColumnMeta]


class Object:
    """
    Represents a data object stored in a ClickHouse table.

    Each Object instance corresponds to a ClickHouse table and supports
    operations like addition and subtraction that create new tables with results.
    """

    def __init__(self, table: Optional[str] = None):
        """
        Initialize an Object.

        Args:
            table: Optional table name. If not provided, generates unique table name
                  using Snowflake ID prefixed with 't' for ClickHouse compatibility
        """
        self._table_name = table if table is not None else f"t{generate_snowflake_id()}"

    @property
    def table(self) -> str:
        """Get the table name for this object."""
        return self._table_name

    async def result(self):
        """
        Query and return all data from the object's table.

        Returns:
            Query result with all rows from the table
        """
        client = await get_client()
        return await client.query(f"SELECT * FROM {self.table}")

    async def data(self):
        """
        Get the data from the object's table.

        Returns:
            - For scalar: returns the value directly
            - For array: returns list of values
            - For dict: returns dict with column names as keys
        """
        client = await get_client()

        # Query column names and comments
        columns_query = f"""
        SELECT name, comment
        FROM system.columns
        WHERE table = '{self.table}'
        ORDER BY position
        """
        columns_result = await client.query(columns_query)

        # Parse YAML from comments and get column names
        columns: Dict[str, ColumnMeta] = {}
        column_names: List[str] = []
        for name, comment in columns_result.result_rows:
            columns[name] = ColumnMeta.from_yaml(comment)
            column_names.append(name)

        # Query data
        result = await self.result()
        rows = result.result_rows

        # Determine data type based on columns
        has_row_id = "row_id" in columns
        has_value = "value" in columns
        is_simple_structure = set(column_names) <= {"row_id", "value"}

        if not is_simple_structure:
            # Dict: return dict with column names as keys
            if rows:
                return {name: rows[0][i] for i, name in enumerate(column_names)}
            return {}

        value_meta = columns.get("value")
        if value_meta and value_meta.fieldtype == FIELDTYPE_SCALAR:
            # Scalar: return single value
            return rows[0][0] if rows else None
        else:
            # Array: return list of values
            if has_row_id:
                return [row[1] for row in rows]
            else:
                return [row[0] for row in rows]

    async def _has_row_id(self) -> bool:
        """Check if this object's table has a row_id column."""
        client = await get_client()
        columns_query = f"""
        SELECT name FROM system.columns
        WHERE table = '{self.table}' AND name = 'row_id'
        """
        result = await client.query(columns_query)
        return len(result.result_rows) > 0

    async def _get_fieldtype(self) -> Optional[str]:
        """Get the fieldtype of the value column."""
        client = await get_client()
        columns_query = f"""
        SELECT comment FROM system.columns
        WHERE table = '{self.table}' AND name = 'value'
        """
        result = await client.query(columns_query)
        if result.result_rows:
            meta = ColumnMeta.from_yaml(result.result_rows[0][0])
            return meta.fieldtype
        return None

    async def __add__(self, other: "Object") -> "Object":
        """
        Add two objects together.

        Creates a new Object with a table containing the result of element-wise addition.

        Args:
            other: Another Object to add

        Returns:
            Object: New Object instance pointing to result table
        """
        result = Object()
        client = await get_client()

        # Check if operating on scalars or arrays
        has_row_id = await self._has_row_id()
        fieldtype = await self._get_fieldtype()
        comment = ColumnMeta(fieldtype=fieldtype).to_yaml()

        if has_row_id:
            # Array operation with row_id
            row_id_comment = ColumnMeta(fieldtype=FIELDTYPE_SCALAR).to_yaml()
            create_query = f"""
            CREATE TABLE {result.table}
            ENGINE = MergeTree ORDER BY tuple()
            AS SELECT a.row_id, a.value + b.value AS value
            FROM {self.table} AS a
            JOIN {other.table} AS b
            ON a.row_id = b.row_id
            """
            await client.command(create_query)
            # Add comments
            await client.command(f"ALTER TABLE {result.table} COMMENT COLUMN row_id '{row_id_comment}'")
            await client.command(f"ALTER TABLE {result.table} COMMENT COLUMN value '{comment}'")
        else:
            # Scalar operation
            create_query = f"""
            CREATE TABLE {result.table}
            ENGINE = MergeTree ORDER BY tuple()
            AS SELECT a.value + b.value AS value
            FROM {self.table} AS a, {other.table} AS b
            """
            await client.command(create_query)
            await client.command(f"ALTER TABLE {result.table} COMMENT COLUMN value '{comment}'")

        return result

    async def __sub__(self, other: "Object") -> "Object":
        """
        Subtract one object from another.

        Creates a new Object with a table containing the result of element-wise subtraction.

        Args:
            other: Another Object to subtract

        Returns:
            Object: New Object instance pointing to result table
        """
        result = Object()
        client = await get_client()

        # Check if operating on scalars or arrays
        has_row_id = await self._has_row_id()
        fieldtype = await self._get_fieldtype()
        comment = ColumnMeta(fieldtype=fieldtype).to_yaml()

        if has_row_id:
            # Array operation with row_id
            row_id_comment = ColumnMeta(fieldtype=FIELDTYPE_SCALAR).to_yaml()
            create_query = f"""
            CREATE TABLE {result.table}
            ENGINE = MergeTree ORDER BY tuple()
            AS SELECT a.row_id, a.value - b.value AS value
            FROM {self.table} AS a
            JOIN {other.table} AS b
            ON a.row_id = b.row_id
            """
            await client.command(create_query)
            await client.command(f"ALTER TABLE {result.table} COMMENT COLUMN row_id '{row_id_comment}'")
            await client.command(f"ALTER TABLE {result.table} COMMENT COLUMN value '{comment}'")
        else:
            # Scalar operation
            create_query = f"""
            CREATE TABLE {result.table}
            ENGINE = MergeTree ORDER BY tuple()
            AS SELECT a.value - b.value AS value
            FROM {self.table} AS a, {other.table} AS b
            """
            await client.command(create_query)
            await client.command(f"ALTER TABLE {result.table} COMMENT COLUMN value '{comment}'")

        return result

    async def delete_table(self) -> None:
        """
        Delete the ClickHouse table associated with this object.
        """
        client = await get_client()
        await client.command(f"DROP TABLE IF EXISTS {self.table}")

    async def min(self) -> float:
        """
        Calculate the minimum value from the object's table.

        Returns:
            float: Minimum value from the 'value' column
        """
        client = await get_client()
        result = await client.query(f"SELECT min(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def max(self) -> float:
        """
        Calculate the maximum value from the object's table.

        Returns:
            float: Maximum value from the 'value' column
        """
        client = await get_client()
        result = await client.query(f"SELECT max(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def sum(self) -> float:
        """
        Calculate the sum of values from the object's table.

        Returns:
            float: Sum of values from the 'value' column
        """
        client = await get_client()
        result = await client.query(f"SELECT sum(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def mean(self) -> float:
        """
        Calculate the mean (average) value from the object's table.

        Returns:
            float: Mean value from the 'value' column
        """
        client = await get_client()
        result = await client.query(f"SELECT avg(value) FROM {self.table}")
        return result.result_rows[0][0]

    async def std(self) -> float:
        """
        Calculate the standard deviation of values from the object's table.

        Returns:
            float: Standard deviation from the 'value' column
        """
        client = await get_client()
        result = await client.query(f"SELECT stddevPop(value) FROM {self.table}")
        return result.result_rows[0][0]

    def __repr__(self) -> str:
        """String representation of the Object."""
        return f"Object(table='{self._table_name}')"
