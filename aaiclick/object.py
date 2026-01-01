"""
aaiclick.object - Core Object class for the aaiclick framework.

This module provides the Object class that represents data in ClickHouse tables
and supports operations through operator overloading.
"""

from typing import Optional
from .client import get_client
from .snowflake import generate_snowflake_id


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
        Get the data from the object's table as a list of tuples.

        Returns:
            List of tuples containing all rows from the table
        """
        result = await self.result()
        return result.result_rows

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

        # Execute the addition operation in ClickHouse
        client = await get_client()
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {result.table}
        ENGINE = Memory
        AS SELECT a.value + b.value AS value
        FROM (
            SELECT value, ROW_NUMBER() OVER () AS row_num
            FROM {self.table}
        ) AS a
        JOIN (
            SELECT value, ROW_NUMBER() OVER () AS row_num
            FROM {other.table}
        ) AS b
        ON a.row_num = b.row_num
        """
        await client.command(create_query)

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

        # Execute the subtraction operation in ClickHouse
        client = await get_client()
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {result.table}
        ENGINE = Memory
        AS SELECT a.value - b.value AS value
        FROM (
            SELECT value, ROW_NUMBER() OVER () AS row_num
            FROM {self.table}
        ) AS a
        JOIN (
            SELECT value, ROW_NUMBER() OVER () AS row_num
            FROM {other.table}
        ) AS b
        ON a.row_num = b.row_num
        """
        await client.command(create_query)

        return result

    async def delete_table(self) -> None:
        """
        Delete the ClickHouse table associated with this object.
        """
        client = await get_client()
        await client.command(f"DROP TABLE IF EXISTS {self.table}")

    def __repr__(self) -> str:
        """String representation of the Object."""
        return f"Object(table='{self._table_name}')"
