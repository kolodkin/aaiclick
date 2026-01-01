"""
aaiclick.object - Core Object class for the aaiclick framework.

This module provides the Object class that represents data in ClickHouse tables
and supports operations through operator overloading.
"""

from typing import Optional
from .client import get_client


class Object:
    """
    Represents a data object stored in a ClickHouse table.

    Each Object instance corresponds to a ClickHouse table and supports
    operations like addition and subtraction that create new tables with results.
    """

    def __init__(self, name: str, table: Optional[str] = None):
        """
        Initialize an Object.

        Args:
            name: Name of the object
            table: Optional table name. If not provided, generates unique table name
        """
        self._name = name
        self._table_name = table if table is not None else f"{name}_{id(self)}"

    @property
    def table(self) -> str:
        """Get the table name for this object."""
        return self._table_name

    @property
    def name(self) -> str:
        """Get the name of this object."""
        return self._name

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
        result_name = f"{self._name}_plus_{other._name}"
        result = Object(result_name)

        # Execute the addition operation in ClickHouse
        client = await get_client()
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {result.table}
        ENGINE = Memory
        AS SELECT a.value + b.value AS value
        FROM {self.table} AS a, {other.table} AS b
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
        result_name = f"{self._name}_minus_{other._name}"
        result = Object(result_name)

        # Execute the subtraction operation in ClickHouse
        client = await get_client()
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {result.table}
        ENGINE = Memory
        AS SELECT a.value - b.value AS value
        FROM {self.table} AS a, {other.table} AS b
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
        return f"Object(name='{self._name}', table='{self._table_name}')"
