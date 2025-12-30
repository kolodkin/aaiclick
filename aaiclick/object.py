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
        client = get_client()
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
        client = get_client()
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {result.table}
        ENGINE = Memory
        AS SELECT a.value - b.value AS value
        FROM {self.table} AS a, {other.table} AS b
        """
        await client.command(create_query)

        return result

    @staticmethod
    async def create(name: str, coltype: str) -> "Object":
        """
        Create a new Object with a ClickHouse table.

        Args:
            name: Name for the object
            coltype: Column type definition (e.g., "value Float64")

        Returns:
            Object: New Object instance with created table
        """
        obj = Object(name)
        client = get_client()
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {obj.table} (
            {coltype}
        ) ENGINE = Memory
        """
        await client.command(create_query)
        return obj

    @staticmethod
    async def create_from_value(name: str, val: "Object") -> "Object":
        """
        Create a new Object from an existing Object's values.

        Args:
            name: Name for the new object
            val: Source Object to copy data from

        Returns:
            Object: New Object instance with copied data
        """
        obj = Object(name)
        client = get_client()
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {obj.table}
        ENGINE = Memory
        AS SELECT * FROM {val.table}
        """
        await client.command(create_query)
        return obj

    async def delete_table(self) -> None:
        """
        Delete the ClickHouse table associated with this object.
        """
        client = get_client()
        await client.command(f"DROP TABLE IF EXISTS {self.table}")

    def __repr__(self) -> str:
        """String representation of the Object."""
        return f"Object(name='{self._name}', table='{self._table_name}')"
