"""
aaiclick.config - Configuration management.

This module provides a singleton configuration class for managing
global settings like table TTL (time-to-live).
"""

from . import env


class Config:
    """
    Singleton configuration class for aaiclick.

    This class manages global settings including table TTL.
    Access the singleton instance via Config.get_instance().
    """

    _instance = None

    def __init__(self):
        """Initialize configuration with default values."""
        self._table_ttl_days = env.OBJECT_TABLE_TTL

    @classmethod
    def get_instance(cls) -> "Config":
        """
        Get the singleton Config instance.

        Returns:
            Config: The singleton configuration instance
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @property
    def table_ttl_days(self) -> int:
        """
        Get the TTL (time-to-live) for Object tables in days.

        Returns:
            int: Number of days before table data expires
        """
        return self._table_ttl_days

    @table_ttl_days.setter
    def table_ttl_days(self, value: int):
        """
        Set the TTL (time-to-live) for Object tables in days.

        Args:
            value: Number of days before table data expires
        """
        self._table_ttl_days = value


def get_config() -> Config:
    """
    Get the global Config instance.

    This is a convenience function for accessing the singleton Config.

    Returns:
        Config: The singleton configuration instance
    """
    return Config.get_instance()


def get_ttl_clause() -> str:
    """
    Get the TTL clause for CREATE TABLE statements.

    The TTL is based on the created_at column, which stores the table creation
    timestamp as a DateTime value.

    Returns:
        str: TTL clause string for ClickHouse tables
    """
    config = get_config()
    return f"TTL created_at + INTERVAL {config.table_ttl_days} DAY"
