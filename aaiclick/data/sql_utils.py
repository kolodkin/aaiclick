"""
aaiclick.data.sql_utils - SQL utility functions for safe identifier handling.
"""


def quote_identifier(name: str) -> str:
    """Backtick-quote a ClickHouse identifier, escaping internal backticks."""
    return f"`{name.replace('`', '``')}`"
