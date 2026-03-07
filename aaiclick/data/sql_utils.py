"""
aaiclick.data.sql_utils - SQL utility functions for safe identifier handling.
"""

from __future__ import annotations

from typing import Union

from ..snowflake_id import reserve_snowflake_ids_sql


def quote_identifier(name: str) -> str:
    """Backtick-quote a ClickHouse identifier, escaping internal backticks."""
    return f"`{name.replace('`', '``')}`"


def _format_sql_value(val: Union[int, float, bool, str]) -> str:
    """Format a Python scalar as a ClickHouse SQL literal."""
    if isinstance(val, bool):
        return "1" if val else "0"
    elif isinstance(val, str):
        escaped = val.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    else:
        return str(val)


def _format_sql_array(values: list) -> str:
    """Format a Python list as a ClickHouse array literal."""
    return f"[{', '.join(_format_sql_value(v) for v in values)}]"


def values_to_select(val: Union[int, float, bool, str, list, dict]) -> str | None:
    """Convert Python value to a ClickHouse SELECT query string (without aai_id).

    Returns None for empty data (empty list, dict with empty arrays).
    """
    if isinstance(val, dict):
        has_arrays = any(isinstance(v, list) for v in val.values())
        if has_arrays:
            first_len = None
            for v in val.values():
                if isinstance(v, list):
                    if first_len is None:
                        first_len = len(v)
                    if not v:
                        return None
            arr_exprs = [f"{_format_sql_array(v)} AS {quote_identifier(k)}" for k, v in val.items()]
            join_cols = ", ".join(quote_identifier(k) for k in val.keys())
            return (
                f"SELECT {join_cols} "
                f"FROM (SELECT {', '.join(arr_exprs)}) "
                f"ARRAY JOIN {join_cols}"
            )
        else:
            exprs = [f"{_format_sql_value(v)} AS {quote_identifier(k)}" for k, v in val.items()]
            return f"SELECT {', '.join(exprs)}"
    elif isinstance(val, list):
        if not val:
            return None
        return f"SELECT value FROM (SELECT arrayJoin({_format_sql_array(val)}) AS value)"
    else:
        return f"SELECT {_format_sql_value(val)} AS value"


async def insert_with_ids(
    ch_client, table: str, select_cols: str, from_clause: str,
    *, count: int | None = None,
) -> None:
    """INSERT...SELECT with SQL-generated snowflake IDs prepended.

    Pass count to skip the COUNT round-trip when the caller already knows it.
    No-op if count is 0.
    """
    if count is None:
        count_result = await ch_client.query(f"SELECT count() {from_clause}")
        count = count_result.result_rows[0][0]
    if count == 0:
        return
    id_expr = reserve_snowflake_ids_sql(count)
    await ch_client.command(
        f"INSERT INTO {table} SELECT {id_expr} AS aai_id, {select_cols} {from_clause}"
    )
