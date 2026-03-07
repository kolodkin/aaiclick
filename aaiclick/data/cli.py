"""CLI helper functions for persistent data commands.

Provides async commands used by __main__.py for managing
persistent named objects via the command line.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from .data_context import (
    data_context,
    delete_persistent_object,
    list_persistent_objects,
    open_object,
)


def _parse_datetime(value: str) -> datetime:
    """Parse a datetime string in ISO 8601 format.

    Supports:
        2026-03-07
        2026-03-07T15:00:00
        2026-03-07T15:00:00Z
    """
    value = value.rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Invalid datetime format: {value!r}. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS")


async def list_objects_cmd() -> None:
    """List all persistent objects."""
    async with data_context():
        names = await list_persistent_objects()
        if not names:
            print("No persistent objects found")
            return

        print(f"{'Name':<40}")
        print("-" * 40)
        for name in sorted(names):
            print(f"{name:<40}")
        print(f"\nTotal: {len(names)}")


async def show_object_cmd(name: str) -> None:
    """Show details of a persistent object."""
    async with data_context():
        try:
            obj = await open_object(name)
        except RuntimeError:
            print(f"Persistent object not found: {name}")
            return

        meta = await obj.metadata()
        print(f"Name:      {name}")
        print(f"Table:     {obj.table}")
        print(f"Fieldtype: {meta.fieldtype}")
        print(f"Columns:")
        for col_name, col_info in meta.columns.items():
            if col_name == "aai_id":
                continue
            print(f"  {col_name}: {col_info.type}")


async def delete_object_cmd(
    name: str,
    *,
    since: Optional[str] = None,
    before: Optional[str] = None,
) -> None:
    """Delete a persistent object or rows within a time range."""
    since_dt = _parse_datetime(since) if since else None
    before_dt = _parse_datetime(before) if before else None

    async with data_context():
        await delete_persistent_object(name, since=since_dt, before=before_dt)

        if since_dt or before_dt:
            parts = []
            if since_dt:
                parts.append(f"since {since_dt}")
            if before_dt:
                parts.append(f"before {before_dt}")
            print(f"Deleted rows from '{name}' ({', '.join(parts)})")
        else:
            print(f"Deleted persistent object '{name}'")
