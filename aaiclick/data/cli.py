"""CLI helper functions for persistent data commands.

Provides async commands used by __main__.py for managing
persistent named objects via the command line.
"""

from __future__ import annotations

from datetime import datetime

from .data_context import (
    data_context,
    delete_persistent_object,
    delete_persistent_objects,
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

        schema = obj.schema
        print(f"Name:      {name}")
        print(f"Table:     {obj.table}")
        print(f"Fieldtype: {schema.fieldtype}")
        print("Columns:")
        for col_name, col_info in schema.columns.items():
            if col_name == "aai_id":
                continue
            print(f"  {col_name}: {col_info.type}")


async def delete_object_cmd(name: str) -> None:
    """Delete a single persistent object by name."""
    async with data_context():
        await delete_persistent_object(name)
        print(f"Deleted persistent object '{name}'")


async def delete_objects_cmd(
    *,
    after: str | None = None,
    before: str | None = None,
) -> None:
    """Delete persistent objects filtered by creation time."""
    after_dt = _parse_datetime(after) if after else None
    before_dt = _parse_datetime(before) if before else None

    async with data_context():
        deleted = await delete_persistent_objects(after=after_dt, before=before_dt)

        if not deleted:
            print("No persistent objects matched the filter")
            return

        parts = []
        if after_dt:
            parts.append(f"after {after_dt}")
        if before_dt:
            parts.append(f"before {before_dt}")
        filter_str = f" ({', '.join(parts)})" if parts else ""
        print(f"Deleted {len(deleted)} persistent object(s){filter_str}:")
        for name in deleted:
            print(f"  {name}")
