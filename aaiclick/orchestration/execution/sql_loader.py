"""Loader for the shared SQL files in ``sql/``.

Worker differences are bound values, not query edits. Neutral module so both
the execution package and the background package import it without cycles.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path


@cache
def load_sql(name: str) -> str:
    """Read a shared SQL file from the packaged ``sql/`` directory, once."""
    return (Path(__file__).parent / "sql" / name).read_text()
