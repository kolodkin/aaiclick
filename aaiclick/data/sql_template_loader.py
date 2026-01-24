"""
SQL template loader with caching for aaiclick framework.

This module provides functionality to load SQL templates from files
and cache them for efficient reuse.
"""

from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=None)
def load_sql_template(template_name: str) -> str:
    """
    Load a SQL template from file with caching.

    Templates are loaded from the sql_templates directory and cached
    in memory for efficient reuse.

    Args:
        template_name: Name of the template file (without .sql extension)

    Returns:
        str: Template content with placeholder variables like {operator}, {result_table}, etc.

    Raises:
        FileNotFoundError: If the template file does not exist
    """
    template_dir = Path(__file__).parent / "sql_templates"
    template_path = template_dir / f"{template_name}.sql"

    if not template_path.exists():
        raise FileNotFoundError(f"SQL template not found: {template_path}")

    return template_path.read_text()
