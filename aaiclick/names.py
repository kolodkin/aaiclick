"""Name rule shared by objects, jobs and registered jobs.

Letters, digits and ``_``, not starting with a digit: such a name is safe in
a table name, a prompt word and a ``LIKE`` pattern alike.
"""

from __future__ import annotations

import re

VALID_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
MAX_NAME_LEN = 128


def name_error(kind: str, name: str) -> str | None:
    """Why ``name`` breaks the rule, or ``None`` when it is valid."""
    if not VALID_NAME_RE.match(name):
        return f"Invalid {kind} name '{name}': must match [a-zA-Z_][a-zA-Z0-9_]*"
    if len(name) > MAX_NAME_LEN:
        return f"Invalid {kind} name: {len(name)} characters exceeds the {MAX_NAME_LEN}-character limit"
    return None
