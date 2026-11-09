"""Name rules for objects, jobs and registered jobs.

Object names become table names: letters, digits and ``_``, not starting with
a digit. Job names may also use ``-``.
"""

from __future__ import annotations

import re

VALID_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
VALID_JOB_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")
MAX_NAME_LEN = 128


def name_error(kind: str, name: str, pattern: re.Pattern[str] = VALID_NAME_RE) -> str | None:
    """Why ``name`` breaks ``pattern``, or ``None`` when it is valid."""
    if not pattern.match(name):
        return f"Invalid {kind} name '{name}': must match {pattern.pattern.strip('^$')}"
    if len(name) > MAX_NAME_LEN:
        return f"Invalid {kind} name: {len(name)} characters exceeds the {MAX_NAME_LEN}-character limit"
    return None
