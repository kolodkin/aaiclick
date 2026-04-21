"""Typed error hierarchy raised by ``internal_api`` functions.

Each surface maps these to its own error shape:

- CLI renderer: non-zero exit code + human message.
- FastAPI: ``NotFound`` → 404, ``Conflict`` → 409, ``Invalid`` → 422 (see
  ``server/errors.py`` once Phase 3 lands).
- FastMCP: tool error.
"""

from __future__ import annotations


class InternalApiError(Exception):
    """Base class for ``internal_api`` failures."""


class NotFound(InternalApiError):
    """The referenced entity does not exist."""


class Conflict(InternalApiError):
    """State-transition violation (e.g. cancelling a finished job)."""


class Invalid(InternalApiError):
    """Request or filter validation failed."""
