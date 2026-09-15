"""Typed error hierarchy raised by ``internal_api`` functions.

Each surface maps these to its own error shape:

- CLI renderer: non-zero exit code + human message.
- FastAPI: ``NotFound`` → 404, ``Conflict`` → 409, ``Invalid`` → 422,
  ``Unauthorized`` → 401, ``Forbidden`` → 403, ``ExecutionWorkerSpawnFailed`` → 503
  (see ``server/errors.py``).
- FastMCP: tool error.

Auth errors (``Unauthorized`` / ``Forbidden``) are raised only by the HTTP
transport layer (``server/auth.py``); the CLI and in-process MCP client
bypass the bearer check entirely.
"""

from __future__ import annotations


class InternalApiError(Exception):
    """Base class for ``internal_api`` failures."""


class NotFound(InternalApiError):
    """The referenced entity does not exist."""


class Conflict(InternalApiError):
    """State-transition violation (e.g. cancelling a finished job)."""


class ExecutionWorkerSpawnFailed(Conflict):
    """Spawning a detached worker subprocess failed (missing binary, etc.).

    A ``Conflict`` subclass so ``except Conflict`` still catches it, but
    mapped to its own HTTP status (503) and ``ProblemCode`` by the server's
    MRO-aware exception handler lookup.
    """


class Invalid(InternalApiError):
    """Request or filter validation failed."""


class Unauthorized(InternalApiError):
    """Missing or invalid bearer token (HTTP transport only)."""


class Forbidden(InternalApiError):
    """Authenticated but insufficient role — raised by ``require_admin`` when a
    viewer hits a mutating route or the admin-only ``/users`` and ``/mcp`` surfaces."""


class MfaRequired(Unauthorized):
    """Password accepted but the account has MFA enabled and no (valid) TOTP
    code was supplied. An ``Unauthorized`` subclass with its own
    ``ProblemCode`` so a client can prompt for the code and retry."""
