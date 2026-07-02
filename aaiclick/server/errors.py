"""Map ``internal_api.errors.*`` to RFC 7807 ``Problem`` + HTTP status."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from aaiclick.internal_api.errors import (
    Conflict,
    ExecutionWorkerSpawnFailed,
    Forbidden,
    InternalApiError,
    Invalid,
    NotFound,
    Unauthorized,
)
from aaiclick.view_models import Problem, ProblemCode

_PROBLEM_MAP: dict[type[InternalApiError], tuple[str, int, ProblemCode]] = {
    NotFound: ("Not Found", 404, ProblemCode.NOT_FOUND),
    Conflict: ("Conflict", 409, ProblemCode.CONFLICT),
    ExecutionWorkerSpawnFailed: ("ExecutionWorker Spawn Failed", 503, ProblemCode.EXECUTION_WORKER_SPAWN_FAILED),
    Invalid: ("Invalid Request", 422, ProblemCode.INVALID),
    Unauthorized: ("Unauthorized", 401, ProblemCode.UNAUTHORIZED),
    Forbidden: ("Forbidden", 403, ProblemCode.FORBIDDEN),
}

# Extra response headers per error type. 401s must advertise the scheme so
# clients know to retry with a bearer token (RFC 7235).
_HEADERS: dict[type[InternalApiError], dict[str, str]] = {
    Unauthorized: {"WWW-Authenticate": "Bearer"},
}


def register_exception_handlers(app: FastAPI) -> None:
    # Takes `app` as a parameter — `app.py` imports this module, so `from .app import app` would cycle.
    for exc_type, (title, status, code) in _PROBLEM_MAP.items():
        _register(app, exc_type, title, status, code, _HEADERS.get(exc_type))


def problem_response(
    title: str,
    status: int,
    detail: str | None,
    code: ProblemCode,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """Build an RFC 7807 ``Problem`` JSON response.

    Shared by the registered exception handlers and the ``/mcp`` auth
    middleware — the middleware can't reach the app's handlers across the
    mount boundary, so it calls this directly to emit an identical envelope.
    """
    return JSONResponse(
        status_code=status,
        content=Problem(title=title, status=status, detail=detail, code=code).model_dump(mode="json"),
        headers=headers,
    )


def problem_responses(*codes: int) -> dict[int | str, dict[str, Any]]:
    """OpenAPI ``responses=`` mapping for the ``Problem`` codes a route can emit.

    Codes must appear in ``_PROBLEM_MAP`` — single source of truth with the
    runtime exception handlers.
    """
    titles = {status: title for title, status, _ in _PROBLEM_MAP.values()}
    return {code: {"model": Problem, "description": titles[code]} for code in codes}


def _register(
    app: FastAPI,
    exc_type: type[InternalApiError],
    title: str,
    status: int,
    code: ProblemCode,
    headers: dict[str, str] | None = None,
) -> None:
    @app.exception_handler(exc_type)
    async def _handler(request: Request, exc: InternalApiError) -> JSONResponse:
        return problem_response(title, status, str(exc), code, headers)
