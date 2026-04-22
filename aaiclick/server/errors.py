"""Map ``internal_api.errors.*`` to RFC 7807 ``Problem`` + HTTP status.

Unhandled ``InternalApiError`` subclasses fall through to FastAPI's default
500 handler — we do not install a blanket catch-all, so bugs surface instead
of being silently wrapped.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from aaiclick.internal_api.errors import Conflict, Invalid, NotFound
from aaiclick.view_models import Problem


def _problem_response(title: str, status: int, detail: str, code: str) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content=Problem(title=title, status=status, detail=detail, code=code).model_dump(),
    )


def register_exception_handlers(app: FastAPI) -> None:
    """Install the three ``internal_api`` → HTTP Problem handlers on ``app``."""

    @app.exception_handler(NotFound)
    async def _handle_not_found(request: Request, exc: NotFound) -> JSONResponse:
        return _problem_response("Not Found", 404, str(exc), "not_found")

    @app.exception_handler(Conflict)
    async def _handle_conflict(request: Request, exc: Conflict) -> JSONResponse:
        return _problem_response("Conflict", 409, str(exc), "conflict")

    @app.exception_handler(Invalid)
    async def _handle_invalid(request: Request, exc: Invalid) -> JSONResponse:
        return _problem_response("Invalid Request", 422, str(exc), "invalid")
