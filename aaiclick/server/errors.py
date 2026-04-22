"""Map ``internal_api.errors.*`` to RFC 7807 ``Problem`` + HTTP status."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from aaiclick.internal_api.errors import Conflict, InternalApiError, Invalid, NotFound
from aaiclick.view_models import Problem, ProblemCode

_PROBLEM_MAP: dict[type[InternalApiError], tuple[str, int, ProblemCode]] = {
    NotFound: ("Not Found", 404, ProblemCode.NOT_FOUND),
    Conflict: ("Conflict", 409, ProblemCode.CONFLICT),
    Invalid: ("Invalid Request", 422, ProblemCode.INVALID),
}


def register_exception_handlers(app: FastAPI) -> None:
    for exc_type, (title, status, code) in _PROBLEM_MAP.items():
        _register(app, exc_type, title, status, code)


def _register(
    app: FastAPI,
    exc_type: type[InternalApiError],
    title: str,
    status: int,
    code: ProblemCode,
) -> None:
    @app.exception_handler(exc_type)
    async def _handler(request: Request, exc: InternalApiError) -> JSONResponse:
        return JSONResponse(
            status_code=status,
            content=Problem(title=title, status=status, detail=str(exc), code=code).model_dump(mode="json"),
        )
