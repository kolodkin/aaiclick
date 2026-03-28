"""Pydantic models for IMDb dataset builder task results."""

from pydantic import BaseModel


class RawProfile(BaseModel):
    total_titles: int
    by_type: dict[str, int]
    adult_count: int
    adult_pct: float


class QualityIssues(BaseModel):
    total_movies: int
    missing_runtime: int
    missing_runtime_pct: float
    short_runtime: int
    long_runtime: int
    pre_1970: int
    pre_1970_pct: float


class HFPublishResult(BaseModel):
    status: str
    repo: str
    reason: str | None = None
    rows: int | None = None
