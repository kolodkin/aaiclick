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
    pre_1980: int
    pre_1980_pct: float


class HFPublishResult(BaseModel):
    status: str
    repo: str
    reason: str | None = None
    rows: int | None = None


class EnrichmentStats(BaseModel):
    total_clean: int
    titles_resolved: int
    titles_resolved_pct: float
    articles_matched: int
    articles_matched_pct: float
    plots_usable: int
    plots_usable_pct: float
    avg_plot_chars: float
