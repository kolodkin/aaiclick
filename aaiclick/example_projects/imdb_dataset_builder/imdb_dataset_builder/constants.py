"""Shared constants for the IMDb Dataset Builder pipeline."""

import os

from aaiclick.data.models import ColumnInfo

IMDB_URL = os.environ.get("IMDB_URL", "https://datasets.imdbws.com/title.basics.tsv.gz")

IMDB_COLUMNS = [
    "tconst",
    "titleType",
    "primaryTitle",
    "originalTitle",
    "isAdult",
    "startYear",
    "endYear",
    "runtimeMinutes",
    "genres",
]

IMDB_RAW_COLUMNS: dict[str, ColumnInfo] = {
    "tconst": ColumnInfo(type="String", description="IMDb title identifier (e.g. tt0000001)"),
    "titleType": ColumnInfo(type="String", description="Type of title (movie, short, tvSeries, ...)"),
    "primaryTitle": ColumnInfo(type="String", description="Popular title used for promotion"),
    "originalTitle": ColumnInfo(type="String", description="Original language title"),
    "isAdult": ColumnInfo(type="String", description="'1' for adult content, '0' otherwise"),
    "startYear": ColumnInfo(type="String", description="Release year (or '\\N' if unknown)"),
    "endYear": ColumnInfo(type="String", description="End year for series ('\\N' for movies)"),
    "runtimeMinutes": ColumnInfo(type="String", description="Runtime in minutes ('\\N' if unknown)"),
    "genres": ColumnInfo(type="String", description="Comma-separated genres ('\\N' if unknown)"),
}

CLEAN_COLUMNS: dict[str, ColumnInfo] = {
    "tconst": ColumnInfo(type="String", description="IMDb title identifier (e.g. tt0000001)"),
    "primaryTitle": ColumnInfo(type="String", description="Popular title used for promotion"),
    "startYear": ColumnInfo(type="String", description="Release year (>= 1980)"),
    "genres": ColumnInfo(type="String", description="Comma-separated genres (no Adult)"),
    "runtimeMinutes": ColumnInfo(type="String", description="Runtime in minutes (40-300)"),
}

HF_REPO_ID = os.environ.get("HF_REPO_ID", "aaiclick/imdb-wikipedia-enriched")

WIKIPEDIA_COLUMNS: dict[str, ColumnInfo] = {
    "id": ColumnInfo(type="String", description="Wikipedia page ID"),
    "url": ColumnInfo(type="String", description="Canonical Wikipedia article URL"),
    "title": ColumnInfo(type="String", description="Article title (aligns with Wikidata P345 sitelink)"),
    "text": ColumnInfo(type="String", description="Cleaned plaintext article body"),
}
