"""TMDB plot enrichment for the IMDb dataset builder.

A static Hugging Face Parquet dump that already cross-references IMDb's
``tconst``, eliminating the SPARQL → Wikipedia-dump → regex-extract chain
the project used originally. Two tasks:

1. ``load_tmdb_dump`` — fetch the single Parquet shard via ClickHouse
   ``url()``, pre-filtered at insert time to ``tconst`` IDs already in
   ``clean`` (so we materialize only ~10s of MB locally regardless of
   how big the upstream file gets).

2. ``enrich_with_tmdb`` — single-stage ``AggregatingMergeTree`` merge on
   ``tconst``: insert ``clean`` (TMDB columns auto-NULL) + insert ``tmdb``
   (IMDb columns auto-NULL), then ``group_by(tconst).agg(any)``. Emits the
   ``plot`` field (renamed from TMDB's ``overview``) so downstream consumers
   keep the same column name they used with the Wikipedia plot text.

Coverage: ~90% of clean IMDb rows pick up a non-null overview from TMDB,
typical length 200-400 chars (clean editorial single-paragraph synopses,
no Wikipedia-style templates or refs).
"""

import os

from aaiclick import create_object_from_url
from aaiclick.data.data_context import create_object
from aaiclick.data.models import (
    ENGINE_AGGREGATING_MERGE_TREE,
    FIELDTYPE_ARRAY,
    GB_ANY,
    ColumnInfo,
    Computed,
    Schema,
)
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .models import EnrichmentStats

TMDB_URL = os.environ.get(
    "IMDB_TMDB_URL",
    "https://huggingface.co/api/datasets/HenryWaltson/TMDB-IMDB-Movies-Dataset/parquet/default/train/0.parquet",
)

# AggregatingMergeTree stage table — both sources merge here keyed on tconst.
_STAGE_COLUMNS = {
    "tconst": ColumnInfo("String"),
    "primaryTitle": ColumnInfo("String", nullable=True),
    "startYear": ColumnInfo("String", nullable=True),
    "genres": ColumnInfo("Array(String)"),
    "runtimeMinutes": ColumnInfo("String", nullable=True),
    "overview": ColumnInfo("String", nullable=True),
}


@task
async def load_tmdb_dump(clean: Object) -> Object:
    """Fetch the TMDB-IMDB Parquet, pre-filtered to titles in ``clean``.

    ClickHouse's ``url()`` table function streams the upstream file but
    only writes rows matching ``tconst IN clean`` to the local table —
    on a ~190 MB upstream file we end up with a few MB locally for the
    demo dataset. Hugging Face 302-redirects ``/api/.../parquet/...``
    URLs to a CDN host, so ``max_http_get_redirects`` is raised.
    """
    return await create_object_from_url(
        url=TMDB_URL,
        columns=["tconst", "title", "overview"],
        format="Parquet",
        column_types={
            "tconst": ColumnInfo("String"),
            "title": ColumnInfo("String", nullable=True),
            "overview": ColumnInfo("String", nullable=True),
        },
        ch_settings={"max_http_get_redirects": 10},
        where=f"tconst IN (SELECT tconst FROM {clean.table})",
    )


@task
async def enrich_with_tmdb(clean: Object, tmdb: Object) -> Object:
    """Merge clean IMDb subset with TMDB overviews via AggregatingMergeTree.

    Single-stage merge (vs. the old 2-stage Wikipedia chain): both sources
    share the ``tconst`` key, so one insert pair + one ``group_by/agg(any)``
    produces the enriched output. Adding a third source (TMDB ratings,
    cast, …) is a one-line extra ``insert()`` — the showcased property
    of ``AggregatingMergeTree`` over chained joins.

    Returns ``(tconst, primaryTitle, startYear, genres, runtimeMinutes, plot)``
    where ``plot`` is renamed from TMDB's ``overview`` via a Computed
    alias. Rows without a TMDB overview (no plot to show) are filtered out.
    """
    stage = await create_object(
        Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns=_STAGE_COLUMNS,
            engine=ENGINE_AGGREGATING_MERGE_TREE,
            order_by="tconst",
        )
    )
    await stage.insert(clean)  # overview auto-NULL
    # Materialize a tmdb projection to (tconst, overview): the upstream
    # Parquet has its own String `genres` column that would collide
    # with the stage's Array(String) `genres` slot during tolerant
    # column-name matching. A subscript view alone isn't enough — the
    # underlying schema still carries genres; .copy() rewrites the
    # schema to just the projected pair.
    tmdb_projected = await tmdb[["tconst", "overview"]].copy()
    await stage.insert(tmdb_projected)

    enriched = await stage.group_by("tconst").agg(
        {
            "primaryTitle": GB_ANY,
            "startYear": GB_ANY,
            "genres": GB_ANY,
            "runtimeMinutes": GB_ANY,
            "overview": GB_ANY,
        }
    )
    matched = enriched.where("primaryTitle IS NOT NULL AND overview IS NOT NULL")

    # Rename overview → plot via Computed alias + projection so the public
    # column stays "plot" (Airtable spec, HF parquet, and report all use it).
    materialized = await matched.copy()
    aliased = materialized.with_columns({"plot": Computed("String", "overview")})
    final = aliased[
        [
            "tconst",
            "primaryTitle",
            "startYear",
            "genres",
            "runtimeMinutes",
            "plot",
        ]
    ]
    return await final.copy()


@task
async def measure_enrichment(clean: Object, plots: Object) -> EnrichmentStats:
    """Compute coverage stats for the TMDB enrichment."""
    total_clean = await (await clean["tconst"].count()).data()
    matched = await (await plots["tconst"].count()).data()

    plot_stats = await (await plots.count_if({"usable": "length(plot) >= 120"})).data()
    plots_usable = plot_stats["usable"]

    avg_obj = plots.with_columns({"plot_len": Computed("UInt32", "length(plot)")})
    avg = await (await avg_obj["plot_len"].mean()).data()

    def pct(n: int) -> float:
        return (n / total_clean * 100) if total_clean > 0 else 0.0

    return EnrichmentStats(
        total_clean=total_clean,
        matched=matched,
        matched_pct=pct(matched),
        plots_usable=plots_usable,
        plots_usable_pct=pct(plots_usable),
        avg_plot_chars=float(avg or 0.0),
    )
