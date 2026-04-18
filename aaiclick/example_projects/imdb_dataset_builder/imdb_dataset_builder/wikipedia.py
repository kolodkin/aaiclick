"""Wikipedia plot enrichment for the IMDb dataset builder.

Two-stage enrichment keyed on authoritative identifiers (no lossy string
matching on film titles):

1. Resolve ``tconst`` (IMDb) → ``wp_title`` (Wikipedia article) via Wikidata
   SPARQL ``P345`` (IMDb ID) plus ``schema:isPartOf <en.wikipedia.org>``.
   Returns a mapping Object.

2. Load the English Wikipedia article dump from Hugging Face as Parquet.
   All shards fetched in parallel via ClickHouse ``url()`` brace expansion.

3. Build enrichment via two cascaded AggregatingMergeTree tables — avoids the
   need for an ``Object.join()`` method while remaining fully within the
   aaiclick public API:
      • Stage A — key ``tconst``: clean ⊕ sparql_mapping → imdb_with_wp_title
      • Stage B — key ``wp_title``: imdb_with_wp_title ⊕ wiki_dump → enriched

4. Heuristically extract the Plot section (or a lead fallback) from the
   plaintext article via ``extract()`` regexes evaluated inside ClickHouse.

5. Measure coverage with a single ``count_if`` pass.
"""

import asyncio
import json
import os
import urllib.parse
import urllib.request

from aaiclick import create_object_from_url, create_object_from_value, literal
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

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
SPARQL_BATCH = int(os.environ.get("IMDB_SPARQL_BATCH", "400"))
SPARQL_UA = "aaiclick-imdb-dataset-builder/0.1 (https://github.com/kolodkin/aaiclick)"

HF_WIKIPEDIA_SNAPSHOT = os.environ.get("IMDB_WIKI_SNAPSHOT", "20231101")
HF_WIKIPEDIA_SHARDS = int(os.environ.get("IMDB_WIKI_SHARDS", "41"))
HF_WIKIPEDIA_URL_TEMPLATE = (
    "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/"
    "{snapshot}.en/train-{{00000..{last:05d}}}-of-{total:05d}.parquet"
)

# Agg-table column schemas — keep tight so insert() auto-NULLs missing fields.
_STAGE_A_COLUMNS = {
    "aai_id": ColumnInfo("UInt64"),
    "tconst": ColumnInfo("String"),
    "primaryTitle": ColumnInfo("String", nullable=True),
    "startYear": ColumnInfo("String", nullable=True),
    "genres": ColumnInfo("String", nullable=True),
    "runtimeMinutes": ColumnInfo("String", nullable=True),
    "wp_title": ColumnInfo("String", nullable=True),
}

_STAGE_B_COLUMNS = {
    "aai_id": ColumnInfo("UInt64"),
    "wp_title": ColumnInfo("String"),
    "tconst": ColumnInfo("String", nullable=True),
    "primaryTitle": ColumnInfo("String", nullable=True),
    "startYear": ColumnInfo("String", nullable=True),
    "genres": ColumnInfo("String", nullable=True),
    "runtimeMinutes": ColumnInfo("String", nullable=True),
    "wiki_text": ColumnInfo("String", nullable=True),
}

ENRICHED_DISPLAY_COLUMNS = [
    "tconst",
    "primaryTitle",
    "startYear",
    "wp_title",
    "plot",
]


def _sparql_query(tconst_batch: list[str]) -> str:
    """Build a SPARQL query resolving a batch of IMDb IDs to Wikipedia titles."""
    values = " ".join(f"'{t}'" for t in tconst_batch)
    return f"""SELECT ?imdb ?wp_title WHERE {{
  VALUES ?imdb {{ {values} }}
  ?film wdt:P345 ?imdb .
  ?article schema:about ?film ;
           schema:isPartOf <https://en.wikipedia.org/> .
  BIND(REPLACE(REPLACE(STR(?article),
    '^https://en.wikipedia.org/wiki/', ''), '_', ' ') AS ?wp_title)
}}"""


def _sparql_post(query: str) -> list[tuple[str, str]]:
    """POST one SPARQL query, return list of (tconst, wp_title) tuples.

    Uses ``urllib`` (sync) to match the pattern in ``cyber_threat_feeds/epss.py``;
    called via ``asyncio.to_thread`` from the async task below.
    """
    data = urllib.parse.urlencode({"query": query}).encode("utf-8")
    req = urllib.request.Request(
        WIKIDATA_SPARQL_URL,
        data=data,
        method="POST",
        headers={
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": SPARQL_UA,
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    out: list[tuple[str, str]] = []
    for b in body["results"]["bindings"]:
        imdb = b["imdb"]["value"]
        wp = urllib.parse.unquote(b["wp_title"]["value"])
        out.append((imdb, wp))
    return out


@task
async def resolve_wikipedia_titles(clean: Object) -> Object:
    """Resolve ``tconst`` → ``wp_title`` via batched Wikidata SPARQL.

    Returns an Object with columns ``(tconst, wp_title)`` for the subset of
    IMDb titles that have a Wikidata P345 triple linked to an English
    Wikipedia article. The non-resolved subset is simply absent — coverage
    is reported downstream by ``measure_enrichment``.
    """
    data = await clean[["tconst"]].data()
    tconsts = data["tconst"] if isinstance(data, dict) else [row[0] for row in data]
    tconsts = list(dict.fromkeys(tconsts))  # dedupe, preserve order

    resolved: list[tuple[str, str]] = []
    for i in range(0, len(tconsts), SPARQL_BATCH):
        chunk = tconsts[i : i + SPARQL_BATCH]
        query = _sparql_query(chunk)
        pairs = await asyncio.to_thread(_sparql_post, query)
        resolved.extend(pairs)

    if not resolved:
        return await create_object_from_value({"tconst": [], "wp_title": []})

    # Deduplicate: a tconst may match multiple Wikipedia articles across language
    # sitelinks; we already filtered to en.wikipedia but keep the first hit per
    # tconst to avoid row duplication in the agg table.
    seen: dict[str, str] = {}
    for tid, wp in resolved:
        seen.setdefault(tid, wp)

    return await create_object_from_value(
        {
            "tconst": list(seen.keys()),
            "wp_title": list(seen.values()),
        }
    )


@task
async def load_wikipedia_dump(title_map: Object) -> Object:
    """Load the English Wikipedia article dump from Hugging Face as Parquet,
    pre-filtered to the set of titles resolved in ``title_map``.

    Filtering happens INSIDE ClickHouse's ``INSERT … SELECT … WHERE title IN (…)``,
    so the full ~20 GB dump is streamed but only the matched ~tens of thousands
    of rows are written to disk — crucial on constrained CI runners.

    ClickHouse's native ``url()`` function expands ``{0..40}`` brace patterns
    into parallel HTTP reads, so all shards stream concurrently.

    Hugging Face 302-redirects ``/resolve/main/...`` URLs to a CDN host;
    ClickHouse's default ``max_http_get_redirects=0`` rejects any redirect,
    so we raise it explicitly.
    """
    last = HF_WIKIPEDIA_SHARDS - 1
    total = HF_WIKIPEDIA_SHARDS
    url = HF_WIKIPEDIA_URL_TEMPLATE.format(
        snapshot=HF_WIKIPEDIA_SNAPSHOT,
        last=last,
        total=total,
    )
    return await create_object_from_url(
        url=url,
        columns=["id", "url", "title", "text"],
        format="Parquet",
        column_types={
            "id": ColumnInfo("String"),
            "url": ColumnInfo("String"),
            "title": ColumnInfo("String"),
            "text": ColumnInfo("String"),
        },
        ch_settings={"max_http_get_redirects": 10},
        where=f"title IN (SELECT wp_title FROM {title_map.table})",
    )


@task
async def enrich_with_wikipedia(
    clean: Object,
    title_map: Object,
    wiki: Object,
) -> Object:
    """Build enriched (tconst, primaryTitle, startYear, genres, runtimeMinutes,
    wp_title, wiki_text) via two cascaded AggregatingMergeTree tables.

    No ``.join()`` method is required — each AggregatingMergeTree collapses
    rows from two sources keyed on a shared column, with the non-contributing
    source's columns auto-filled NULL, then ``group_by().agg(any)`` merges
    them into one row per key.
    """
    # --------------------- Stage A: tconst ⊕ wp_title -----------------------
    stage_a = await create_object(
        Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns=_STAGE_A_COLUMNS,
            engine=ENGINE_AGGREGATING_MERGE_TREE,
            order_by="tconst",
        )
    )
    await stage_a.insert(clean)  # wp_title auto-NULL
    await stage_a.insert(title_map)  # primaryTitle/startYear/... auto-NULL

    imdb_with_wp_title = await stage_a.group_by("tconst").agg(
        {
            "primaryTitle": GB_ANY,
            "startYear": GB_ANY,
            "genres": GB_ANY,
            "runtimeMinutes": GB_ANY,
            "wp_title": GB_ANY,
        }
    )
    # Keep only titles that both exist in IMDb clean AND resolved to a Wikipedia article.
    matched = imdb_with_wp_title.where("wp_title IS NOT NULL AND primaryTitle IS NOT NULL")

    # --------------------- Stage B: wp_title ⊕ wiki_text --------------------
    stage_b = await create_object(
        Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns=_STAGE_B_COLUMNS,
            engine=ENGINE_AGGREGATING_MERGE_TREE,
            order_by="wp_title",
        )
    )
    await stage_b.insert(matched)  # wiki_text auto-NULL
    # Rename wiki side so titles align; ``insert`` silently drops the
    # extra id/url columns that aren't in the target schema (same idiom
    # used by cyber_threat_feeds/consolidated.py).
    wiki_view = wiki.rename({"title": "wp_title", "text": "wiki_text"})
    await stage_b.insert(wiki_view)  # IMDb columns auto-NULL

    enriched = await stage_b.group_by("wp_title").agg(
        {
            "tconst": GB_ANY,
            "primaryTitle": GB_ANY,
            "startYear": GB_ANY,
            "genres": GB_ANY,
            "runtimeMinutes": GB_ANY,
            "wiki_text": GB_ANY,
        }
    )
    # Keep only rows where both sides contributed.
    return await enriched.where("tconst IS NOT NULL AND wiki_text IS NOT NULL").copy()


@task
async def extract_plot_text(enriched: Object) -> Object:
    """Heuristically isolate the Plot section from each article's plaintext.

    The HF ``wikimedia/wikipedia`` dump stores articles as cleaned plaintext —
    section headings are bare lines (``"Plot\\n"``), templates/refs stripped.
    Strategy:
      1. Extract ``Plot|Synopsis|Premise|Story`` section via regex up to the
         next heading-shaped line (short line starting with a capital).
      2. Always expose ``lead`` = first 2000 chars as a universal fallback.
      3. ``plot`` column coalesces: plot section if found, else lead.
    """
    # ClickHouse extract() returns the first capture group; empty if no match.
    plot_regex = (
        r"(?:^|\n)(?:Plot|Synopsis|Premise|Story)\s*\n+"
        r"([\s\S]{50,6000}?)"
        r"(?:\n[A-Z][A-Za-z][A-Za-z ]{1,30}\n|$)"
    )
    with_plot = enriched.with_columns(
        {
            "plot_raw": Computed("String", f"extract(wiki_text, {_sql_str(plot_regex)})"),
            "lead": Computed("String", "substring(wiki_text, 1, 2000)"),
        }
    )
    # Strip a few residual wiki artefacts still present in the HF plaintext
    # (orphan brackets, double-space collapse).
    with_clean = with_plot.with_columns(
        {
            "plot": Computed(
                "String",
                "if(length(plot_raw) >= 120, "
                "replaceRegexpAll(plot_raw, '[\\\\[\\\\]]+', ''), "
                "replaceRegexpAll(lead, '[\\\\[\\\\]]+', ''))",
            ),
        }
    )
    result = with_clean[
        [
            "tconst",
            "primaryTitle",
            "startYear",
            "genres",
            "runtimeMinutes",
            "wp_title",
            "plot",
        ]
    ]
    return await result.copy()


@task
async def measure_enrichment(
    clean: Object,
    title_map: Object,
    plots: Object,
) -> EnrichmentStats:
    """Compute coverage stats for the Wikipedia enrichment stages."""
    total_obj = await clean["tconst"].count()
    total_clean = await total_obj.data()

    resolved_obj = await title_map["tconst"].count()
    titles_resolved = await resolved_obj.data()

    matched_obj = await plots["tconst"].count()
    articles_matched = await matched_obj.data()

    plot_stats_obj = await plots.count_if(
        {
            "usable": "length(plot) >= 120",
        }
    )
    plot_stats = await plot_stats_obj.data()
    plots_usable = plot_stats["usable"]

    avg_obj = plots.with_columns({"plot_len": Computed("UInt32", "length(plot)")})
    avg = await (await avg_obj["plot_len"].mean()).data()

    def pct(n: int) -> float:
        return (n / total_clean * 100) if total_clean > 0 else 0.0

    return EnrichmentStats(
        total_clean=total_clean,
        titles_resolved=titles_resolved,
        titles_resolved_pct=pct(titles_resolved),
        articles_matched=articles_matched,
        articles_matched_pct=pct(articles_matched),
        plots_usable=plots_usable,
        plots_usable_pct=pct(plots_usable),
        avg_plot_chars=float(avg or 0.0),
    )


def _sql_str(s: str) -> str:
    """Escape a Python string as a single-quoted ClickHouse SQL literal."""
    escaped = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
