"""Consolidated AggregatingMergeTree table — merges KEV and Shodan sources."""

from aaiclick.data.data_context import create_object, get_ch_client
from aaiclick.data.models import (
    ENGINE_AGGREGATING_MERGE_TREE,
    FIELDTYPE_ARRAY,
    ColumnInfo,
    Computed,
    Schema,
)
from aaiclick.data.object import Object
from aaiclick.orchestration import task

_HAS_KEV = "has(sources, 'kev')"
_HAS_SHODAN = "has(sources, 'shodan')"

CONSOLIDATED_COLUMNS = {
    "aai_id": ColumnInfo("UInt64"),
    "cve_id": ColumnInfo("String"),
    "source": ColumnInfo("String"),
    "vendor": ColumnInfo("String", nullable=True),
    "product": ColumnInfo("String", nullable=True),
    "vulnerability_name": ColumnInfo("String", nullable=True),
    "short_description": ColumnInfo("String", nullable=True),
    "date_added": ColumnInfo("Date", nullable=True),
    "known_ransomware": ColumnInfo("String", nullable=True),
    "cvss": ColumnInfo("Float64", nullable=True),
    "cvss_v2": ColumnInfo("Float64", nullable=True),
    "cvss_v3": ColumnInfo("Float64", nullable=True),
    "epss": ColumnInfo("Float64", nullable=True),
    "ranking_epss": ColumnInfo("Float64", nullable=True),
    "summary": ColumnInfo("String", nullable=True),
}

MERGED_COLUMNS = {
    "aai_id": ColumnInfo("UInt64"),
    "cve_id": ColumnInfo("String", description="CVE identifier (GROUP BY key)"),
    "sources": ColumnInfo("String", array=True, description="Contributing feeds, e.g. ['kev','shodan']"),
    "vendor": ColumnInfo("String", nullable=True, description="Vendor name (any() aggregated)"),
    "product": ColumnInfo("String", nullable=True, description="Product name (any() aggregated)"),
    "vulnerability_name": ColumnInfo("String", nullable=True, description="Vulnerability title from KEV"),
    "short_description": ColumnInfo("String", nullable=True, description="Brief description from KEV"),
    "date_added": ColumnInfo("Date", nullable=True, description="Date added to KEV catalog"),
    "known_ransomware": ColumnInfo("String", nullable=True, description="Ransomware campaign linkage"),
    "cvss": ColumnInfo("Float64", nullable=True, description="Combined CVSS score"),
    "cvss_v2": ColumnInfo("Float64", nullable=True, description="CVSS v2.0 base score"),
    "cvss_v3": ColumnInfo("Float64", nullable=True, description="CVSS v3.x base score"),
    "epss": ColumnInfo("Float64", nullable=True, description="EPSS exploitation probability 0-1"),
    "ranking_epss": ColumnInfo("Float64", nullable=True, description="EPSS percentile ranking 0-1"),
    "summary": ColumnInfo("String", nullable=True, description="Vulnerability description from Shodan"),
}


@task
async def build_consolidated_table(
    kev: Object,
    cves: Object,
    start_date: str,
    end_date: str,
) -> Object:
    """Build a consolidated AggregatingMergeTree table from all sources.

    Uses rename() + with_columns() to map each source's columns to the
    consolidated schema, then insert() to load them into a shared
    AggregatingMergeTree table keyed by cve_id. Extra source columns
    are silently skipped by insert(). Finally collapses via GROUP BY
    with groupArrayDistinct(source) to produce one row per CVE with
    an Array(String) 'sources' column tracking which feeds contributed.
    """
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns=CONSOLIDATED_COLUMNS,
        engine=ENGINE_AGGREGATING_MERGE_TREE,
        order_by="cve_id",
    )
    agg = await create_object(schema)

    # KEV: rename camelCase → snake_case, filter to date window, add source tag
    kev_view = (
        kev
        .rename({
            "cveID": "cve_id",
            "vendorProject": "vendor",
            "vulnerabilityName": "vulnerability_name",
            "shortDescription": "short_description",
            "dateAdded": "date_added",
            "knownRansomwareCampaignUse": "known_ransomware",
        })
        .with_columns({"source": Computed("String", "'kev'")})
        .where(f"dateAdded >= '{start_date}' AND dateAdded < '{end_date}'")
    )
    await agg.insert(kev_view)

    # Shodan: already snake_case, just add source tag
    shodan_view = cves.with_columns({
        "source": Computed("String", "'shodan'"),
    })
    await agg.insert(shodan_view)

    # Collapse: merge rows per CVE with groupArrayDistinct for sources
    # TODO: replace with agg API once groupArrayDistinct is supported
    ch = get_ch_client()
    merged_schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns=MERGED_COLUMNS,
    )
    merged = await create_object(merged_schema)
    await ch.command(
        f"INSERT INTO {merged.table} "
        f"(cve_id, sources, vendor, product, vulnerability_name, "
        f"short_description, date_added, known_ransomware, "
        f"cvss, cvss_v2, cvss_v3, epss, ranking_epss, summary) "
        f"SELECT cve_id, groupArrayDistinct(source), "
        f"any(vendor), any(product), any(vulnerability_name), "
        f"any(short_description), any(date_added), any(known_ransomware), "
        f"any(cvss), any(cvss_v2), any(cvss_v3), "
        f"any(epss), any(ranking_epss), any(summary) "
        f"FROM {agg.table} GROUP BY cve_id"
    )

    return merged


@task
async def analyze_consolidated(consolidated: Object) -> dict:
    """Analyze the consolidated table for cross-source coverage stats."""
    stats = await consolidated.count_if({
        "total_unique_cves":  "1",
        "in_both_sources":    f"{_HAS_KEV} AND {_HAS_SHODAN}",
        "kev_only":           f"{_HAS_KEV} AND NOT {_HAS_SHODAN}",
        "shodan_only":        f"{_HAS_SHODAN} AND NOT {_HAS_KEV}",
        "kev_with_high_epss": f"{_HAS_KEV} AND epss > 0.5",
    })
    return await stats.data()
