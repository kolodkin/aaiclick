"""Consolidated AggregatingMergeTree table — merges KEV and Shodan sources."""

from aaiclick import literal
from aaiclick.data.data_context import create_object
from aaiclick.data.models import (
    ENGINE_AGGREGATING_MERGE_TREE,
    FIELDTYPE_ARRAY,
    GB_ANY,
    GB_GROUP_ARRAY_DISTINCT,
    ColumnInfo,
    Schema,
)
from aaiclick.data.object import Object
from aaiclick.orchestration import task

_HAS_KEV = "has(sources, 'kev')"
_HAS_SHODAN = "has(sources, 'shodan')"
_HAS_EPSS = "has(sources, 'epss')"

CONSOLIDATED_COLUMNS = {
    "aai_id": ColumnInfo("UInt64"),
    "cve_id": ColumnInfo("String"),
    "source": ColumnInfo("String"),
    "is_kev": ColumnInfo("Bool", nullable=True),
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
    "sources": ColumnInfo("String", array=True, description="Contributing feeds, e.g. ['kev','shodan','epss']"),
    "is_kev": ColumnInfo("Bool", nullable=True, description="True if CVE is in the CISA KEV catalog (from source data)"),
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
    epss: Object,
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

    # KEV: rename camelCase → snake_case, add source tag
    # All KEV records are KEV by definition → is_kev = true
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
        .with_columns({
            "source": literal("kev", "String"),
            "is_kev": literal(True, "Bool"),
        })
    )
    await agg.insert(kev_view)

    # Shodan: already snake_case, rename kev → is_kev, add source tag
    shodan_view = (
        cves
        .rename({"kev": "is_kev"})
        .with_columns({"source": literal("shodan", "String")})
    )
    await agg.insert(shodan_view)

    # EPSS: rename cve→cve_id, percentile→ranking_epss, add source tag
    epss_view = (
        epss
        .rename({"cve": "cve_id", "percentile": "ranking_epss"})
        .with_columns({"source": literal("epss", "String")})
    )
    await agg.insert(epss_view)

    # Collapse: merge rows per CVE — groupArrayDistinct for sources, any() for all other columns
    # is_kev comes from source data: true for KEV records, Shodan's kev field for Shodan, NULL for EPSS
    merged = await agg.group_by("cve_id").agg({
        "source":             GB_GROUP_ARRAY_DISTINCT,
        "is_kev":             GB_ANY,
        "vendor":             GB_ANY,
        "product":            GB_ANY,
        "vulnerability_name": GB_ANY,
        "short_description":  GB_ANY,
        "date_added":         GB_ANY,
        "known_ransomware":   GB_ANY,
        "cvss":               GB_ANY,
        "cvss_v2":            GB_ANY,
        "cvss_v3":            GB_ANY,
        "epss":               GB_ANY,
        "ranking_epss":       GB_ANY,
        "summary":            GB_ANY,
    })
    return merged.rename({"source": "sources"})


@task
async def analyze_consolidated(consolidated: Object) -> dict:
    """Analyze the consolidated table for cross-source coverage stats."""
    stats = await consolidated.count_if({
        "total_unique_cves":  "1",
        "in_both_sources":    f"{_HAS_KEV} AND {_HAS_SHODAN}",
        "kev_only":           f"{_HAS_KEV} AND NOT {_HAS_SHODAN}",
        "shodan_only":        f"{_HAS_SHODAN} AND NOT {_HAS_KEV}",
        "kev_with_high_epss": f"{_HAS_KEV} AND epss > 0.5",
        "epss_coverage":      _HAS_EPSS,
        "kev_with_epss":      f"{_HAS_KEV} AND {_HAS_EPSS}",
    })
    return await stats.data()
