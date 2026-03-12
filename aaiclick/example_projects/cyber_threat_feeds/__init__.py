"""
Cyber Threat Feeds Pipeline - Multi-Source Security Data Example

Demonstrates loading multiple cybersecurity data feeds into ClickHouse
Objects via JSON URL ingestion, consolidating them into an AggregatingMergeTree
table keyed by CVE ID, then analyzing and correlating them:

- CISA KEV (Known Exploited Vulnerabilities) — JSON API
- Shodan CVEDB (CVE Database with EPSS scores) — JSON API

All data flows directly from URLs into ClickHouse — zero Python memory footprint.
ClickHouse fetches each URL, explodes JSON arrays, extracts typed columns,
and performs all analysis via SQL.

Data sources:
- CISA KEV: https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json
- Shodan CVEDB: https://cvedb.shodan.io/cves

Usage:
    # Register job (requires PostgreSQL)
    python -m aaiclick.example_projects.cyber_threat_feeds

    # Then run worker to execute
    python -m aaiclick.orchestration.worker
"""

import asyncio

from aaiclick import create_object_from_url
from aaiclick.data.data_context import create_object, get_ch_client
from aaiclick.data.models import (
    ENGINE_AGGREGATING_MERGE_TREE,
    FIELDTYPE_ARRAY,
    ColumnInfo,
    Computed,
    Schema,
)
from aaiclick.data.object import Object
from aaiclick.orchestration import job, task

# =============================================================================
# Data source URLs
# =============================================================================

CISA_KEV_URL = (
    "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
)

SHODAN_CVEDB_URL = "https://cvedb.shodan.io/cves"

# Shared date window — both sources are filtered to this range so the
# consolidated table has meaningful cross-source overlap.
START_DATE = "2025-01-01"
END_DATE = "2026-01-01"

# =============================================================================
# Column definitions
# =============================================================================

KEV_COLUMNS = {
    "cveID": ColumnInfo("String", description="CVE identifier (e.g. CVE-2024-1234)"),
    "vendorProject": ColumnInfo("String", description="Vendor or project name"),
    "product": ColumnInfo("String", description="Affected product name"),
    "vulnerabilityName": ColumnInfo("String", description="Human-readable vulnerability title"),
    "dateAdded": ColumnInfo("Date", description="Date added to KEV catalog"),
    "shortDescription": ColumnInfo("String", description="Brief vulnerability description"),
    "requiredAction": ColumnInfo("String", description="Required remediation action"),
    "dueDate": ColumnInfo("Date", description="Deadline for required action"),
    "knownRansomwareCampaignUse": ColumnInfo("String", description="'Known' if linked to ransomware, else 'Unknown'"),
    "notes": ColumnInfo("String", nullable=True, description="Additional notes"),
    "cwes": ColumnInfo("String", array=True, description="Associated CWE identifiers"),
}

SHODAN_COLUMNS = {
    "cve_id": ColumnInfo("String", description="CVE identifier (e.g. CVE-2024-1234)"),
    "summary": ColumnInfo("String", description="Vulnerability description text"),
    "cvss": ColumnInfo("Float64", nullable=True, description="Combined CVSS score"),
    "cvss_v2": ColumnInfo("Float64", nullable=True, description="CVSS v2.0 base score"),
    "cvss_v3": ColumnInfo("Float64", nullable=True, description="CVSS v3.x base score"),
    "epss": ColumnInfo("Float64", nullable=True, description="EPSS exploitation probability 0-1"),
    "ranking_epss": ColumnInfo("Float64", nullable=True, description="EPSS percentile ranking 0-1"),
    "kev": ColumnInfo("Bool", description="Whether CVE is in CISA KEV catalog"),
    "published_time": ColumnInfo("String", description="CVE publication datetime (ISO 8601)"),
    "vendor": ColumnInfo("String", nullable=True, description="Vendor name"),
    "product": ColumnInfo("String", nullable=True, description="Product name"),
    "references": ColumnInfo("String", array=True, description="Reference URLs"),
}


# =============================================================================
# Phase 1: CISA KEV tasks
# =============================================================================


@task
async def load_kev_data() -> Object:
    """Load CISA Known Exploited Vulnerabilities catalog via JSON URL ingestion."""
    return await create_object_from_url(
        url=CISA_KEV_URL,
        format="RawBLOB",
        json_path="vulnerabilities",
        json_columns=KEV_COLUMNS,
    )


async def _kev_by_vendor(kev: Object) -> Object:
    """Top vendors by number of known exploited vulnerabilities."""
    return await kev.group_by("vendorProject").agg({
        "cveID": "count",
    })


async def _kev_by_year(kev: Object) -> Object:
    """KEV entries grouped by year added to the catalog."""
    kev_with_year = kev.with_columns({
        "year": Computed("UInt16", "toYear(dateAdded)"),
    })
    return await kev_with_year.group_by("year").agg({"cveID": "count"})


async def _kev_ransomware(kev: Object) -> dict:
    """Count vulnerabilities linked to known ransomware campaigns."""
    total_count = await (await kev["cveID"].count()).data()

    by_ransomware = await kev.group_by("knownRansomwareCampaignUse").agg({
        "cveID": "count",
    })
    ransomware_data = await by_ransomware.data()
    ransomware_count_val = 0
    for i, label in enumerate(ransomware_data["knownRansomwareCampaignUse"]):
        if label == "Known":
            ransomware_count_val = ransomware_data["cveID"][i]
            break
    ransomware_pct = (ransomware_count_val / total_count) * 100 if total_count > 0 else 0.0

    return {
        "total_kev": total_count,
        "ransomware_linked": ransomware_count_val,
        "ransomware_pct": ransomware_pct,
    }


@task
async def analyze_kev(kev: Object) -> dict:
    """Analyze KEV: top vendors, yearly trends, and ransomware linkage."""
    by_vendor = await _kev_by_vendor(kev)
    by_year = await _kev_by_year(kev)
    ransomware = await _kev_ransomware(kev)

    vendor_data = await by_vendor.data()
    vendor_counts = sorted(
        zip(vendor_data["vendorProject"], vendor_data["cveID"]),
        key=lambda x: x[1],
        reverse=True,
    )

    year_data = await by_year.data()
    year_counts = sorted(
        zip(year_data["year"], year_data["cveID"]),
        key=lambda x: x[0],
    )

    report = {
        "kev_summary": {
            "total_vulnerabilities": ransomware["total_kev"],
            "ransomware_linked": ransomware["ransomware_linked"],
            "ransomware_pct": ransomware["ransomware_pct"],
            "top_vendors": {name: count for name, count in vendor_counts[:10]},
            "by_year": {year: count for year, count in year_counts},
        },
    }

    _print_kev_report(report)
    return report


# =============================================================================
# Phase 2: Shodan CVEDB tasks
# =============================================================================


@task
async def load_shodan_kev_cves() -> Object:
    """Load KEV-flagged CVEs from Shodan CVEDB.

    Uses is_kev=true to fetch CVEs that Shodan knows are in the CISA KEV
    catalog, ensuring cross-source overlap in the consolidated table.
    """
    url = (
        f"{SHODAN_CVEDB_URL}"
        f"?is_kev=true&limit=5000"
        f"&start_date={START_DATE}&end_date={END_DATE}"
    )
    return await create_object_from_url(
        url=url,
        format="RawBLOB",
        json_path="cves",
        json_columns=SHODAN_COLUMNS,
    )


@task
async def load_shodan_general_cves(limit: int = 5000) -> Object:
    """Load non-KEV CVEs from Shodan CVEDB.

    Uses is_kev=false to fetch general CVEs that are not in CISA KEV,
    providing broader vulnerability coverage with EPSS scores.
    """
    url = (
        f"{SHODAN_CVEDB_URL}"
        f"?is_kev=false&limit={limit}"
        f"&start_date={START_DATE}&end_date={END_DATE}"
    )
    return await create_object_from_url(
        url=url,
        format="RawBLOB",
        json_path="cves",
        json_columns=SHODAN_COLUMNS,
    )


@task
async def combine_shodan_cves(kev_cves: Object, general_cves: Object) -> Object:
    """Combine KEV-flagged and general Shodan CVEs into a single Object.

    Since is_kev=true and is_kev=false return disjoint sets, no
    deduplication is needed.
    """
    return await kev_cves.concat(general_cves)


async def _cvss_distribution(cves: Object) -> dict:
    """Compute CVSS score distribution statistics."""
    cvss = cves["cvss"]

    avg_cvss = await (await cvss.mean()).data()
    std_cvss = await (await cvss.std()).data()
    median_cvss = await (await cvss.quantile(0.5)).data()
    p90_cvss = await (await cvss.quantile(0.9)).data()
    p99_cvss = await (await cvss.quantile(0.99)).data()
    min_cvss = await (await cvss.min()).data()
    max_cvss = await (await cvss.max()).data()

    # Comparison operators on Nullable(Float64) produce Float64-typed results,
    # so we use arithmetic: mean of (cvss >= threshold) gives the fraction.
    gte9 = await (cvss >= 9.0)
    critical_pct = (await (await gte9.mean()).data()) * 100

    gte7 = await (cvss >= 7.0)
    gte7_pct = (await (await gte7.mean()).data()) * 100
    high_pct = gte7_pct - critical_pct

    return {
        "avg": avg_cvss,
        "std": std_cvss,
        "median": median_cvss,
        "p90": p90_cvss,
        "p99": p99_cvss,
        "min": min_cvss,
        "max": max_cvss,
        "critical_pct": critical_pct,
        "high_pct": high_pct,
    }


async def _epss_distribution(cves: Object) -> dict:
    """Compute EPSS score distribution statistics."""
    epss = cves["epss"]

    avg_epss = await (await epss.mean()).data()
    median_epss = await (await epss.quantile(0.5)).data()
    p90_epss = await (await epss.quantile(0.9)).data()
    p99_epss = await (await epss.quantile(0.99)).data()

    high_prob = await (epss > 0.5)
    high_prob_pct = (await (await high_prob.mean()).data()) * 100

    return {
        "avg": avg_epss,
        "median": median_epss,
        "p90": p90_epss,
        "p99": p99_epss,
        "high_probability_pct": high_prob_pct,
    }


async def _high_risk_cves(cves: Object) -> dict:
    """Find CVEs with both high CVSS (>=9.0) and high EPSS (>0.5).

    Uses multiplication instead of bitAnd to combine boolean-like results,
    since comparison on Nullable(Float64) produces Float64-typed Objects.
    """
    critical_cvss = await (cves["cvss"] >= 9.0)
    high_epss = await (cves["epss"] > 0.5)
    # Multiply two 0/1 results: 1*1=1 (both true), otherwise 0
    high_risk = await (critical_cvss * high_epss)

    high_risk_count = await (await high_risk.sum()).data()
    total_count = await (await cves["cve_id"].count()).data()

    return {
        "high_risk_count": high_risk_count,
        "total_cves": total_count,
        "high_risk_pct": (high_risk_count / total_count) * 100 if total_count > 0 else 0.0,
    }


@task
async def analyze_shodan_cves(cves: Object) -> dict:
    """Analyze Shodan CVEs: CVSS/EPSS distributions and high-risk identification."""
    cvss_stats = await _cvss_distribution(cves)
    epss_stats = await _epss_distribution(cves)
    high_risk = await _high_risk_cves(cves)

    return {
        "cvss_distribution": cvss_stats,
        "epss_distribution": epss_stats,
        "high_risk": high_risk,
    }


# =============================================================================
# Phase 3: Consolidated AggregatingMergeTree table
# =============================================================================

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
async def build_consolidated_table(kev: Object, cves: Object) -> Object:
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
        .where(f"dateAdded >= '{START_DATE}' AND dateAdded < '{END_DATE}'")
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
    total = await (await consolidated["cve_id"].count()).data()

    # Add computed boolean columns for source presence
    tagged = consolidated.with_columns({
        "has_kev": Computed("UInt8", "has(sources, 'kev')"),
        "has_shodan": Computed("UInt8", "has(sources, 'shodan')"),
    })

    both_count = await (await tagged.where("has_kev AND has_shodan")["cve_id"].count()).data()
    kev_only_count = await (await tagged.where("has_kev AND NOT has_shodan")["cve_id"].count()).data()
    shodan_only_count = await (await tagged.where("has_shodan AND NOT has_kev")["cve_id"].count()).data()
    kev_high_epss = await (await tagged.where("has_kev AND epss > 0.5")["cve_id"].count()).data()

    stats = {
        "total_unique_cves": total,
        "in_both_sources": both_count,
        "kev_only": kev_only_count,
        "shodan_only": shodan_only_count,
        "kev_with_high_epss": kev_high_epss,
    }

    _print_consolidated_stats(stats)
    return stats


def _print_consolidated_stats(stats: dict) -> None:
    """Print consolidated table statistics."""
    print("\n--- Consolidated Table Coverage ---")
    print(f"  Total unique CVEs:     {_fmt(stats['total_unique_cves'])}")
    print(f"  In both sources:       {_fmt(stats['in_both_sources'])}")
    print(f"  KEV only:              {_fmt(stats['kev_only'])}")
    print(f"  Shodan only:           {_fmt(stats['shodan_only'])}")
    print(f"  KEV + high EPSS (>0.5): {_fmt(stats['kev_with_high_epss'])}")


# =============================================================================
# Phase 4: Combined report
# =============================================================================


@task
async def generate_threat_report(
    kev: Object,
    cves: Object,
    consolidated: Object,
    kev_report: dict,
    shodan_analysis: dict,
    consolidated_stats: dict,
) -> dict:
    """Combine all source analyses into a unified threat intelligence report."""
    report = {
        "kev": kev_report["kev_summary"],
        "cvss_distribution": shodan_analysis["cvss_distribution"],
        "epss_distribution": shodan_analysis["epss_distribution"],
        "high_risk": shodan_analysis["high_risk"],
        "consolidated": consolidated_stats,
    }

    kev_md = await kev.view(limit=5).markdown()
    cves_md = await cves.view(limit=5).markdown(truncate={"summary": 40})
    consolidated_md = await consolidated.view(limit=5).markdown(truncate={"summary": 40})

    _print_threat_report(report, kev_md, cves_md, consolidated_md)
    return report


# =============================================================================
# Report formatting
# =============================================================================


def _fmt(value: object) -> str:
    """Format a numeric value for display."""
    if isinstance(value, float):
        return f"{value:,.2f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def _print_kev_report(report: dict) -> None:
    """Print CISA KEV analysis report."""
    kev = report["kev_summary"]
    print("\n### CISA KEV Analysis Report\n")
    print(f"- Total KEV entries: {_fmt(kev['total_vulnerabilities'])}")
    print(f"- Ransomware-linked: {_fmt(kev['ransomware_linked'])} ({_fmt(kev['ransomware_pct'])}%)")

    print("\n#### Top 10 Vendors by KEV Count\n")
    for vendor, count in kev["top_vendors"].items():
        print(f"- {vendor}: {count}")

    print("\n#### KEV Entries by Year\n")
    for year, count in kev["by_year"].items():
        print(f"- {year}: {count}")


def _print_field_table(columns: dict[str, ColumnInfo]) -> None:
    """Print a markdown table of field names, types, and descriptions.

    Skips columns without a description (e.g. internal aai_id).
    """
    described = {f: c for f, c in columns.items() if c.description}
    if not described:
        return
    name_w = max(len("Field"), max(len(f) for f in described))
    type_w = max(len("Type"), max(len(c.ch_type()) for c in described.values()))
    desc_w = max(len("Description"), max(len(c.description) for c in described.values()))

    print(f"| {'Field':<{name_w}s} | {'Type':<{type_w}s} | {'Description':<{desc_w}s} |")
    print(f"|{'-' * (name_w + 2)}|{'-' * (type_w + 2)}|{'-' * (desc_w + 2)}|")
    for field, col in described.items():
        print(f"| {field:<{name_w}s} | {col.ch_type():<{type_w}s} | {col.description:<{desc_w}s} |")



def _print_md_table(md: str) -> None:
    """Print a pre-rendered markdown table."""
    for line in md.splitlines():
        print(line)


def _print_threat_report(
    report: dict,
    kev_md: str,
    cves_md: str,
    consolidated_md: str,
) -> None:
    """Print unified threat intelligence report."""
    print("\n## Cyber Threat Intelligence Report\n")

    # ---- Source 1: CISA KEV ----
    kev = report["kev"]
    print("### Source 1: CISA KEV (Known Exploited Vulnerabilities)\n")
    print(f"URL: {CISA_KEV_URL}")
    print(f"Total rows: {_fmt(kev['total_vulnerabilities'])}\n")

    print("#### Field Schema\n")
    _print_field_table(KEV_COLUMNS)

    print("\n#### Sample (first 5 rows)\n")
    _print_md_table(kev_md)

    print("\n#### Statistics\n")
    print(f"- Total KEV entries: {_fmt(kev['total_vulnerabilities'])}")
    print(f"- Ransomware-linked: {_fmt(kev['ransomware_linked'])} ({_fmt(kev['ransomware_pct'])}%)")
    print("- Top 5 vendors:")
    for i, (vendor, count) in enumerate(kev["top_vendors"].items()):
        if i >= 5:
            break
        print(f"  - {vendor}: {count}")

    # ---- Source 2: Shodan CVEDB ----
    cvss = report["cvss_distribution"]
    epss = report["epss_distribution"]
    hr = report["high_risk"]
    print("\n### Source 2: Shodan CVEDB (CVE Database with EPSS)\n")
    print(f"URL: {SHODAN_CVEDB_URL}")
    print(f"Total rows: {_fmt(hr['total_cves'])}\n")

    print("#### Field Schema\n")
    _print_field_table(SHODAN_COLUMNS)

    print("\n#### Sample (first 5 rows)\n")
    _print_md_table(cves_md)

    print("\n#### Statistics\n")
    print(f"- CVSS — mean: {_fmt(cvss['avg'])}, std: {_fmt(cvss['std'])}, "
          f"median: {_fmt(cvss['median'])}, p90: {_fmt(cvss['p90'])}, p99: {_fmt(cvss['p99'])}")
    print(f"- CVSS — critical (>=9.0): {_fmt(cvss['critical_pct'])}%, "
          f"high (7.0-8.9): {_fmt(cvss['high_pct'])}%")
    print(f"- EPSS — mean: {_fmt(epss['avg'])}, median: {_fmt(epss['median'])}, "
          f"p90: {_fmt(epss['p90'])}, p99: {_fmt(epss['p99'])}")
    print(f"- EPSS — high probability (>0.5): {_fmt(epss['high_probability_pct'])}%")
    print(f"- High risk (CVSS>=9 AND EPSS>0.5): {_fmt(hr['high_risk_count'])} ({_fmt(hr['high_risk_pct'])}%)")

    # ---- Consolidated Table ----
    cons = report["consolidated"]
    print(f"\n### Consolidated Table ({START_DATE} — {END_DATE})\n")

    print("#### Field Schema\n")
    _print_field_table(MERGED_COLUMNS)

    print("\n#### Sample (first 5 rows)\n")
    _print_md_table(consolidated_md)

    print("\n#### Statistics\n")
    print(f"- Total unique CVEs: {_fmt(cons['total_unique_cves'])}")
    print(f"- In both sources: {_fmt(cons['in_both_sources'])}")
    print(f"- KEV only: {_fmt(cons['kev_only'])}")
    print(f"- Shodan only: {_fmt(cons['shodan_only'])}")
    print(f"- KEV + high EPSS (>0.5): {_fmt(cons['kev_with_high_epss'])}")


# =============================================================================
# Job Definition
# =============================================================================


@job("cyber_threat_feeds")
def cyber_threat_pipeline(shodan_limit: int = 5000):
    """
    Cyber Threat Feeds Pipeline.

    Loads CISA KEV and Shodan CVEDB data directly into ClickHouse via
    JSON URL ingestion, then consolidates into an AggregatingMergeTree
    table and performs multi-source threat analysis.

    DAG Structure:
        load_kev_data ------+---> analyze_kev ---------------------------------+
                            |                                                  |
                            +---------------------------+                      |
                                                        v                      |
        load_shodan_kev_cves ---+                 build_consolidated_table      |
                                |                       |                      |
                                +--> combine_shodan_cves +-> analyze_consolidated ---+
                                |       |                                            v
        load_shodan_general_cves +      +---> analyze_shodan_cves -------> generate_threat_report
    """
    # Phase 1: CISA KEV
    kev = load_kev_data()
    kev_report = analyze_kev(kev=kev)

    # Phase 2: Shodan CVEDB (two parallel loads + combine)
    shodan_kev = load_shodan_kev_cves()
    shodan_general = load_shodan_general_cves(limit=shodan_limit)
    cves = combine_shodan_cves(kev_cves=shodan_kev, general_cves=shodan_general)
    shodan_analysis = analyze_shodan_cves(cves=cves)

    # Phase 3: Consolidated AggregatingMergeTree table
    consolidated = build_consolidated_table(kev=kev, cves=cves)
    consolidated_stats = analyze_consolidated(consolidated=consolidated)

    # Phase 4: Combined report
    threat_report = generate_threat_report(
        kev=kev,
        cves=cves,
        consolidated=consolidated,
        kev_report=kev_report,
        shodan_analysis=shodan_analysis,
        consolidated_stats=consolidated_stats,
    )

    return threat_report


async def main():
    """Register the cyber threat feeds pipeline job."""
    created_job = await cyber_threat_pipeline()
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")


if __name__ == "__main__":
    asyncio.run(main())
