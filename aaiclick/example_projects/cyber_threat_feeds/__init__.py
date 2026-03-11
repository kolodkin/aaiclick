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

# =============================================================================
# Column definitions
# =============================================================================

KEV_COLUMNS = {
    "cveID": ColumnInfo("String"),
    "vendorProject": ColumnInfo("String"),
    "product": ColumnInfo("String"),
    "vulnerabilityName": ColumnInfo("String"),
    "dateAdded": ColumnInfo("Date"),
    "shortDescription": ColumnInfo("String"),
    "requiredAction": ColumnInfo("String"),
    "dueDate": ColumnInfo("Date"),
    "knownRansomwareCampaignUse": ColumnInfo("String"),
    "notes": ColumnInfo("String", nullable=True),
    "cwes": ColumnInfo("String", array=True),
}

SHODAN_COLUMNS = {
    "cve_id": ColumnInfo("String"),
    "summary": ColumnInfo("String"),
    "cvss": ColumnInfo("Float64", nullable=True),
    "cvss_v2": ColumnInfo("Float64", nullable=True),
    "cvss_v3": ColumnInfo("Float64", nullable=True),
    "epss": ColumnInfo("Float64", nullable=True),
    "ranking_epss": ColumnInfo("Float64", nullable=True),
    "kev": ColumnInfo("Bool"),
    "published_time": ColumnInfo("String"),
    "vendor": ColumnInfo("String", nullable=True),
    "product": ColumnInfo("String", nullable=True),
    "references": ColumnInfo("String", array=True),
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


@task
async def analyze_kev_by_vendor(kev: Object) -> Object:
    """Top vendors by number of known exploited vulnerabilities."""
    return await kev.group_by("vendorProject").agg({
        "cveID": "count",
    })


@task
async def analyze_kev_by_year(kev: Object) -> Object:
    """KEV entries grouped by year added to the catalog.

    Materializes toYear(dateAdded) into a 'year' column since group_by()
    only accepts column names, not SQL expressions.
    """
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "year": ColumnInfo("UInt16"),
            "cveID": ColumnInfo("String"),
        },
    )
    year_obj = await create_object(schema)
    ch = get_ch_client()
    await ch.command(
        f"INSERT INTO {year_obj.table} (year, cveID) "
        f"SELECT toYear(dateAdded) AS year, cveID FROM {kev.table}"
    )
    return await year_obj.group_by("year").agg({"cveID": "count"})


@task
async def analyze_kev_ransomware(kev: Object) -> dict:
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
async def generate_kev_report(
    kev: Object,
    by_vendor: Object,
    by_year: Object,
    ransomware_stats: dict,
) -> dict:
    """Combine KEV analyses into a summary report."""
    total_kev = ransomware_stats["total_kev"]

    vendor_data = await by_vendor.data()
    vendor_counts = sorted(
        zip(vendor_data["vendorProject"], vendor_data["cveID"]),
        key=lambda x: x[1],
        reverse=True,
    )
    top_vendors = vendor_counts[:10]

    year_data = await by_year.data()
    year_counts = sorted(
        zip(year_data["year"], year_data["cveID"]),
        key=lambda x: x[0],
    )

    report = {
        "kev_summary": {
            "total_vulnerabilities": total_kev,
            "ransomware_linked": ransomware_stats["ransomware_linked"],
            "ransomware_pct": ransomware_stats["ransomware_pct"],
            "top_vendors": {name: count for name, count in top_vendors},
            "by_year": {year: count for year, count in year_counts},
        },
    }

    _print_kev_report(report)
    return report


# =============================================================================
# Phase 2: Shodan CVEDB tasks
# =============================================================================


@task
async def load_shodan_cves(limit: int = 5000) -> Object:
    """Load CVE data from Shodan CVEDB with EPSS scores.

    Uses date range filtering to target CVEs old enough to have EPSS scores
    computed. The Shodan API returns newest CVEs first by default, and recent
    CVEs typically have null EPSS values since scoring takes time.
    """
    url = (
        f"{SHODAN_CVEDB_URL}"
        f"?limit={limit}"
        f"&start_date=2024-01-01&end_date=2026-01-01"
    )
    return await create_object_from_url(
        url=url,
        format="RawBLOB",
        json_path="cves",
        json_columns=SHODAN_COLUMNS,
    )


@task
async def analyze_cvss_distribution(cves: Object) -> dict:
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


@task
async def analyze_epss_distribution(cves: Object) -> dict:
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


@task
async def find_high_risk_cves(cves: Object) -> dict:
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


# =============================================================================
# Phase 3: Consolidated AggregatingMergeTree table
# =============================================================================

CONSOLIDATED_COLUMNS = {
    "aai_id": ColumnInfo("UInt64"),
    "cve_id": ColumnInfo("String"),
    "in_kev": ColumnInfo("UInt8"),
    "in_shodan": ColumnInfo("UInt8"),
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


@task
async def build_consolidated_table(kev: Object, cves: Object) -> Object:
    """Build a consolidated AggregatingMergeTree table from all sources.

    Inserts KEV and Shodan data into a shared table keyed by cve_id,
    then collapses via group_by().agg() to merge columns from both
    sources into a single row per CVE.
    """
    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns=CONSOLIDATED_COLUMNS,
        engine=ENGINE_AGGREGATING_MERGE_TREE,
        order_by="cve_id",
    )
    agg = await create_object(schema)
    ch = get_ch_client()

    # Insert KEV data — map KEV column names to consolidated schema
    await ch.command(
        f"INSERT INTO {agg.table} "
        f"(cve_id, in_kev, in_shodan, vendor, product, "
        f"vulnerability_name, short_description, date_added, known_ransomware) "
        f"SELECT cveID, 1, 0, vendorProject, product, "
        f"vulnerabilityName, shortDescription, dateAdded, knownRansomwareCampaignUse "
        f"FROM {kev.table}"
    )

    # Insert Shodan data — score columns; vendor/product come from Shodan too
    await ch.command(
        f"INSERT INTO {agg.table} "
        f"(cve_id, in_kev, in_shodan, vendor, product, summary, "
        f"cvss, cvss_v2, cvss_v3, epss, ranking_epss) "
        f"SELECT cve_id, 0, 1, vendor, product, summary, "
        f"cvss, cvss_v2, cvss_v3, epss, ranking_epss "
        f"FROM {cves.table}"
    )

    # Collapse: merge rows from different sources into one row per CVE
    merged = await agg.group_by("cve_id").agg({
        "in_kev": "max",
        "in_shodan": "max",
        "vendor": "any",
        "product": "any",
        "vulnerability_name": "any",
        "short_description": "any",
        "date_added": "any",
        "known_ransomware": "any",
        "cvss": "any",
        "cvss_v2": "any",
        "cvss_v3": "any",
        "epss": "any",
        "ranking_epss": "any",
        "summary": "any",
    })

    return merged


@task
async def analyze_consolidated(consolidated: Object) -> dict:
    """Analyze the consolidated table for cross-source coverage stats."""
    total = await (await consolidated["cve_id"].count()).data()

    # Count CVEs by source presence
    both_sources = consolidated.where("in_kev = 1").where("in_shodan = 1")
    both_count = await (await both_sources["cve_id"].count()).data()

    kev_only = consolidated.where("in_kev = 1").where("in_shodan = 0")
    kev_only_count = await (await kev_only["cve_id"].count()).data()

    shodan_only = consolidated.where("in_kev = 0").where("in_shodan = 1")
    shodan_only_count = await (await shodan_only["cve_id"].count()).data()

    # High-risk from consolidated: in KEV + high EPSS
    kev_with_epss = consolidated.where("in_kev = 1").where("epss > 0.5")
    kev_high_epss = await (await kev_with_epss["cve_id"].count()).data()

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
    kev_report: dict,
    cvss_stats: dict,
    epss_stats: dict,
    high_risk: dict,
    consolidated_stats: dict,
) -> dict:
    """Combine all source analyses into a unified threat intelligence report."""
    report = {
        "kev": kev_report["kev_summary"],
        "cvss_distribution": cvss_stats,
        "epss_distribution": epss_stats,
        "high_risk": high_risk,
        "consolidated": consolidated_stats,
    }

    kev_sample = await kev.view(limit=10).data()
    cves_sample = await cves.view(limit=10).data()

    _print_threat_report(report, kev_sample, cves_sample)
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
    print("\n" + "=" * 60)
    print("CISA KEV ANALYSIS REPORT")
    print("=" * 60)

    print(f"\n  Total KEV entries:        {_fmt(kev['total_vulnerabilities'])}")
    print(f"  Ransomware-linked:        {_fmt(kev['ransomware_linked'])} ({_fmt(kev['ransomware_pct'])}%)")

    print("\n--- Top 10 Vendors by KEV Count ---")
    for vendor, count in kev["top_vendors"].items():
        print(f"  {vendor:<30s} {count:>5}")

    print("\n--- KEV Entries by Year ---")
    for year, count in kev["by_year"].items():
        print(f"  {year}  {count:>5}")

    print("=" * 60)


def _print_threat_report(
    report: dict,
    kev_sample: dict,
    cves_sample: dict,
) -> None:
    """Print unified threat intelligence report."""
    print("\n" + "=" * 60)
    print("CYBER THREAT INTELLIGENCE REPORT")
    print("=" * 60)

    print("\n--- Data Sources ---")
    print(f"  CISA KEV:     {CISA_KEV_URL}")
    print(f"    Total rows: {_fmt(report['kev']['total_vulnerabilities'])}")
    print(f"  Shodan CVEDB: {SHODAN_CVEDB_URL}")
    print(f"    Total rows: {_fmt(report['high_risk']['total_cves'])}")

    kev = report["kev"]
    print("\n--- CISA KEV Summary ---")
    print(f"  Total KEV entries:     {_fmt(kev['total_vulnerabilities'])}")
    print(f"  Ransomware-linked:     {_fmt(kev['ransomware_linked'])} ({_fmt(kev['ransomware_pct'])}%)")

    print("\n--- KEV Sample (first 10 rows) ---")
    print(f"  {'CVE ID':<20s} {'Vendor':<20s} {'Product':<20s} {'Date Added'}")
    for i in range(len(kev_sample["cveID"])):
        print(
            f"  {str(kev_sample['cveID'][i]):<20s} "
            f"{str(kev_sample['vendorProject'][i]):<20s} "
            f"{str(kev_sample['product'][i]):<20s} "
            f"{kev_sample['dateAdded'][i]}"
        )

    cvss = report["cvss_distribution"]
    print("\n--- CVSS Score Distribution ---")
    print(f"  Mean:     {_fmt(cvss['avg'])}")
    print(f"  Std:      {_fmt(cvss['std'])}")
    print(f"  Median:   {_fmt(cvss['median'])}")
    print(f"  P90:      {_fmt(cvss['p90'])}")
    print(f"  P99:      {_fmt(cvss['p99'])}")
    print(f"  Critical (>=9.0): {_fmt(cvss['critical_pct'])}%")
    print(f"  High (7.0-8.9):   {_fmt(cvss['high_pct'])}%")

    epss = report["epss_distribution"]
    print("\n--- EPSS Score Distribution ---")
    print(f"  Mean:     {_fmt(epss['avg'])}")
    print(f"  Median:   {_fmt(epss['median'])}")
    print(f"  P90:      {_fmt(epss['p90'])}")
    print(f"  P99:      {_fmt(epss['p99'])}")
    print(f"  High probability (>0.5): {_fmt(epss['high_probability_pct'])}%")

    print("\n--- Shodan CVEDB Sample (first 10 rows) ---")
    print(f"  {'CVE ID':<20s} {'CVSS':>6s} {'EPSS':>8s} {'KEV':>5s}")
    for i in range(len(cves_sample["cve_id"])):
        cvss_val = cves_sample["cvss"][i]
        epss_val = cves_sample["epss"][i]
        print(
            f"  {str(cves_sample['cve_id'][i]):<20s} "
            f"{_fmt(cvss_val) if cvss_val is not None else 'N/A':>6s} "
            f"{_fmt(epss_val) if epss_val is not None else 'N/A':>8s} "
            f"{'Yes' if cves_sample['kev'][i] else 'No':>5s}"
        )

    hr = report["high_risk"]
    print("\n--- High Risk CVEs (CVSS>=9 AND EPSS>0.5) ---")
    print(f"  Count:    {_fmt(hr['high_risk_count'])}")
    print(f"  Of total: {_fmt(hr['high_risk_pct'])}%")

    cons = report["consolidated"]
    print("\n--- Consolidated Table (AggregatingMergeTree) ---")
    print(f"  Total unique CVEs:      {_fmt(cons['total_unique_cves'])}")
    print(f"  In both sources:        {_fmt(cons['in_both_sources'])}")
    print(f"  KEV only:               {_fmt(cons['kev_only'])}")
    print(f"  Shodan only:            {_fmt(cons['shodan_only'])}")
    print(f"  KEV + high EPSS (>0.5): {_fmt(cons['kev_with_high_epss'])}")

    print("\n" + "=" * 60)


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
                                    +-> analyze_kev_by_vendor ------+
                                    |                               |
        load_kev_data --+-----------+-> analyze_kev_by_year --------+-> generate_kev_report --+
                        |           |                               |                         |
                        |           +-> analyze_kev_ransomware -----+                         |
                        |                                                                     |
                        +---> build_consolidated_table --> analyze_consolidated --+            |
                        |                                                        v            v
        load_shodan_cves --+---> analyze_cvss_distribution ------+-----> generate_threat_report
                           |                                     |
                           +--> analyze_epss_distribution -------+
                           |                                     |
                           +--> find_high_risk_cves -------------+
    """
    # Phase 1: CISA KEV
    kev = load_kev_data()
    by_vendor = analyze_kev_by_vendor(kev=kev)
    by_year = analyze_kev_by_year(kev=kev)
    ransomware = analyze_kev_ransomware(kev=kev)
    kev_report = generate_kev_report(
        kev=kev,
        by_vendor=by_vendor,
        by_year=by_year,
        ransomware_stats=ransomware,
    )

    # Phase 2: Shodan CVEDB
    cves = load_shodan_cves(limit=shodan_limit)
    cvss_stats = analyze_cvss_distribution(cves=cves)
    epss_stats = analyze_epss_distribution(cves=cves)
    high_risk = find_high_risk_cves(cves=cves)

    # Phase 3: Consolidated AggregatingMergeTree table
    consolidated = build_consolidated_table(kev=kev, cves=cves)
    consolidated_stats = analyze_consolidated(consolidated=consolidated)

    # Phase 4: Combined report
    threat_report = generate_threat_report(
        kev=kev,
        cves=cves,
        kev_report=kev_report,
        cvss_stats=cvss_stats,
        epss_stats=epss_stats,
        high_risk=high_risk,
        consolidated_stats=consolidated_stats,
    )

    return [
        kev,
        by_vendor,
        by_year,
        ransomware,
        kev_report,
        cves,
        cvss_stats,
        epss_stats,
        high_risk,
        consolidated,
        consolidated_stats,
        threat_report,
    ]


async def main():
    """Register the cyber threat feeds pipeline job."""
    created_job = await cyber_threat_pipeline()
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")


if __name__ == "__main__":
    asyncio.run(main())
