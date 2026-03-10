"""
Cyber Threat Feeds Pipeline - Multi-Source Security Data Example

Demonstrates loading multiple cybersecurity data feeds into ClickHouse
Objects via JSON URL ingestion, then analyzing and correlating them:

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
from aaiclick.data.models import ColumnInfo
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
            ransomware_count_val = ransomware_data["count(cveID)"][i]
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
    ransomware_stats: dict,
) -> dict:
    """Combine KEV analyses into a summary report."""
    total_kev = ransomware_stats["total_kev"]

    vendor_data = await by_vendor.data()
    vendor_counts = sorted(
        zip(vendor_data["vendorProject"], vendor_data["count(cveID)"]),
        key=lambda x: x[1],
        reverse=True,
    )
    top_vendors = vendor_counts[:10]

    report = {
        "kev_summary": {
            "total_vulnerabilities": total_kev,
            "ransomware_linked": ransomware_stats["ransomware_linked"],
            "ransomware_pct": ransomware_stats["ransomware_pct"],
            "top_vendors": {name: count for name, count in top_vendors},
        },
    }

    _print_kev_report(report)
    return report


# =============================================================================
# Phase 2: Shodan CVEDB tasks
# =============================================================================


@task
async def load_shodan_cves(limit: int = 5000) -> Object:
    """Load CVE data from Shodan CVEDB with EPSS scores."""
    return await create_object_from_url(
        url=f"{SHODAN_CVEDB_URL}?limit={limit}",
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
# Phase 3: Combined report
# =============================================================================


@task
async def generate_threat_report(
    kev_report: dict,
    cvss_stats: dict,
    epss_stats: dict,
    high_risk: dict,
) -> dict:
    """Combine all source analyses into a unified threat intelligence report."""
    report = {
        "kev": kev_report["kev_summary"],
        "cvss_distribution": cvss_stats,
        "epss_distribution": epss_stats,
        "high_risk": high_risk,
    }

    _print_threat_report(report)
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

    print("=" * 60)


def _print_threat_report(report: dict) -> None:
    """Print unified threat intelligence report."""
    print("\n" + "=" * 60)
    print("CYBER THREAT INTELLIGENCE REPORT")
    print("=" * 60)

    kev = report["kev"]
    print("\n--- CISA KEV Summary ---")
    print(f"  Total KEV entries:     {_fmt(kev['total_vulnerabilities'])}")
    print(f"  Ransomware-linked:     {_fmt(kev['ransomware_linked'])} ({_fmt(kev['ransomware_pct'])}%)")

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

    hr = report["high_risk"]
    print("\n--- High Risk CVEs (CVSS>=9 AND EPSS>0.5) ---")
    print(f"  Count:    {_fmt(hr['high_risk_count'])}")
    print(f"  Of total: {_fmt(hr['high_risk_pct'])}%")

    print("\n" + "=" * 60)


# =============================================================================
# Job Definition
# =============================================================================


@job("cyber_threat_feeds")
def cyber_threat_pipeline(shodan_limit: int = 5000):
    """
    Cyber Threat Feeds Pipeline.

    Loads CISA KEV and Shodan CVEDB data directly into ClickHouse via
    JSON URL ingestion, then performs multi-source threat analysis.

    DAG Structure:
                                    +-> analyze_kev_by_vendor ------+
                                    |                               |
        load_kev_data --------------+-> analyze_kev_ransomware -----+-> generate_kev_report --+
                                                                                              |
                                    +-> analyze_cvss_distribution --+                         |
                                    |                               |                         |
        load_shodan_cves -----------+-> analyze_epss_distribution --+-> generate_threat_report
                                    |                               |
                                    +-> find_high_risk_cves --------+
    """
    # Phase 1: CISA KEV
    kev = load_kev_data()
    by_vendor = analyze_kev_by_vendor(kev=kev)
    ransomware = analyze_kev_ransomware(kev=kev)
    kev_report = generate_kev_report(
        kev=kev,
        by_vendor=by_vendor,
        ransomware_stats=ransomware,
    )

    # Phase 2: Shodan CVEDB
    cves = load_shodan_cves(limit=shodan_limit)
    cvss_stats = analyze_cvss_distribution(cves=cves)
    epss_stats = analyze_epss_distribution(cves=cves)
    high_risk = find_high_risk_cves(cves=cves)

    # Phase 3: Combined report
    threat_report = generate_threat_report(
        kev_report=kev_report,
        cvss_stats=cvss_stats,
        epss_stats=epss_stats,
        high_risk=high_risk,
    )

    return [
        kev,
        by_vendor,
        ransomware,
        kev_report,
        cves,
        cvss_stats,
        epss_stats,
        high_risk,
        threat_report,
    ]


async def main():
    """Register the cyber threat feeds pipeline job."""
    created_job = await cyber_threat_pipeline()
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")


if __name__ == "__main__":
    asyncio.run(main())
