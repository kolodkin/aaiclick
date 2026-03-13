"""Shodan CVEDB data loading and analysis."""

from aaiclick import create_object_from_url
from aaiclick.data.models import ColumnInfo
from aaiclick.data.object import Object
from aaiclick.orchestration import task

SHODAN_CVEDB_URL = "https://cvedb.shodan.io/cves"

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


@task
async def load_shodan_kev_cves(start_date: str, end_date: str) -> Object:
    """Load KEV-flagged CVEs from Shodan CVEDB.

    Uses is_kev=true to fetch CVEs that Shodan knows are in the CISA KEV
    catalog, ensuring cross-source overlap in the consolidated table.
    """
    url = (
        f"{SHODAN_CVEDB_URL}"
        f"?is_kev=true&limit=5000"
        f"&start_date={start_date}&end_date={end_date}"
    )
    return await create_object_from_url(
        url=url,
        format="RawBLOB",
        json_path="cves",
        json_columns=SHODAN_COLUMNS,
    )


@task
async def load_shodan_general_cves(start_date: str, end_date: str, limit: int = 5000) -> Object:
    """Load non-KEV CVEs from Shodan CVEDB.

    Uses is_kev=false to fetch general CVEs that are not in CISA KEV,
    providing broader vulnerability coverage with EPSS scores.
    """
    url = (
        f"{SHODAN_CVEDB_URL}"
        f"?is_kev=false&limit={limit}"
        f"&start_date={start_date}&end_date={end_date}"
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
