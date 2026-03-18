"""Threat intelligence report generation and formatting."""

from aaiclick.data.models import ColumnInfo
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .consolidated import MERGED_COLUMNS
from .epss import EPSS_COLUMNS, EPSS_URL
from .kev import CISA_KEV_URL, KEV_COLUMNS
from .shodan import SHODAN_CVEDB_URL, SHODAN_COLUMNS


@task
async def generate_threat_report(
    kev: Object,
    cves: Object,
    epss: Object,
    consolidated: Object,
    kev_report: dict,
    shodan_analysis: dict,
    epss_analysis: dict,
    consolidated_stats: dict,
    start_date: str,
    end_date: str,
) -> dict:
    """Combine all source analyses into a unified threat intelligence report."""
    report = {
        "kev": kev_report["kev_summary"],
        "cvss_distribution": shodan_analysis["cvss_distribution"],
        "epss_distribution": shodan_analysis["epss_distribution"],
        "high_risk": shodan_analysis["high_risk"],
        "epss": epss_analysis,
        "consolidated": consolidated_stats,
    }

    kev_md = await kev.view(limit=5).markdown()
    cves_md = await cves.view(limit=5).markdown(truncate={"summary": 40})
    epss_md = await epss.view(limit=5).markdown()
    consolidated_md = await consolidated.view(limit=5).markdown(truncate={"summary": 40})

    _print_threat_report(report, kev_md, cves_md, epss_md, consolidated_md, start_date, end_date)
    return report


# =============================================================================
# Report formatting
# =============================================================================


def _fmt(value: object) -> str:
    """Format a numeric value for display."""
    if isinstance(value, float):
        return f"{value:,.2f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


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
    epss_md: str,
    consolidated_md: str,
    start_date: str,
    end_date: str,
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
    shodan_epss = report["epss_distribution"]
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
    print(f"- EPSS — mean: {_fmt(shodan_epss['avg'])}, median: {_fmt(shodan_epss['median'])}, "
          f"p90: {_fmt(shodan_epss['p90'])}, p99: {_fmt(shodan_epss['p99'])}")
    print(f"- EPSS — high probability (>0.5): {_fmt(shodan_epss['high_probability_pct'])}%")
    print(f"- High risk (CVSS>=9 AND EPSS>0.5): {_fmt(hr['high_risk_count'])} ({_fmt(hr['high_risk_pct'])}%)")

    # ---- Source 3: FIRST EPSS ----
    epss = report["epss"]
    print("\n### Source 3: FIRST EPSS (Exploit Prediction Scoring System)\n")
    print(f"URL: {EPSS_URL}")
    print(f"Total rows: {_fmt(epss['total_scored_cves'])}\n")

    print("#### Field Schema\n")
    _print_field_table(EPSS_COLUMNS)

    print("\n#### Sample (first 5 rows)\n")
    _print_md_table(epss_md)

    print("\n#### Statistics\n")
    print(f"- Total scored CVEs: {_fmt(epss['total_scored_cves'])}")
    print(f"- EPSS — mean: {_fmt(epss['avg'])}, median: {_fmt(epss['median'])}, "
          f"p90: {_fmt(epss['p90'])}, p99: {_fmt(epss['p99'])}")
    print(f"- High probability (>0.5): {_fmt(epss['high_probability_count'])} "
          f"({_fmt(epss['high_probability_pct'])}%)")

    # ---- Consolidated Table ----
    cons = report["consolidated"]
    print(f"\n### Consolidated Table ({start_date} — {end_date})\n")

    print("#### Field Schema\n")
    _print_field_table(MERGED_COLUMNS)

    print("\n#### Sample (first 5 rows)\n")
    _print_md_table(consolidated_md)

    print("\n#### Statistics\n")
    print(f"- Total unique CVEs: {_fmt(cons['total_unique_cves'])}")
    print(f"- In both KEV + Shodan: {_fmt(cons['in_both_sources'])}")
    print(f"- KEV only: {_fmt(cons['kev_only'])}")
    print(f"- Shodan only: {_fmt(cons['shodan_only'])}")
    print(f"- EPSS coverage (full feed): {_fmt(cons['epss_coverage'])}")
    print(f"- KEV with EPSS score: {_fmt(cons['kev_with_epss'])}")
    print(f"- KEV + high EPSS (>0.5): {_fmt(cons['kev_with_high_epss'])}")
