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

from aaiclick.orchestration import job

from .consolidated import analyze_consolidated, build_consolidated_table
from .kev import analyze_kev, load_kev_data
from .report import generate_threat_report
from .shodan import (
    analyze_shodan_cves,
    combine_shodan_cves,
    load_shodan_general_cves,
    load_shodan_kev_cves,
)

# Shared date window — both sources are filtered to this range so the
# consolidated table has meaningful cross-source overlap.
START_DATE = "2025-01-01"
END_DATE = "2026-01-01"


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
    shodan_kev = load_shodan_kev_cves(start_date=START_DATE, end_date=END_DATE)
    shodan_general = load_shodan_general_cves(
        start_date=START_DATE, end_date=END_DATE, limit=shodan_limit,
    )
    cves = combine_shodan_cves(kev_cves=shodan_kev, general_cves=shodan_general)
    shodan_analysis = analyze_shodan_cves(cves=cves)

    # Phase 3: Consolidated AggregatingMergeTree table
    consolidated = build_consolidated_table(
        kev=kev, cves=cves, start_date=START_DATE, end_date=END_DATE,
    )
    consolidated_stats = analyze_consolidated(consolidated=consolidated)

    # Phase 4: Combined report
    threat_report = generate_threat_report(
        kev=kev,
        cves=cves,
        consolidated=consolidated,
        kev_report=kev_report,
        shodan_analysis=shodan_analysis,
        consolidated_stats=consolidated_stats,
        start_date=START_DATE,
        end_date=END_DATE,
    )

    return threat_report


async def main():
    """Register the cyber threat feeds pipeline job."""
    created_job = await cyber_threat_pipeline()
    print(f"Registered job: {created_job.name} (ID: {created_job.id})")


if __name__ == "__main__":
    asyncio.run(main())
