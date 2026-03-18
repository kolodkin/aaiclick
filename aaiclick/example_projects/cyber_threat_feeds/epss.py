"""FIRST EPSS (Exploit Prediction Scoring System) data loading and analysis."""

from aaiclick import create_object_from_url
from aaiclick.data.models import ColumnInfo
from aaiclick.data.object import Object
from aaiclick.orchestration import task

EPSS_URL = "https://epss.cyentia.com/epss_scores-current.csv.gz"

EPSS_COLUMNS = {
    "cve": ColumnInfo("String", description="CVE identifier (e.g. CVE-2024-1234)"),
    "epss": ColumnInfo("Float64", description="Exploitation probability in next 30 days (0-1)"),
    "percentile": ColumnInfo("Float64", description="Relative ranking among all scored CVEs (0-1)"),
}

# Skip the first line: #model_version:...,score_date:... comment
_EPSS_CH_SETTINGS = {"input_format_csv_skip_first_lines": 1}


@task
async def load_epss_data() -> Object:
    """Load FIRST EPSS scores for all CVEs from the current daily feed.

    The feed is a gzip-compressed CSV (auto-decompressed by ClickHouse) with a
    comment line before the column headers that must be skipped via ch_settings.
    Covers ~319K CVEs with exploitation probability and percentile scores.
    """
    return await create_object_from_url(
        url=EPSS_URL,
        columns=list(EPSS_COLUMNS.keys()),
        format="CSVWithNames",
        ch_settings=_EPSS_CH_SETTINGS,
    )


@task
async def analyze_epss(epss: Object) -> dict:
    """Analyze EPSS score distribution across all scored CVEs."""
    epss_col = epss["epss"]

    total_count = await (await epss_col.count()).data()
    avg_epss = await (await epss_col.mean()).data()
    median_epss = await (await epss_col.quantile(0.5)).data()
    p90_epss = await (await epss_col.quantile(0.9)).data()
    p99_epss = await (await epss_col.quantile(0.99)).data()

    high_prob = await (epss_col > 0.5)
    high_prob_count = await (await high_prob.sum()).data()

    return {
        "total_scored_cves": total_count,
        "avg": avg_epss,
        "median": median_epss,
        "p90": p90_epss,
        "p99": p99_epss,
        "high_probability_count": high_prob_count,
        "high_probability_pct": (high_prob_count / total_count) * 100 if total_count > 0 else 0.0,
    }
