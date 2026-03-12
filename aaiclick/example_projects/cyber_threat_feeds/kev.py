"""CISA KEV (Known Exploited Vulnerabilities) data loading and analysis."""

from aaiclick import create_object_from_url
from aaiclick.data.models import ColumnInfo, Computed
from aaiclick.data.object import Object
from aaiclick.orchestration import task

CISA_KEV_URL = (
    "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
)

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

    return {
        "kev_summary": {
            "total_vulnerabilities": ransomware["total_kev"],
            "ransomware_linked": ransomware["ransomware_linked"],
            "ransomware_pct": ransomware["ransomware_pct"],
            "top_vendors": {name: count for name, count in vendor_counts[:10]},
            "by_year": {year: count for year, count in year_counts},
        },
    }
