"""
Aggregation Table example for aaiclick.

Demonstrates the multi-source aggregation pattern: multiple data sources
with different schemas insert() into a shared AggregatingMergeTree table
(missing nullable columns auto-fill with NULL), then collapse via
group_by().agg() with any() to merge columns from different sources into
a single row per key.

Uses AggregatingMergeTree with ORDER BY on the key column so ClickHouse
can optimize reads by key and merge parts efficiently.

This pattern is used in the Cyber Threat Feeds pipeline to merge CVE data
from CISA KEV, Shodan CVEDB, and other sources into one unified table.
"""

import asyncio

from aaiclick import create_object, create_object_from_value
from aaiclick.data.data_context import data_context, get_ch_client
from aaiclick.data.models import (
    ENGINE_AGGREGATING_MERGE_TREE,
    FIELDTYPE_ARRAY,
    ColumnInfo,
    Computed,
    Schema,
)


async def example():
    """Run the aggregation table example."""
    # ---------------------------------------------------------------
    # Step 1: Create source data (simulating two different feeds)
    # ---------------------------------------------------------------
    print("Step 1: Create two data sources")
    print("-" * 50)

    # Source A: vulnerability catalog (has vendor and severity info)
    catalog = await create_object_from_value({
        "cve_id": ["CVE-2024-001", "CVE-2024-002", "CVE-2024-003"],
        "vendor": ["Apache", "Microsoft", "Linux"],
        "severity": ["Critical", "High", "Medium"],
    })
    print(f"  Catalog: {await (await catalog.count()).data()} CVEs with vendor/severity")

    # Source B: scoring database (has CVSS and EPSS scores)
    scores = await create_object_from_value({
        "cve_id": ["CVE-2024-001", "CVE-2024-003", "CVE-2024-004"],
        "cvss": [9.8, 6.5, 8.1],
        "epss": [0.95, 0.12, 0.67],
    })
    print(f"  Scores:  {await (await scores.count()).data()} CVEs with CVSS/EPSS")
    print()

    # ---------------------------------------------------------------
    # Step 2: Create AggregatingMergeTree table with ORDER BY cve_id
    # ---------------------------------------------------------------
    print("Step 2: Create AggregatingMergeTree table and insert from each source")
    print("-" * 50)

    schema = Schema(
        fieldtype=FIELDTYPE_ARRAY,
        columns={
            "aai_id": ColumnInfo("UInt64"),
            "cve_id": ColumnInfo("String"),
            "in_catalog": ColumnInfo("UInt8"),
            "in_scores": ColumnInfo("UInt8"),
            "vendor": ColumnInfo("String", nullable=True),
            "severity": ColumnInfo("String", nullable=True),
            "cvss": ColumnInfo("Float64", nullable=True),
            "epss": ColumnInfo("Float64", nullable=True),
        },
        engine=ENGINE_AGGREGATING_MERGE_TREE,
        order_by="cve_id",
    )
    agg = await create_object(schema)

    # Verify engine and ORDER BY
    ch = get_ch_client()
    result = await ch.query(
        f"SELECT engine, sorting_key FROM system.tables WHERE name = '{agg.table}'"
    )
    engine_name, sorting_key = result.result_rows[0]
    print(f"  Engine: {engine_name}, ORDER BY: {sorting_key}")

    # Insert catalog with computed flag columns; score columns auto-fill NULL
    view_catalog = catalog.with_columns({
        "in_catalog": Computed("UInt8", "1"),
        "in_scores": Computed("UInt8", "0"),
    })
    await agg.insert(view_catalog)
    print("  Inserted catalog data (3 rows) — cvss/epss auto-filled with NULL")

    # Insert scores with computed flag columns; vendor/severity auto-fill NULL
    view_scores = scores.with_columns({
        "in_catalog": Computed("UInt8", "0"),
        "in_scores": Computed("UInt8", "1"),
    })
    await agg.insert(view_scores)
    print("  Inserted scores data (3 rows) — vendor/severity auto-filled with NULL")
    print(f"  Raw table has {await (await agg.count()).data()} rows (before collapse)")
    print()

    # ---------------------------------------------------------------
    # Step 3: Collapse with GROUP BY + any()
    # ---------------------------------------------------------------
    print("Step 3: Collapse via group_by('cve_id') with any()/max()")
    print("-" * 50)

    merged = await agg.group_by("cve_id").agg({
        "in_catalog": "max",
        "in_scores": "max",
        "vendor": "any",
        "severity": "any",
        "cvss": "any",
        "epss": "any",
    })

    data = await merged.data()
    print(f"  Merged table: {len(data['cve_id'])} unique CVEs")
    print()

    # ---------------------------------------------------------------
    # Step 4: Display merged results
    # ---------------------------------------------------------------
    print("Step 4: Merged results")
    print("-" * 50)
    print(f"  {'CVE':<16} {'Cat':>3} {'Scr':>3} {'Vendor':<12} {'Severity':<10} {'CVSS':>5} {'EPSS':>5}")
    print(f"  {'-'*14}  {'---':>3} {'---':>3} {'-'*10}   {'-'*8}   {'-----':>5} {'-----':>5}")

    for i in range(len(data["cve_id"])):
        cve = data["cve_id"][i]
        cat = data["in_catalog"][i]
        scr = data["in_scores"][i]
        vendor = data["vendor"][i] or "-"
        sev = data["severity"][i] or "-"
        cvss = f"{data['cvss'][i]:.1f}" if data["cvss"][i] is not None else "-"
        epss = f"{data['epss'][i]:.2f}" if data["epss"][i] is not None else "-"
        print(f"  {cve:<16} {cat:>3} {scr:>3} {vendor:<12} {sev:<10} {cvss:>5} {epss:>5}")

    print()
    print("  Legend: Cat=in_catalog, Scr=in_scores")
    print("  CVE-2024-001: in both sources (has all fields)")
    print("  CVE-2024-002: catalog only (no scores)")
    print("  CVE-2024-003: in both sources")
    print("  CVE-2024-004: scores only (no vendor/severity)")


async def amain():
    """Run as standalone."""
    async with data_context():
        await example()


if __name__ == "__main__":
    asyncio.run(amain())
