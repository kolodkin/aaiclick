# Cyber Threat Feeds Pipeline - Example Project Plan

## Overview

A distributed pipeline example that loads multiple cybersecurity data sources into ClickHouse
Objects, enriches/correlates them, and produces a unified threat report — all computation
inside ClickHouse, zero Python memory footprint.

**Directory**: `aaiclick/example_projects/cyber_threat_feeds/`

## Data Sources

### Source 1: CISA KEV (Known Exploited Vulnerabilities)

| Property      | Value                                                                                  |
|---------------|----------------------------------------------------------------------------------------|
| URL           | `https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json`  |
| Format        | JSON envelope — `{"vulnerabilities": [...]}` array                                     |
| Load mode     | `RawBLOB` + `json_path="vulnerabilities"`                                              |
| Records       | ~1,539 actively exploited vulnerabilities                                              |
| Auth required | No                                                                                     |

**Columns to extract**:

| Field                         | ColumnInfo                     | JSONExtract variant            |
|-------------------------------|--------------------------------|--------------------------------|
| `cveID`                       | `ColumnInfo("String")`         | `JSONExtractString`            |
| `vendorProject`               | `ColumnInfo("String")`         | `JSONExtractString`            |
| `product`                     | `ColumnInfo("String")`         | `JSONExtractString`            |
| `vulnerabilityName`           | `ColumnInfo("String")`         | `JSONExtractString`            |
| `dateAdded`                   | `ColumnInfo("Date")`           | `JSONExtract(..., 'Date')`     |
| `shortDescription`            | `ColumnInfo("String")`         | `JSONExtractString`            |
| `requiredAction`              | `ColumnInfo("String")`         | `JSONExtractString`            |
| `dueDate`                     | `ColumnInfo("Date")`           | `JSONExtract(..., 'Date')`     |
| `knownRansomwareCampaignUse`  | `ColumnInfo("String")`         | `JSONExtractString`            |
| `notes`                       | `ColumnInfo("String", nullable=True)` | `JSONExtract(..., 'Nullable(String)')` |
| `cwes`                        | `ColumnInfo("String", array=True)`    | `JSONExtract(..., 'Array(String)')` |

### Source 2: Shodan CVEDB (CVE Database with EPSS)

| Property      | Value                                                             |
|---------------|-------------------------------------------------------------------|
| URL           | `https://cvedb.shodan.io/cves?limit=5000`                        |
| Format        | JSON envelope — `{"cves": [...]}` array                           |
| Load mode     | `RawBLOB` + `json_path="cves"`                                    |
| Records       | Configurable via `limit` param (up to full DB, ~300K+)            |
| Auth required | No                                                                |

**Columns to extract**:

| Field                | ColumnInfo                            | Notes                           |
|----------------------|---------------------------------------|---------------------------------|
| `cve_id`             | `ColumnInfo("String")`                | CVE identifier                  |
| `summary`            | `ColumnInfo("String")`                | Vulnerability description       |
| `cvss`               | `ColumnInfo("Float64", nullable=True)`| Combined CVSS score             |
| `cvss_v2`            | `ColumnInfo("Float64", nullable=True)`| CVSS v2 score (if available)    |
| `cvss_v3`            | `ColumnInfo("Float64", nullable=True)`| CVSS v3 score (if available)    |
| `epss`               | `ColumnInfo("Float64", nullable=True)`| EPSS probability (0-1)          |
| `ranking_epss`       | `ColumnInfo("Float64", nullable=True)`| EPSS percentile (0-1)           |
| `kev`                | `ColumnInfo("Bool")`                  | In CISA KEV catalog             |
| `published_time`     | `ColumnInfo("String")`                | ISO 8601 datetime               |
| `vendor`             | `ColumnInfo("String", nullable=True)` | Vendor name                     |
| `product`            | `ColumnInfo("String", nullable=True)` | Product name                    |
| `references`         | `ColumnInfo("String", array=True)`    | Reference URLs                  |

### Source 3: FIRST EPSS (Exploit Prediction Scoring System)

| Property      | Value                                                  |
|---------------|--------------------------------------------------------|
| URL           | `https://epss.cyentia.com/epss_scores-current.csv.gz`  |
| Format        | Gzip-compressed CSV with comment header line            |
| Load mode     | `CSVWithNames` (ClickHouse auto-decompresses gzip)      |
| Records       | ~319,355 CVEs with exploitation probability scores      |
| Auth required | No                                                      |

**Columns**:

| Field        | Type    | Description                                     |
|--------------|---------|-------------------------------------------------|
| `cve`        | String  | CVE identifier (e.g., "CVE-2024-1234")          |
| `epss`       | Float64 | Probability of exploitation in next 30 days     |
| `percentile` | Float64 | Relative ranking among all scored CVEs          |

**Note**: The CSV has a comment first line (`#model_version:...`) that needs to be
skipped. ClickHouse's `input_format_csv_skip_first_lines` setting can handle this.
This may require passing settings to the `url()` call or pre-processing.
If the comment line causes issues, we can use `RawBLOB` + custom parsing, or
skip EPSS as a standalone source since Shodan CVEDB already includes EPSS scores.

### Source 4: NVD CVE 2.0 API (National Vulnerability Database)

| Property      | Value                                                                     |
|---------------|---------------------------------------------------------------------------|
| URL           | `https://services.nvd.nist.gov/rest/json/cves/2.0?resultsPerPage=2000`   |
| Format        | JSON envelope — `{"vulnerabilities": [{"cve": {...}}, ...]}` array        |
| Load mode     | `RawBLOB` + `json_path="vulnerabilities"`                                  |
| Records       | ~336,818 total (paginated, 2000/page); demo loads single page             |
| Auth required | No (rate limited: 5 req/30s without key)                                  |

**Challenge**: Nested structure — each record is `{"cve": {"id": "...", ...}}` so JSONExtract
paths need to traverse into `cve` subobject. This may require a two-step approach or
using `JSONExtract(elem, 'cve', 'id')` dot-path syntax.

**Columns to extract** (from the `cve` subobject inside each array element):

| Field              | ColumnInfo               | JSONExtract path                    |
|--------------------|--------------------------|-------------------------------------|
| `id`               | `ColumnInfo("String")`   | `JSON_VALUE(elem, '$.cve.id')`      |
| `sourceIdentifier` | `ColumnInfo("String")`   | `JSON_VALUE(elem, '$.cve.sourceIdentifier')` |
| `published`        | `ColumnInfo("String")`   | `JSON_VALUE(elem, '$.cve.published')` |
| `lastModified`     | `ColumnInfo("String")`   | `JSON_VALUE(elem, '$.cve.lastModified')` |
| `vulnStatus`       | `ColumnInfo("String")`   | `JSON_VALUE(elem, '$.cve.vulnStatus')` |

**Note**: NVD's nested structure (`vulnerabilities[].cve.field`) doesn't map cleanly to
our current `_json_extract_expr` which expects flat fields. Implementation options:
1. Extend `_json_extract_expr` to support dot-paths
2. Use a two-step extraction (extract `cve` as String, then re-parse)
3. Defer NVD to a later phase and use Shodan CVEDB (which has NVD data pre-flattened)

**Recommendation**: Start without NVD. Shodan CVEDB already aggregates NVD data with
EPSS scores in a flat structure. Add NVD in a later phase if deeper CVE details are needed.

### Source 5: OSV.dev (Open Source Vulnerabilities)

| Property      | Value                                              |
|---------------|----------------------------------------------------|
| API           | `https://api.osv.dev/v1/query` (POST only)         |
| Format        | JSON response to POST query                         |
| Auth required | No                                                  |

**Challenge**: POST-only API — ClickHouse's `url()` table function only supports GET requests.
Cannot be loaded directly via `create_object_from_url`.

**Options**:
1. Use a lightweight Python wrapper to fetch → save to temp file → load via file URL
2. Defer to a future phase when we add POST support to `create_object_from_url`
3. Skip for this example (focus on GET-accessible feeds)

**Recommendation**: Skip for initial implementation. OSV requires POST with package-specific
queries, making it unsuitable for bulk feed loading.

## Pipeline Architecture

### Phase 1: CISA KEV (single source, simplest)

```
load_kev -> analyze_kev -> kev_report
```

**Tasks**:
- `load_kev_data()` — Load CISA KEV via JSON mode
- `analyze_kev_by_vendor()` — Group by vendor, count vulns
- `analyze_kev_by_year()` — Group by year (from dateAdded), count vulns
- `analyze_kev_ransomware()` — Filter/count ransomware-linked vulns
- `generate_kev_report()` — Combine analyses into report

### Phase 2: Shodan CVEDB (add second source)

```
load_kev --------+
                  +-> correlate_kev_cvedb -> enriched_report
load_shodan_cves -+
```

**Tasks**:
- `load_shodan_cves()` — Load Shodan CVEDB via JSON mode
- `analyze_cvss_distribution()` — CVSS score statistics (mean, std, quantiles)
- `analyze_epss_distribution()` — EPSS score statistics
- `find_high_risk_cves()` — Filter: CVSS >= 9.0 AND EPSS > 0.5
- `correlate_kev_cvedb()` — Cross-reference KEV with CVEDB for EPSS enrichment

### Phase 3: Multi-source report (combine all)

```
load_kev --------+
                  +-> correlate -> analyze_risk_tiers -> generate_report
load_shodan_cves -+
```

**Tasks**:
- `analyze_risk_tiers()` — Categorize CVEs by risk level (Critical/High/Medium/Low)
  using combined CVSS + EPSS + KEV status
- `generate_threat_report()` — Final report combining all sources:
  - KEV summary (count, top vendors, ransomware %)
  - CVSS distribution across all CVEs
  - EPSS high-probability threats
  - Cross-referenced high-risk findings

## Job DAG (Final)

```
                                +-> analyze_kev_by_vendor ------+
                                |                               |
load_kev_data ------------------+-> analyze_kev_by_year --------+
                                |                               |
                                +-> analyze_kev_ransomware -----+---> generate_threat_report
                                |                               |
                                +-------------------------------+
                                                                |
load_shodan_cves ------+-> analyze_cvss_distribution -----------+
                       |                                        |
                       +-> analyze_epss_distribution -----------+
                       |                                        |
                       +-> find_high_risk_cves -----------------+
```

## Implementation Phases

### Phase 1: CISA KEV source + analysis

| Task                                                     | Status |
|----------------------------------------------------------|--------|
| Create `aaiclick/example_projects/cyber_threat_feeds/`   |        |
| Implement `load_kev_data` task                           |        |
| Implement `analyze_kev_by_vendor` task                   |        |
| Implement `analyze_kev_by_year` task                     |        |
| Implement `analyze_kev_ransomware` task                  |        |
| Implement `generate_kev_report` task                     |        |
| Create `__main__.py` entry point                         |        |
| Create `cyber_threat_feeds.sh` launch script             |        |
| Add to CI matrix in `test.yaml`                          |        |
| Push and verify CI                                       |        |

### Phase 2: Shodan CVEDB source + enrichment

| Task                                                     | Status |
|----------------------------------------------------------|--------|
| Implement `load_shodan_cves` task                        |        |
| Implement `analyze_cvss_distribution` task               |        |
| Implement `analyze_epss_distribution` task               |        |
| Implement `find_high_risk_cves` task                     |        |
| Update job DAG with new source                           |        |
| Push and verify CI                                       |        |

### Phase 3: Cross-source correlation + final report

| Task                                                     | Status |
|----------------------------------------------------------|--------|
| Implement `analyze_risk_tiers` task                      |        |
| Implement `generate_threat_report` task (combined)       |        |
| Print formatted report to stdout                         |        |
| Update documentation                                     |        |
| Push and verify CI                                       |        |

### Phase 4 (Future): Additional sources

| Task                                                     | Status |
|----------------------------------------------------------|--------|
| Add EPSS CSV feed as standalone source                   |        |
| Extend `create_object_from_url` for nested JSON paths    |        |
| Add NVD CVE 2.0 API support (nested `cve` subobject)    |        |
| Add POST support for OSV.dev                             |        |
| Add headers support for authenticated APIs               |        |

## File Changes

| File                                                        | Description                              |
|-------------------------------------------------------------|------------------------------------------|
| `aaiclick/example_projects/cyber_threat_feeds/__init__.py`  | Tasks, job definition, report formatting |
| `aaiclick/example_projects/cyber_threat_feeds/__main__.py`  | Entry point (register job)               |
| `aaiclick/example_projects/cyber_threat_feeds/cyber_threat_feeds.sh` | Launch script              |
| `.github/workflows/test.yaml`                               | Add to example-projects matrix           |
| `docs/cyber_threat_feeds.md`                                | This plan document                       |

## Key Design Decisions

1. **Start with CISA KEV + Shodan CVEDB only** — both are single-GET JSON feeds that
   work perfectly with our JSON mode. Skip EPSS standalone (redundant with Shodan),
   NVD (nested structure needs extension), and OSV (POST-only).

2. **Shodan CVEDB over NVD** — Shodan pre-flattens NVD data and includes EPSS scores,
   making it ideal for our flat JSONExtract approach. NVD's deeply nested structure
   would require extending `_json_extract_expr` for dot-paths.

3. **Configurable limits** — Both sources support limiting via URL params or `limit=`
   parameter. Default to smaller subsets for CI (e.g., 1000 Shodan CVEs) with option
   for full dataset in production.

4. **Source-by-source phasing** — Each phase adds one source and is independently
   testable in CI. Phase 1 is a working example on its own.
