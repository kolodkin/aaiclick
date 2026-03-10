# JSON URL Ingestion - Implementation Plan

## Motivation

`create_object_from_url` currently supports tabular formats (Parquet, CSV, JSONEachRow) where each
row maps directly to an Object row. Real-world REST APIs return **nested JSON envelopes** — an
outer object wrapping an array of records:

```json
{
  "title": "CISA KEV Catalog",
  "count": 1200,
  "vulnerabilities": [
    {"cveID": "CVE-2021-1234", "vendorProject": "Acme", "dateAdded": "2024-01-15", ...},
    {"cveID": "CVE-2022-5678", "vendorProject": "Beta", "dateAdded": "2024-02-20", ...}
  ]
}
```

Goal: support loading these APIs directly into structured Objects with **zero Python memory
footprint** — ClickHouse fetches the URL, explodes the JSON array, and extracts typed columns
in a single `INSERT ... SELECT` statement.

## Target API

```python
obj = await create_object_from_url(
    url="https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json",
    format="RawBLOB",
    json_path="vulnerabilities",
    json_columns={
        "cveID":                      ColumnInfo("String"),
        "vendorProject":              ColumnInfo("String"),
        "product":                    ColumnInfo("String"),
        "vulnerabilityName":          ColumnInfo("String"),
        "dateAdded":                  ColumnInfo("Date"),
        "shortDescription":           ColumnInfo("String"),
        "requiredAction":             ColumnInfo("String"),
        "dueDate":                    ColumnInfo("Date"),
        "knownRansomwareCampaignUse": ColumnInfo("String"),
        "notes":                      ColumnInfo("String", nullable=True),
        "cwes":                       ColumnInfo("String", array=True),
    },
)
```

### Generated SQL (single statement)

```sql
INSERT INTO obj_xxxxx (cveID, vendorProject, product, vulnerabilityName,
                       dateAdded, shortDescription, requiredAction, dueDate,
                       knownRansomwareCampaignUse, notes, cwes)
SELECT
    JSONExtractString(elem, 'cveID')                      AS `cveID`,
    JSONExtractString(elem, 'vendorProject')               AS `vendorProject`,
    JSONExtractString(elem, 'product')                     AS `product`,
    JSONExtractString(elem, 'vulnerabilityName')            AS `vulnerabilityName`,
    JSONExtract(elem, 'dateAdded', 'Date')                 AS `dateAdded`,
    JSONExtractString(elem, 'shortDescription')             AS `shortDescription`,
    JSONExtractString(elem, 'requiredAction')               AS `requiredAction`,
    JSONExtract(elem, 'dueDate', 'Date')                   AS `dueDate`,
    JSONExtractString(elem, 'knownRansomwareCampaignUse')   AS `knownRansomwareCampaignUse`,
    JSONExtractString(elem, 'notes')                        AS `notes`,
    JSONExtract(elem, 'cwes', 'Array(String)')              AS `cwes`
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(raw_blob, 'vulnerabilities')) AS elem
    FROM url('https://...', 'RawBLOB')
)
```

### `JSONExtract` variant selection

The correct ClickHouse function depends on the `ColumnInfo` base type:

| ColumnInfo type | ClickHouse function                              |
|-----------------|--------------------------------------------------|
| `String`        | `JSONExtractString(elem, 'field')`               |
| `Int*`/`UInt*`  | `JSONExtractInt(elem, 'field')`                  |
| `Float*`        | `JSONExtractFloat(elem, 'field')`                |
| `Bool`          | `JSONExtractBool(elem, 'field')`                 |
| Other / complex | `JSONExtract(elem, 'field', '<ch_type>')`        |

When `ColumnInfo.array=True`, always use the generic form: `JSONExtract(elem, 'field', 'Array(T)')`.
When `ColumnInfo.nullable=True`, always use the generic form: `JSONExtract(elem, 'field', 'Nullable(T)')`.

## Phases

### Phase 1: Add `RawBLOB` and `JSONAsString` formats ✅

| Task                                                                    | Status |
|-------------------------------------------------------------------------|--------|
| Add `RawBLOB` and `JSONAsString` to `SUPPORTED_URL_FORMATS`            | ✅     |

**Implementation**: `aaiclick/data/url.py` — see `SUPPORTED_URL_FORMATS` frozenset.

### Phase 2: JSON extraction helpers ✅

| Task                                                                    | Status |
|-------------------------------------------------------------------------|--------|
| Add `_json_extract_expr(field_name, col_info)` helper                   | ✅     |
| Add `_build_json_select(json_columns, json_path, format, safe_url)`     | ✅     |
| Add unit tests for `_json_extract_expr`                                  | ✅     |

**Implementation**: `aaiclick/data/url.py` — see `_json_extract_expr()` and `_build_json_select()`.

**Tests**: `aaiclick/data/test_url_json.py` — unit tests for all type variants.

### Phase 3: Extend `create_object_from_url` signature ✅

| Task                                                                    | Status |
|-------------------------------------------------------------------------|--------|
| Add `json_path: str | None = None` parameter                           | ✅     |
| Add `json_columns: dict[str, ColumnInfo] | None = None` parameter       | ✅     |
| Validate: `json_path` and `json_columns` must be provided together      | ✅     |
| Validate: `columns` and `json_columns` are mutually exclusive            | ✅     |
| When `json_columns` provided, skip DESCRIBE (types are explicit)         | ✅     |
| Build schema from `json_columns` dict                                    | ✅     |
| Generate INSERT...SELECT using `_build_json_select`                      | ✅     |
| Write integration tests                                                  | ✅     |

**Implementation**: `aaiclick/data/url.py` — see `_create_from_json()`.

**Parameter rules**:
- **Tabular mode** (existing): `columns` required, `json_path`/`json_columns` must be None
- **JSON mode** (new): `json_path` + `json_columns` required, `columns` must be empty or omitted
- `where` and `limit` work in both modes (applied to the subquery output)

**Tests**: `aaiclick/data/test_url_json.py` — validation tests + integration tests with local JSON server.

### Phase 4: Headers support ⚠️ NOT YET IMPLEMENTED

| Task                                                                    | Status |
|-------------------------------------------------------------------------|--------|
| Add `headers: dict[str, str] | None = None` parameter                   |        |
| Pass headers as third argument to `url()` table function                 |        |

ClickHouse `url()` supports headers via third positional arg:
```sql
url('https://api.github.com/...', 'RawBLOB', 'headers: Authorization=Bearer xxx')
```

This enables authenticated APIs (GitHub, NVD with API key, etc.).

## File Changes

| File                             | Changes                                               |
|----------------------------------|-------------------------------------------------------|
| `aaiclick/data/url.py`           | Phases 1-3 — formats, helpers, extended signature     |
| `aaiclick/data/test_url_json.py` | Tests for JSON extraction helpers and integration     |
| `aaiclick/data/__init__.py`      | No changes needed (already exports `create_object_from_url`) |
