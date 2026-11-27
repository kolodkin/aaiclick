Export
---

# Export

<!-- req: export -->
Export data to a local file, in the format its extension names. The embedded
chdb engine writes straight to disk with no Python round-trip, so
multi-million-row exports stay memory-bounded. View constraints (`where`,
`limit`, `order_by`) are honored. An existing file is replaced only once the
export has succeeded. Returns the absolute path written.

```python
await obj.export("/tmp/data.csv")
await obj.export("/tmp/data.parquet")
await obj.view(where="score > 10", limit=1000).export("/tmp/top.jsonl")
```

## Supported extensions

<!-- req: export-extensions -->
| Extension                       | ClickHouse format | Notes                                  |
|---------------------------------|-------------------|----------------------------------------|
| `.csv`                          | `CSV`             | no header row                          |
| `.tsv`                          | `TSV`             | no header row                          |
| `.json`                         | `JSON`            | one document: `meta`, `data`, `rows`   |
| `.jsonl` / `.ndjson`            | `JSONEachRow`     | newline-delimited JSON                 |
| `.parquet`                      | `Parquet`         | columnar, best for large datasets      |
| `.arrow`                        | `Arrow`           | Apache Arrow IPC stream                |
| `.orc`                          | `ORC`             | Apache ORC columnar                    |
| `.avro`                         | `Avro`            | Avro container with embedded schema    |
| `.md`                           | `Markdown`        | pipe-delimited markdown table          |
| `.xml`                          | `XML`             | one `<row>` element per record         |
| `.sql`                          | `SQLInsert`       | `INSERT INTO table VALUES (...)`       |

<!-- req: export-format-override -->
These are the formats ClickHouse itself picks for each extension. `format=`
overrides the extension with any ClickHouse output format:

```python
await obj.export("/tmp/data.csv", format="CSVWithNames")  # with a header row
await obj.export("/tmp/data.bin", format="Native")
```

<!-- req: export-ch-settings -->
`ch_settings=` sends ClickHouse settings with the export statement:

```python
await obj.export("/tmp/data.csv", ch_settings={"format_csv_delimiter": ";"})
await obj.export(url, ch_settings={"output_format_parquet_compression_method": "snappy"})
```

<!-- req: export-reserved-bucket-settings -->
On a bucket export, `s3_truncate_on_insert`, `s3_create_new_file_on_insert` and
`s3_allow_server_credentials_in_user_queries` raise: `overwrite` owns the
first, the second would write to a different key, and the third stays a
[profile setting](blob_storage.md#credentials).

## Compression

<!-- req: export-compression -->
Append a compression suffix and ClickHouse compresses the file: `.gz`, `.xz`,
`.zst`, `.br`, `.lz4`, `.snappy`. `.bz2` works on a ClickHouse server only;
chdb cannot write it and raises `ValueError`.

```python
await obj.export("/tmp/data.csv.gz")
await obj.export("/tmp/data.parquet.gz")
await obj.export("/tmp/data.json.xz")
```

!!! warning "Local files need the chdb backend"
    <!-- req: export-local-needs-chdb -->
    A remote ClickHouse cannot reach the client's disk, so a local path raises
    `ValueError` there. Export to a [bucket](#bucket-targets) instead.

## Bucket targets

<!-- req: export-bucket -->
An `http(s)://` or `s3://` path is written to object storage by ClickHouse
itself, on both backends, and `export` returns the URL:

```python
url = await daily.export("s3://lake/reports/daily_2026_10.parquet")
await daily.export(url, overwrite=True)  # an existing key raises without it
```

Compression suffixes and view constraints work as for local files.
Credentials: [Blob Storage](blob_storage.md#blob-storage).
