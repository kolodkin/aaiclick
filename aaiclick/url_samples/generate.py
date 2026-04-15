#!/usr/bin/env python3
"""
Generate sample data files for URL integration tests.

Requires pyarrow: pip install pyarrow

Usage:
    python -m aaiclick.url_samples.generate

Produces 200 rows with columns (id: int, price: float, name: string) in
every format aaiclick supports as URL input. The bare-CSV/TSV variants
(without column names) and the WithNamesAndTypes variants share the same
data but differ in how the header is encoded.
"""

import csv
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as pq

from aaiclick.data.data_context.chdb_client import get_shared_session

NUM_ROWS = 200
OUT_DIR = Path(__file__).parent

COLUMNS = ("id", "price", "name")
TYPES = ("Int64", "Float64", "String")

ids = list(range(1, NUM_ROWS + 1))
prices = [round(i * 1.5, 2) for i in range(1, NUM_ROWS + 1)]
names = [f"item_{i}" for i in range(1, NUM_ROWS + 1)]

ROWS = list(zip(ids, prices, names))


def _arrow_table() -> pa.Table:
    return pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "price": pa.array(prices, type=pa.float64()),
        "name": pa.array(names, type=pa.string()),
    })


def _write_csv(path: Path, header: list[list[str]]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        for row in header:
            writer.writerow(row)
        writer.writerows(ROWS)


def _write_tsv(path: Path, header_lines: list[str]) -> None:
    with open(path, "w") as f:
        for line in header_lines:
            f.write(line + "\n")
        for row in ROWS:
            f.write("\t".join(str(v) for v in row) + "\n")


def generate_csv() -> None:
    """CSVWithNames sample (with header)."""
    _write_csv(OUT_DIR / "sample.csv", [list(COLUMNS)])


def generate_csv_no_header() -> None:
    """Plain CSV sample (no header — for the ``CSV`` format)."""
    _write_csv(OUT_DIR / "sample_noheader.csv", [])


def generate_csv_with_types() -> None:
    """CSVWithNamesAndTypes sample (header row + types row)."""
    _write_csv(OUT_DIR / "sample_withtypes.csv", [list(COLUMNS), list(TYPES)])


def generate_tsv() -> None:
    """TSVWithNames sample (with header)."""
    _write_tsv(OUT_DIR / "sample.tsv", ["\t".join(COLUMNS)])


def generate_tsv_no_header() -> None:
    """Plain TSV sample (no header)."""
    _write_tsv(OUT_DIR / "sample_noheader.tsv", [])


def generate_tsv_with_types() -> None:
    """TSVWithNamesAndTypes sample."""
    _write_tsv(OUT_DIR / "sample_withtypes.tsv", ["\t".join(COLUMNS), "\t".join(TYPES)])


def generate_jsonl() -> None:
    """JSONEachRow sample (newline-delimited JSON)."""
    with open(OUT_DIR / "sample.jsonl", "w") as f:
        for row in ROWS:
            f.write(json.dumps(dict(zip(COLUMNS, row))) + "\n")


def generate_json_compact_each_row() -> None:
    """JSONCompactEachRow sample (one JSON array per line)."""
    with open(OUT_DIR / "sample_compact.jsonl", "w") as f:
        for row in ROWS:
            f.write(json.dumps(list(row)) + "\n")


def generate_json() -> None:
    """JSON sample (full ClickHouse-style JSON envelope with metadata)."""
    payload = {
        "meta": [{"name": col, "type": typ} for col, typ in zip(COLUMNS, TYPES)],
        "data": [dict(zip(COLUMNS, row)) for row in ROWS],
        "rows": NUM_ROWS,
    }
    with open(OUT_DIR / "sample.json", "w") as f:
        json.dump(payload, f)


def generate_parquet() -> None:
    pq.write_table(_arrow_table(), OUT_DIR / "sample.parquet")


def generate_orc() -> None:
    orc.write_table(_arrow_table(), OUT_DIR / "sample.orc")


def generate_avro() -> None:
    """Generate an Avro container by round-tripping through chdb.

    Avro files cannot be appended to, so any prior copy is removed first;
    chdb would otherwise raise ``CANNOT_APPEND_TO_FILE``.
    """
    avro_path = OUT_DIR / "sample.avro"
    avro_path.unlink(missing_ok=True)
    session = get_shared_session()
    table = _arrow_table()  # noqa: F841 — referenced by chdb's Python() table function
    safe_path = str(avro_path).replace("'", "\\'")
    session.query(
        f"INSERT INTO FUNCTION file('{safe_path}', 'Avro') "
        "SELECT * FROM Python(table)"
    )


if __name__ == "__main__":
    generate_csv()
    generate_csv_no_header()
    generate_csv_with_types()
    generate_tsv()
    generate_tsv_no_header()
    generate_tsv_with_types()
    generate_jsonl()
    generate_json_compact_each_row()
    generate_json()
    generate_parquet()
    generate_orc()
    generate_avro()
    print(f"Generated {NUM_ROWS}-row sample files in {OUT_DIR}")
