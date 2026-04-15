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

NUM_ROWS = 200
OUT_DIR = Path(__file__).parent

ids = list(range(1, NUM_ROWS + 1))
prices = [round(i * 1.5, 2) for i in range(1, NUM_ROWS + 1)]
names = [f"item_{i}" for i in range(1, NUM_ROWS + 1)]


def _arrow_table() -> pa.Table:
    return pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "price": pa.array(prices, type=pa.float64()),
        "name": pa.array(names, type=pa.string()),
    })


def generate_csv() -> None:
    """CSVWithNames sample (with header)."""
    with open(OUT_DIR / "sample.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "price", "name"])
        for i in range(NUM_ROWS):
            writer.writerow([ids[i], prices[i], names[i]])


def generate_csv_no_header() -> None:
    """Plain CSV sample (no header — for the ``CSV`` format)."""
    with open(OUT_DIR / "sample_noheader.csv", "w", newline="") as f:
        writer = csv.writer(f)
        for i in range(NUM_ROWS):
            writer.writerow([ids[i], prices[i], names[i]])


def generate_csv_with_types() -> None:
    """CSVWithNamesAndTypes sample (header row + types row)."""
    with open(OUT_DIR / "sample_withtypes.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "price", "name"])
        writer.writerow(["Int64", "Float64", "String"])
        for i in range(NUM_ROWS):
            writer.writerow([ids[i], prices[i], names[i]])


def generate_tsv() -> None:
    """TSVWithNames sample (with header)."""
    with open(OUT_DIR / "sample.tsv", "w") as f:
        f.write("id\tprice\tname\n")
        for i in range(NUM_ROWS):
            f.write(f"{ids[i]}\t{prices[i]}\t{names[i]}\n")


def generate_tsv_no_header() -> None:
    """Plain TSV sample (no header)."""
    with open(OUT_DIR / "sample_noheader.tsv", "w") as f:
        for i in range(NUM_ROWS):
            f.write(f"{ids[i]}\t{prices[i]}\t{names[i]}\n")


def generate_tsv_with_types() -> None:
    """TSVWithNamesAndTypes sample."""
    with open(OUT_DIR / "sample_withtypes.tsv", "w") as f:
        f.write("id\tprice\tname\n")
        f.write("Int64\tFloat64\tString\n")
        for i in range(NUM_ROWS):
            f.write(f"{ids[i]}\t{prices[i]}\t{names[i]}\n")


def generate_jsonl() -> None:
    """JSONEachRow sample (newline-delimited JSON)."""
    with open(OUT_DIR / "sample.jsonl", "w") as f:
        for i in range(NUM_ROWS):
            f.write(json.dumps({"id": ids[i], "price": prices[i], "name": names[i]}) + "\n")


def generate_json_compact_each_row() -> None:
    """JSONCompactEachRow sample (one JSON array per line)."""
    with open(OUT_DIR / "sample_compact.jsonl", "w") as f:
        for i in range(NUM_ROWS):
            f.write(json.dumps([ids[i], prices[i], names[i]]) + "\n")


def generate_json() -> None:
    """JSON sample (full ClickHouse-style JSON envelope with metadata)."""
    payload = {
        "meta": [
            {"name": "id", "type": "Int64"},
            {"name": "price", "type": "Float64"},
            {"name": "name", "type": "String"},
        ],
        "data": [
            {"id": ids[i], "price": prices[i], "name": names[i]}
            for i in range(NUM_ROWS)
        ],
        "rows": NUM_ROWS,
    }
    with open(OUT_DIR / "sample.json", "w") as f:
        json.dump(payload, f)


def generate_parquet() -> None:
    pq.write_table(_arrow_table(), OUT_DIR / "sample.parquet")


def generate_orc() -> None:
    orc.write_table(_arrow_table(), OUT_DIR / "sample.orc")


def generate_avro() -> None:
    """Generate an Avro container by round-tripping through chdb."""
    from chdb.session import Session

    session = Session()
    try:
        table = _arrow_table()  # noqa: F841 — referenced via Python() table function
        avro_path = OUT_DIR / "sample.avro"
        avro_path_str = str(avro_path).replace("'", "\\'")
        session.query(
            f"INSERT INTO FUNCTION file('{avro_path_str}', 'Avro') "
            "SELECT * FROM Python(table)"
        )
    finally:
        session.cleanup()


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
