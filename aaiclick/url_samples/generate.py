#!/usr/bin/env python3
"""
Generate sample data files for URL integration tests.

Requires pyarrow: pip install pyarrow

Usage:
    python -m aaiclick.url_samples.generate

Produces 200 rows with columns (id: int, price: float, name: string) in:
    sample.csv     - CSVWithNames (header + comma-separated)
    sample.tsv     - TSVWithNames (header + tab-separated)
    sample.jsonl   - JSONEachRow  (one JSON object per line)
    sample.parquet - Apache Parquet
    sample.orc     - Apache ORC
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


def generate_csv() -> None:
    with open(OUT_DIR / "sample.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "price", "name"])
        for i in range(NUM_ROWS):
            writer.writerow([ids[i], prices[i], names[i]])


def generate_tsv() -> None:
    with open(OUT_DIR / "sample.tsv", "w") as f:
        f.write("id\tprice\tname\n")
        for i in range(NUM_ROWS):
            f.write(f"{ids[i]}\t{prices[i]}\t{names[i]}\n")


def generate_jsonl() -> None:
    with open(OUT_DIR / "sample.jsonl", "w") as f:
        for i in range(NUM_ROWS):
            f.write(json.dumps({"id": ids[i], "price": prices[i], "name": names[i]}) + "\n")


def generate_parquet() -> None:
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "price": pa.array(prices, type=pa.float64()),
            "name": pa.array(names, type=pa.string()),
        }
    )
    pq.write_table(table, OUT_DIR / "sample.parquet")


def generate_orc() -> None:
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "price": pa.array(prices, type=pa.float64()),
            "name": pa.array(names, type=pa.string()),
        }
    )
    orc.write_table(table, OUT_DIR / "sample.orc")


if __name__ == "__main__":
    generate_csv()
    generate_tsv()
    generate_jsonl()
    generate_parquet()
    generate_orc()
    print(f"Generated {NUM_ROWS}-row sample files in {OUT_DIR}")
