chdb Benchmark
---

Compares aaiclick's Object API against native chdb SQL on identical data (1M rows, 10 runs averaged). Measures ingest, column sum, multiply, filter, sort, count distinct, and group-by operations.

# How to Run

```bash
./aaiclick/example_projects/chdb_benchmark/chdb_benchmark.sh

# Custom parameters
./aaiclick/example_projects/chdb_benchmark/chdb_benchmark.sh --rows 500000 --runs 5
```
