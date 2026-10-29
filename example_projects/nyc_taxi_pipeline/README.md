NYC Taxi Pipeline
---

Distributed computing example that loads NYC TLC Yellow Taxi trip data from Parquet URLs, runs parallel analysis tasks (basic stats, statistical metrics, group-by, tip and distance analysis), and produces a summary report. Runs in distributed mode: ClickHouse + PostgreSQL servers named by `AAICLICK_CH_URL` / `AAICLICK_SQL_URL`.

```bash
AAICLICK_CH_URL=clickhouse://... AAICLICK_SQL_URL=postgresql+asyncpg://... ./nyc_taxi_pipeline.sh

# Override job kwargs
./nyc_taxi_pipeline.sh --set limit=500000
```
