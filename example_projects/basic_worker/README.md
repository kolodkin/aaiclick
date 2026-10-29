Basic Worker
---

A minimal orchestration example that registers a job printing 6 ticks at 0.5-second intervals, then executes it via a worker. Runs in distributed mode: ClickHouse + PostgreSQL servers named by `AAICLICK_CH_URL` / `AAICLICK_SQL_URL`.

```bash
AAICLICK_CH_URL=clickhouse://... AAICLICK_SQL_URL=postgresql+asyncpg://... ./basic_worker.sh
```
