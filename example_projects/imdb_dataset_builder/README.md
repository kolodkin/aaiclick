IMDb Dataset Builder
---

Large-scale data curation pipeline that loads IMDb title.basics (~10M rows) from the official dataset URL, profiles raw data, filters to quality movies (1950+, 40–300 min runtime, non-adult), normalizes genres via explode, enriches each title with Wikipedia plot text via Wikidata `P345` title resolution plus an `AggregatingMergeTree` merge against the Hugging Face `wikimedia/wikipedia` Parquet dump, and optionally publishes a curated Parquet dataset to Hugging Face. It demonstrates URL ingestion, `explode`, `AggregatingMergeTree` merges and a multi-task `@job` DAG; point `AAICLICK_CH_URL` / `AAICLICK_SQL_URL` at a cluster to run it distributed.

```bash
# Full dataset (~12.6M rows, default)
./imdb_dataset_builder.sh

# Quick sample (500k rows)
./imdb_dataset_builder.sh --sample

# Against a ClickHouse + PostgreSQL cluster
AAICLICK_CH_URL=... AAICLICK_SQL_URL=... ./imdb_dataset_builder.sh --sample
```

Publishing is opt-in. Pass `--publish` (sets `publish_hf=True`) with `HF_TOKEN` set to publish the curated dataset to Hugging Face Hub — the flag requires the token, so the pipeline fails fast if it is unset. Pass `--airtable` (sets `publish_airtable=True`) with `AIRTABLE_API_KEY` + `AIRTABLE_BASE_ID` (table defaults to `IMDB`) to publish a ~200-row stratified-by-genre showcase sample to Airtable.
