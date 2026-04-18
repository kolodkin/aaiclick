IMDb Dataset Builder
---

Large-scale data curation pipeline that loads IMDb title.basics (~10M rows) from the official dataset URL, profiles raw data, filters to quality movies (1980+, 40–300 min runtime, non-adult), normalizes genres via explode, optionally enriches each title with Wikipedia plot text via Wikidata `P345` title resolution plus an `AggregatingMergeTree` join against the Hugging Face `wikimedia/wikipedia` Parquet dump, and optionally publishes a curated Parquet dataset to Hugging Face.

```bash
# Demo mode (500k rows)
./imdb_dataset_builder.sh

# Full dataset (~10M rows)
./imdb_dataset_builder.sh --full

# Enable Wikipedia plot enrichment
IMDB_ENRICH=wikipedia ./imdb_dataset_builder.sh
```

Set `HF_TOKEN` to publish the curated dataset to Hugging Face Hub.
