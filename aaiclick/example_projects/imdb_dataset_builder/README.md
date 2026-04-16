IMDb Dataset Builder
---

Large-scale data curation pipeline that loads IMDb title.basics (~10M rows) from the official dataset URL, profiles raw data, filters to quality movies, normalizes genres via explode, and optionally publishes a curated Parquet dataset to Hugging Face.

```bash
# Demo mode (500k rows)
./imdb_dataset_builder.sh

# Full dataset (~10M rows)
./imdb_dataset_builder.sh --full
```

Set `HF_TOKEN` to publish the curated dataset to Hugging Face Hub.
