IMDb Dataset Builder
---

Large-scale data curation pipeline that loads IMDb title.basics (~10M rows) from the official dataset URL, profiles raw data, filters to quality movies (1980+, 40–300 min runtime, non-adult), normalizes genres via explode, enriches each title with Wikipedia plot text via Wikidata `P345` title resolution plus an `AggregatingMergeTree` merge against the Hugging Face `wikimedia/wikipedia` Parquet dump, and optionally publishes a curated Parquet dataset to Hugging Face.

```bash
# Demo mode (500k rows)
./imdb_dataset_builder.sh

# Full dataset (~10M rows)
./imdb_dataset_builder.sh --full
```

Set `HF_TOKEN` to publish the curated dataset to Hugging Face Hub.

### Design note — why `AggregatingMergeTree` instead of `Object.join()`?

The enrichment chain uses a two-stage `AggregatingMergeTree` merge (`insert()` both sources → `group_by(key).agg(any)`) rather than the equivalent `Object.join()`. For this 2-way key merge a hash join would actually be faster — the ~30 k-row right sides fit trivially in a RAM probe and skip two write-merge-read cycles. The current choice is didactic: `AggregatingMergeTree` + `any()` scales symmetrically to N sources with mixed schemas, so adding a third enrichment feed (e.g. TMDB overviews, IMDb ratings) is a one-line extra `insert()` instead of another chained join. Reach for `Object.join()` when the merge is strictly 2-way and speed matters more than extensibility.
