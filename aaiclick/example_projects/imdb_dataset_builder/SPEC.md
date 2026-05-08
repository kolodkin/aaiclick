IMDb Dataset Builder — Design Notes
---

# Why `AggregatingMergeTree` instead of `Object.join()`?

The Wikipedia enrichment chain uses a two-stage `AggregatingMergeTree` merge (`insert()` both sources → `group_by(key).agg(any)`) rather than the equivalent `Object.join()`. For this 2-way key merge a hash join would actually be faster — the ~30 k-row right sides fit trivially in a RAM probe and skip two write-merge-read cycles.

The current choice is didactic: `AggregatingMergeTree` + `any()` scales symmetrically to N sources with mixed schemas, so adding a third enrichment feed (e.g. TMDB overviews, IMDb ratings) is a one-line extra `insert()` instead of another chained join. Reach for `Object.join()` when the merge is strictly 2-way and speed matters more than extensibility.
