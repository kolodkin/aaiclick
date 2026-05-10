IMDb Dataset Builder — Design Notes
---

# Why `AggregatingMergeTree` instead of `Object.join()`?

The Wikipedia enrichment chain uses a two-stage `AggregatingMergeTree` merge (`insert()` both sources → `group_by(key).agg(any)`) rather than the equivalent `Object.join()`. For this 2-way key merge a hash join would actually be faster — the ~30 k-row right sides fit trivially in a RAM probe and skip two write-merge-read cycles.

The current choice is didactic: `AggregatingMergeTree` + `any()` scales symmetrically to N sources with mixed schemas, so adding a third enrichment feed (e.g. TMDB overviews, IMDb ratings) is a one-line extra `insert()` instead of another chained join. Reach for `Object.join()` when the merge is strictly 2-way and speed matters more than extensibility.

# Planned: `genres` as `Array(String)` + Airtable showcase publishing

Status: ⚠️ NOT YET IMPLEMENTED (design dated 2026-05-08)

Two paired changes shipped together: an upstream cleanup that converts `genres` from a comma-separated `String` into a native `Array(String)`, and a new `publish_to_airtable` task that uploads a stratified-by-genre showcase sample to a configured Airtable base.

## Motivation

`genres` enters the pipeline as a comma-separated string from the IMDb TSV (`"Drama,Romance,War"`). Today every consumer that needs to filter or pivot by genre re-implements the comma-split:

- `normalize_genres` calls `with_split_by_char` then `explode`
- `build_clean_dataset` filters Adult titles via `match(genres, 'Adult') = 0`
- The new Airtable sample task would have to do the same again

A one-time conversion to `Array(String)` in `build_clean_dataset` lets every downstream consumer use first-class array operators (`has(genres, 'Drama')`, `arrayJoin(genres)`, `length(genres)`), and the HF parquet output gains a proper `list<string>` column instead of strings to be re-parsed.

## Refactor: `genres` as `Array(String)`

Move the comma-split from `normalize_genres` (which currently does `with_split_by_char` then `explode`) up into `filter_movies`, so every downstream task sees `genres` as `Array(String)`:

```python
@task
async def filter_movies(raw: Object) -> Object:
    movies = (
        raw.where("titleType = 'movie'")
           .where("isAdult = '0'")
           .where(r"genres != '\N'")
           .where(r"startYear != '\N'")
           .with_columns({"genres_arr": Computed("Array(String)", "splitByChar(',', genres)")})
           .rename({"genres": "genres_raw", "genres_arr": "genres"})
           [["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
             "startYear", "endYear", "runtimeMinutes", "genres"]]
    )
    return await movies.copy()
```

Then in `build_clean_dataset` the Adult filter and column projection use the array form:

```python
clean = (
    typed.where("runtime_int >= 40")
         .where("runtime_int <= 300")
         .where("year_int >= 1980")
         .where("has(genres, 'Adult') = 0")
         [["tconst", "primaryTitle", "startYear", "genres", "runtimeMinutes"]]
)
return await clean.copy()
```

Touch points:

| File / symbol                                        | Change                                                                          |
|------------------------------------------------------|---------------------------------------------------------------------------------|
| `__init__.py` `filter_movies`                        | Adds `splitByChar` Computed + rename, so `movies.genres` is `Array(String)`     |
| `__init__.py` `normalize_genres`                     | Drops `with_split_by_char`; becomes `movies.explode("genres")`                  |
| `__init__.py` `build_clean_dataset`                  | Adult filter via `has(genres, 'Adult')` (was `match(genres, 'Adult')`)          |
| `wikipedia.py` `_STAGE_A_COLUMNS`, `_STAGE_B_COLUMNS`| `genres` → `ColumnInfo("Array(String)", nullable=True)`                         |
| `constants.py` `CLEAN_COLUMNS`                       | `genres` → `ColumnInfo("Array(String)", description="Genre tags (no Adult)")`   |
| `report.py`                                          | No change — `Object.markdown()` renders arrays as `['Drama','Romance']`         |
| `__init__.py` `publish_to_huggingface`               | No change — pyarrow handles `list<string>` natively in parquet                  |
| `extract_plot_text`                                  | No change — `genres` is passthrough                                             |

Note: this preserves both demonstrated idioms — `with_split_by_char` runs once in `filter_movies` (split-into-array demo), and `explode` runs in `normalize_genres` (array-explode demo).

## Airtable sample task

New module `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/airtable.py`:

```python
@task
async def sample_for_airtable(plots: Object, genre_balance: Object) -> Object:
    """Stratified-by-genre showcase: top 10 per genre by plot length, deduped by tconst."""
    genres = await genre_balance["genre"].data()
    sample = await create_object(Schema(fieldtype=FIELDTYPE_ARRAY, columns=_SAMPLE_COLUMNS))
    for g in genres:
        top = await (
            plots.where(f"has(genres, {repr(g)})")
                 .view(order_by="length(plot) DESC", limit=10)
                 .copy()
        )
        await sample.insert(top)
    return await sample.group_by("tconst").agg({c: GB_ANY for c in _SAMPLE_NON_KEY_COLUMNS})


@task
async def publish_to_airtable(sample: Object) -> AirtablePublishResult:
    api_key = os.environ.get("AIRTABLE_API_KEY")
    base_id = os.environ.get("AIRTABLE_BASE_ID")
    table   = os.environ.get("AIRTABLE_TABLE_NAME", "IMDB")
    if not (api_key and base_id):
        return AirtablePublishResult(status="skipped", reason="AIRTABLE_API_KEY/BASE_ID not set")

    rows = await sample.data(orient=ORIENT_DICT)
    records = [{"fields": {col: rows[col][i] for col in rows}} for i in range(len(rows["tconst"]))]

    existing = _airtable_list_all(api_key, base_id, table)
    for batch in _chunks(existing, 10):
        _airtable_delete(api_key, base_id, table, batch)
        await asyncio.sleep(0.2)  # 5 req/sec cap

    for batch in _chunks(records, 10):
        _airtable_create(api_key, base_id, table, batch)
        await asyncio.sleep(0.2)

    return AirtablePublishResult(status="published", rows=len(records), base=base_id, table=table)
```

`_airtable_list_all`, `_airtable_delete`, `_airtable_create`: plain `urllib.request` GET/DELETE/POST calls to `https://api.airtable.com/v0/{base}/{table}`, mirroring the SPARQL helper style in `wikipedia.py`. No `pyairtable` dependency added.

## Selection strategy

- **Stratify**: for each distinct genre in `genre_balance`, take the 10 rows from `plots` with longest `plot` text.
- **Dedupe**: a film tagged `Drama,Romance,War` lands in 3 buckets; final `group_by(tconst).agg(any)` collapses to one row.
- **Expected size**: ~150–200 rows (after dedup across ~20 genres).

## DAG wiring (`__init__.py`)

```python
airtable_sample = sample_for_airtable(plots=plots, genre_balance=genre_balance)
airtable_result = publish_to_airtable(sample=airtable_sample)

return generate_report(
    ...,
    airtable_result=airtable_result,
)
```

`airtable_result` flows into the report alongside `hf_result`.

## New model (`models.py`)

```python
class AirtablePublishResult(BaseModel):
    status: str            # "published" | "skipped" | "error"
    base: str | None = None
    table: str | None = None
    rows: int | None = None
    reason: str | None = None
```

## Field-type mapping (Airtable side)

The user pre-creates the Airtable table with these fields:

| ClickHouse type     | Field name        | Airtable field type      |
|---------------------|-------------------|--------------------------|
| `String`            | `tconst`          | Single line text (key)   |
| `String`            | `primaryTitle`    | Single line text         |
| `String`            | `startYear`       | Single line text         |
| `Array(String)`     | `genres`          | Multiple select          |
| `String`            | `runtimeMinutes`  | Single line text         |
| `String`            | `wp_title`        | Single line text         |
| `String`            | `plot`            | Long text                |

The Airtable `Multiple select` field accepts a JSON array directly via the API — no client-side join needed.

## Environment variables

| Var                    | Default     | Purpose                                            |
|------------------------|-------------|----------------------------------------------------|
| `AIRTABLE_API_KEY`     | (required)  | Personal access token (gates upload)               |
| `AIRTABLE_BASE_ID`     | (required)  | Base id (`appXXXXXXXX`)                            |
| `AIRTABLE_TABLE_NAME`  | `IMDB`      | Table name within the base                         |

`AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID` are required; missing either → task returns `AirtablePublishResult(status="skipped", reason="...")` without raising. Mirrors the `HF_TOKEN` gating pattern already used by `publish_to_huggingface`. `AIRTABLE_TABLE_NAME` defaults to `IMDB` — Airtable accepts table names directly in the API URL, and `IMDB` is the recommended convention for this pipeline. No default is provided for `AIRTABLE_BASE_ID` because Airtable's REST API rejects anything that isn't a real `app...` id.

**Run environment:** the secrets are pre-configured in the **DEV** environment. Cloud / CI invocations must select DEV (e.g. via the worker's environment selector) so `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID` are exported into the task process. Local runs export them manually:

```bash
export AIRTABLE_API_KEY="patXXXXXXXXXXXXXX..."
export AIRTABLE_BASE_ID="appXXXXXXXXXXXXXX"
./imdb_dataset_builder.sh
```

## Failure / rate-limit handling

Airtable rate limit: 5 req/sec per base.

- 200 records / 10 per batch = 20 create requests + ~20 delete requests = ~40 total.
- At 0.2 s between calls → ~8 s of API time, well within typical task timeouts.
- `429 Too Many Requests` → exponential backoff (2 s → 4 s → 8 s, max 3 retries), then fail the task.
- Other 4xx → fail immediately, including the response body in the error.
- Network errors → 3 retries with the same backoff schedule.

## Upload mode

**Replace contents.** Each pipeline run wipes the configured table and re-inserts the new sample. The table is dedicated to this pipeline; reviewers always see the latest run.

## Testing

No new test file — example projects don't carry unit tests in this repo. Verification:

- Smoke test: run `./imdb_dataset_builder.sh` with `AIRTABLE_*` set against a throwaway base, confirm 150–200 rows land with correct field mapping.
- Refactor regression: `genre_balance` row count, HF publish row count, and the report's "Genre Distribution" section stay byte-identical to the pre-refactor run.

