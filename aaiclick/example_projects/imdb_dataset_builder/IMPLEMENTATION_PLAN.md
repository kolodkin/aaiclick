# IMDb genres-Array + Airtable Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert `genres` to native `Array(String)` end-to-end and add a `publish_to_airtable` task that uploads a stratified-by-genre showcase sample to a pre-configured Airtable base.

**Architecture:** Two paired changes shipped in two phases. Phase 1 moves the comma-split from `normalize_genres` up into `filter_movies`, so `genres` flows downstream as `Array(String)`. Phase 2 adds a new `airtable.py` module with two `@task`s — `sample_for_airtable` (per-genre top-N by plot length, then dedupe) and `publish_to_airtable` (replace-mode REST upload with rate-limit handling) — and wires them into the DAG.

**Tech Stack:** aaiclick `Object` API (Computed columns, `has()`, `splitByChar`, `explode`, `group_by/agg`, `AggregatingMergeTree`), Python `urllib.request` (no `pyairtable` dep), Pydantic `BaseModel`, GitHub Actions for verification.

**Spec reference:** `aaiclick/example_projects/imdb_dataset_builder/SPEC.md` — section "Planned: `genres` as `Array(String)` + Airtable showcase publishing" (as of commit `bad5c39`).

**Validation note:** The IMDb example project intentionally has no unit-test suite (per `example_projects/CLAUDE.md` and the spec). Verification of each phase is the existing CI workflow `project-imdb-dataset-builder.yaml`, run via `./.claude/skills/action-run/run.sh project-imdb-dataset-builder.yaml`. The pipeline output (`tmp/imdb_report.md` summary in the workflow log) is the regression check.

---

## File Structure

| File                                                                                       | Phase | Responsibility                                                              |
|--------------------------------------------------------------------------------------------|-------|-----------------------------------------------------------------------------|
| `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/__init__.py`          | 1+2   | Tasks `filter_movies`, `normalize_genres`, `build_clean_dataset`; DAG wiring |
| `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/wikipedia.py`         | 1     | Stage-A/B column schemas (`genres` → `Array(String)`)                       |
| `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/constants.py`         | 1     | `CLEAN_COLUMNS["genres"]` type + description                                |
| `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/models.py`            | 2     | Add `AirtablePublishResult`                                                 |
| `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/airtable.py`          | 2     | New module: `sample_for_airtable`, `publish_to_airtable`, urllib helpers    |
| `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/report.py`            | 2     | Add Airtable section to report                                              |
| `aaiclick/example_projects/imdb_dataset_builder/README.md`                                 | 2     | Mention `AIRTABLE_*` env vars in run-instructions paragraph                 |

`report.py` is touched in Phase 2 only; `Object.markdown()` already renders `Array(String)` as `['Drama','Romance']` so no Phase-1 report change is needed.

---

# Phase 1 — `genres` as `Array(String)`

## Task 1: Refactor `filter_movies` to produce `genres` as `Array(String)`

**Files:**
- Modify: `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/__init__.py` — function `filter_movies`

- [ ] **Step 1: Add `Computed` to the import line for `aaiclick.data.models`**

The current import at the top of `__init__.py` is:

```python
from aaiclick.data.models import ColumnInfo
```

Change to:

```python
from aaiclick.data.models import ColumnInfo, Computed
```

- [ ] **Step 2: Replace the body of `filter_movies`**

Find:

```python
@task
async def filter_movies(raw: Object) -> Object:
    """
    Filter to non-adult movies with known genres and start year.

    All four conditions are pushed down as SQL WHERE clauses — ClickHouse
    executes them as a single filtered SELECT. The result is materialized
    via .copy() into a new table for downstream parallel tasks.
    """
    movies = raw.where("titleType = 'movie'")
    movies = movies.where("isAdult = '0'")
    movies = movies.where(r"genres != '\N'")
    movies = movies.where(r"startYear != '\N'")
    return await movies.copy()
```

Replace with:

```python
@task
async def filter_movies(raw: Object) -> Object:
    """
    Filter to non-adult movies with known genres and start year, and convert
    ``genres`` from a comma-separated ``String`` into a native ``Array(String)``.

    All filter conditions push down as SQL WHERE clauses — ClickHouse
    executes them as a single filtered SELECT. The genres conversion uses
    a Computed column with ``splitByChar(',', genres)``; downstream tasks
    can then use first-class array operators (``has``, ``arrayJoin``, ...).
    """
    movies = (
        raw.where("titleType = 'movie'")
           .where("isAdult = '0'")
           .where(r"genres != '\N'")
           .where(r"startYear != '\N'")
           .with_columns({"genres_arr": Computed("Array(String)", "splitByChar(',', genres)")})
    )
    movies = movies.rename({"genres": "genres_raw", "genres_arr": "genres"})
    movies = movies[
        [
            "tconst", "titleType", "primaryTitle", "originalTitle",
            "isAdult", "startYear", "endYear", "runtimeMinutes", "genres",
        ]
    ]
    return await movies.copy()
```

- [ ] **Step 3: Commit**

```bash
git add aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/__init__.py
git commit -m "$(cat <<'EOF'
refactor: convert genres to Array(String) in filter_movies

Move the comma-split from normalize_genres up into filter_movies so
every downstream task (normalize_genres, build_clean_dataset, the
Wikipedia enrichment chain, the Airtable sample) sees genres as a
native Array(String) instead of re-implementing comma-string parsing.
EOF
)"
```

---

## Task 2: Simplify `normalize_genres` (drop `with_split_by_char`)

**Files:**
- Modify: `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/__init__.py` — function `normalize_genres`

- [ ] **Step 1: Replace the body of `normalize_genres`**

Find:

```python
@task
async def normalize_genres(movies: Object) -> Object:
    """
    Explode comma-separated genres into one row per genre.

    Uses splitByChar(',', genres) to create an Array column, then
    explode() to produce one row per genre. Adult genre entries are
    filtered out. Result is materialized for downstream analysis.
    """
    exploded = movies.with_split_by_char("genres", ",", element_type="LowCardinality(String)", alias="genre").explode(
        "genre"
    )
    return await exploded.copy()
```

Replace with:

```python
@task
async def normalize_genres(movies: Object) -> Object:
    """
    Explode the ``genres`` array into one row per genre.

    ``filter_movies`` already converts ``genres`` to ``Array(String)``,
    so this task is a single ``explode`` on the existing array column.
    """
    exploded = movies.explode("genres")
    return await exploded.copy()
```

- [ ] **Step 2: Verify `analyze_genre_balance` still works against the exploded shape**

Read the current `analyze_genre_balance` body:

```python
@task
async def analyze_genre_balance(exploded: Object) -> Object:
    return await exploded.group_by("genre").agg({"tconst": "count"})
```

It groups by `"genre"` — but `explode("genres")` keeps the column name `genres`, not `genre`. Update the body to match:

Find:

```python
@task
async def analyze_genre_balance(exploded: Object) -> Object:
    """
    Compute genre distribution across all movies.

    Groups by genre, counts titles per genre. Returns an Object with
    (genre, tconst_count) rows for the report.
    """
    return await exploded.group_by("genre").agg({"tconst": "count"})
```

Replace with:

```python
@task
async def analyze_genre_balance(exploded: Object) -> Object:
    """
    Compute genre distribution across all movies.

    Groups by the exploded ``genres`` element, counts titles per genre.
    Returns an Object with ``(genres, tconst_count)`` rows for the report.
    """
    return await exploded.group_by("genres").agg({"tconst": "count"})
```

- [ ] **Step 3: Update `report.py` to read the renamed grouping column**

Read the current relevant block in `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/report.py` (around the `genre_balance.rename(...)` call):

```python
    genre_with_pct = genre_balance.rename({"genre": "Genre", "tconst": "Count"}).with_columns(
        {
            "%": Computed("Float64", "round(Count * 100.0 / sum(Count) OVER(), 2)"),
        }
    )
    genre_md = await genre_with_pct.view(order_by="Count DESC", limit=50).markdown()
    genre_data_raw = await genre_balance.data()
    genre_distinct = len(genre_data_raw["genre"])
    genre_total = sum(genre_data_raw["tconst"])
```

Replace with:

```python
    genre_with_pct = genre_balance.rename({"genres": "Genre", "tconst": "Count"}).with_columns(
        {
            "%": Computed("Float64", "round(Count * 100.0 / sum(Count) OVER(), 2)"),
        }
    )
    genre_md = await genre_with_pct.view(order_by="Count DESC", limit=50).markdown()
    genre_data_raw = await genre_balance.data()
    genre_distinct = len(genre_data_raw["genres"])
    genre_total = sum(genre_data_raw["tconst"])
```

- [ ] **Step 4: Commit**

```bash
git add aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/__init__.py \
        aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/report.py
git commit -m "$(cat <<'EOF'
refactor: simplify normalize_genres to a single explode on Array(String)

filter_movies now produces genres as Array(String), so normalize_genres
no longer needs with_split_by_char. analyze_genre_balance and the
report's genre-distribution rendering are updated to use the renamed
"genres" grouping column.
EOF
)"
```

---

## Task 3: Switch the Adult-genre filter in `build_clean_dataset` to `has(...)`

**Files:**
- Modify: `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/__init__.py` — function `build_clean_dataset`

- [ ] **Step 1: Replace the Adult filter line**

Find:

```python
    clean = clean.where("year_int >= 1980")
    clean = clean.where("match(genres, 'Adult') = 0")
```

Replace with:

```python
    clean = clean.where("year_int >= 1980")
    clean = clean.where("has(genres, 'Adult') = 0")
```

- [ ] **Step 2: Commit**

```bash
git add aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/__init__.py
git commit -m "$(cat <<'EOF'
refactor: filter Adult genre via has() instead of match() string regex

genres is now Array(String) thanks to filter_movies, so the Adult
exclusion uses the proper array operator.
EOF
)"
```

---

## Task 4: Update Wikipedia stage-table schemas to `Array(String)` for `genres`

**Files:**
- Modify: `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/wikipedia.py` — `_STAGE_A_COLUMNS` and `_STAGE_B_COLUMNS`

- [ ] **Step 1: Update both stage-column dicts**

Find:

```python
_STAGE_A_COLUMNS = {
    "tconst": ColumnInfo("String"),
    "primaryTitle": ColumnInfo("String", nullable=True),
    "startYear": ColumnInfo("String", nullable=True),
    "genres": ColumnInfo("String", nullable=True),
    "runtimeMinutes": ColumnInfo("String", nullable=True),
    "wp_title": ColumnInfo("String", nullable=True),
}

_STAGE_B_COLUMNS = {
    "wp_title": ColumnInfo("String"),
    "tconst": ColumnInfo("String", nullable=True),
    "primaryTitle": ColumnInfo("String", nullable=True),
    "startYear": ColumnInfo("String", nullable=True),
    "genres": ColumnInfo("String", nullable=True),
    "runtimeMinutes": ColumnInfo("String", nullable=True),
    "wiki_text": ColumnInfo("String", nullable=True),
}
```

Replace with:

```python
_STAGE_A_COLUMNS = {
    "tconst": ColumnInfo("String"),
    "primaryTitle": ColumnInfo("String", nullable=True),
    "startYear": ColumnInfo("String", nullable=True),
    "genres": ColumnInfo("Array(String)", nullable=True),
    "runtimeMinutes": ColumnInfo("String", nullable=True),
    "wp_title": ColumnInfo("String", nullable=True),
}

_STAGE_B_COLUMNS = {
    "wp_title": ColumnInfo("String"),
    "tconst": ColumnInfo("String", nullable=True),
    "primaryTitle": ColumnInfo("String", nullable=True),
    "startYear": ColumnInfo("String", nullable=True),
    "genres": ColumnInfo("Array(String)", nullable=True),
    "runtimeMinutes": ColumnInfo("String", nullable=True),
    "wiki_text": ColumnInfo("String", nullable=True),
}
```

- [ ] **Step 2: Commit**

```bash
git add aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/wikipedia.py
git commit -m "refactor: switch Wikipedia stage tables genres column to Array(String)"
```

---

## Task 5: Update `CLEAN_COLUMNS` description and type for `genres`

**Files:**
- Modify: `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/constants.py` — `CLEAN_COLUMNS["genres"]`

- [ ] **Step 1: Update the column entry**

Find:

```python
CLEAN_COLUMNS: dict[str, ColumnInfo] = {
    "tconst": ColumnInfo("String", description="IMDb title identifier (e.g. tt0000001)"),
    "primaryTitle": ColumnInfo("String", description="Popular title used for promotion"),
    "startYear": ColumnInfo("String", description="Release year (>= 1980)"),
    "genres": ColumnInfo("String", description="Comma-separated genres (no Adult)"),
    "runtimeMinutes": ColumnInfo("String", description="Runtime in minutes (40-300)"),
}
```

Replace with:

```python
CLEAN_COLUMNS: dict[str, ColumnInfo] = {
    "tconst": ColumnInfo("String", description="IMDb title identifier (e.g. tt0000001)"),
    "primaryTitle": ColumnInfo("String", description="Popular title used for promotion"),
    "startYear": ColumnInfo("String", description="Release year (>= 1980)"),
    "genres": ColumnInfo("Array(String)", description="Genre tags (no Adult)"),
    "runtimeMinutes": ColumnInfo("String", description="Runtime in minutes (40-300)"),
}
```

- [ ] **Step 2: Commit**

```bash
git add aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/constants.py
git commit -m "refactor: declare CLEAN_COLUMNS genres as Array(String) for the report schema"
```

---

## Task 6: Push and verify Phase 1 via the CI workflow

- [ ] **Step 1: Push the branch**

```bash
git push -u origin claude/review-imdb-example-SXAtK
```

- [ ] **Step 2: Run the CI workflow**

```bash
./.claude/skills/action-run/run.sh project-imdb-dataset-builder.yaml
```

Expected: workflow status `completed`, conclusion `success`. The "IMDb Dataset Builder" step's job summary shows the report; in the Genre Distribution table, the "Genre" column should show plain genre strings (one per row, since `analyze_genre_balance` operates on the exploded view) and totals should match the previous run within rounding.

- [ ] **Step 3: If the workflow fails**

Inspect the failed step output:

```bash
gh run view <RUN_ID> --repo kolodkin/aaiclick --log-failed
```

Common failure modes and fixes:

| Failure                                                               | Likely cause                                              | Fix                                                                                            |
|-----------------------------------------------------------------------|-----------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `KeyError: 'genre'` in `report.py`                                    | Missed the rename in Task 2 Step 3                         | Apply the rename and recommit.                                                                 |
| ClickHouse error `Cannot find column 'genres_raw'`                    | The column projection in Task 1 still references `genres_raw` | Drop `genres_raw` from the column-projection list in `filter_movies`.                          |
| ClickHouse error mentioning `match(genres, 'Adult')` and Array        | Task 3 not applied                                         | Apply Task 3 and recommit.                                                                     |
| Stage-A/B insert error `Type mismatch ... String vs Array(String)`    | Task 4 not applied (or applied to only one of the dicts)   | Re-apply Task 4 to both `_STAGE_A_COLUMNS` and `_STAGE_B_COLUMNS`, then recommit.              |

After fixing, repeat Step 1 + Step 2.

---

# Phase 2 — Airtable showcase publishing

## Task 7: Add `AirtablePublishResult` Pydantic model

**Files:**
- Modify: `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/models.py`

- [ ] **Step 1: Append the model**

Find the end of `models.py`:

```python
class EnrichmentStats(BaseModel):
    total_clean: int
    titles_resolved: int
    titles_resolved_pct: float
    articles_matched: int
    articles_matched_pct: float
    plots_usable: int
    plots_usable_pct: float
    avg_plot_chars: float
```

Append directly after:

```python


class AirtablePublishResult(BaseModel):
    status: str
    base: str | None = None
    table: str | None = None
    rows: int | None = None
    reason: str | None = None
```

- [ ] **Step 2: Commit**

```bash
git add aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/models.py
git commit -m "feature: add AirtablePublishResult model for the Airtable upload task"
```

---

## Task 8: Create `airtable.py` with helpers + `sample_for_airtable`

**Files:**
- Create: `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/airtable.py`

- [ ] **Step 1: Write the new module (helpers + sampling task)**

Create the file with this initial content (the `publish_to_airtable` task is added in Task 9):

```python
"""Airtable showcase publishing for the IMDb dataset builder.

Stratified-by-genre sample: top 10 rows per genre by plot length (longest
plots first), deduped by ``tconst``. The deduped sample (~150-200 rows)
is uploaded in *replace* mode: existing records are listed and deleted in
batches of 10, then the new sample is inserted in batches of 10. Airtable's
5 req/sec per-base rate limit is honored via ``asyncio.sleep(0.2)`` between
calls. ``urllib.request`` is used directly to keep the dependency set lean
(no ``pyairtable``).
"""

import asyncio
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterable, Iterator, Sequence

from aaiclick.data.data_context import create_object
from aaiclick.data.models import (
    ENGINE_AGGREGATING_MERGE_TREE,
    FIELDTYPE_ARRAY,
    GB_ANY,
    ColumnInfo,
    Schema,
)
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .models import AirtablePublishResult

AIRTABLE_API_BASE = "https://api.airtable.com/v0"
AIRTABLE_BATCH = 10  # Airtable max records per create / delete request
AIRTABLE_THROTTLE_SECONDS = 0.2  # 5 req/sec per base
AIRTABLE_BACKOFF_SECONDS = (2, 4, 8)
PER_GENRE_LIMIT = 10

_SAMPLE_COLUMNS: dict[str, ColumnInfo] = {
    "tconst": ColumnInfo("String"),
    "primaryTitle": ColumnInfo("String", nullable=True),
    "startYear": ColumnInfo("String", nullable=True),
    "genres": ColumnInfo("Array(String)", nullable=True),
    "runtimeMinutes": ColumnInfo("String", nullable=True),
    "wp_title": ColumnInfo("String", nullable=True),
    "plot": ColumnInfo("String", nullable=True),
}
_SAMPLE_NON_KEY_COLUMNS = [c for c in _SAMPLE_COLUMNS if c != "tconst"]


def _ch_quote(value: str) -> str:
    """Escape a string for safe interpolation inside a ClickHouse SQL literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _chunks(items: Sequence, size: int) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


@task
async def sample_for_airtable(plots: Object, genre_balance: Object) -> Object:
    """Build a stratified-by-genre showcase sample from ``plots``.

    For each distinct genre in ``genre_balance``, take the 10 rows from
    ``plots`` with the longest ``plot`` text. A film tagged ``Drama,Romance,War``
    lands in 3 buckets, so the result is then deduped on ``tconst`` via
    ``AggregatingMergeTree`` + ``group_by(tconst).agg(any)`` — the same
    idiom used by the Wikipedia enrichment chain. Final size: ~150-200 rows.
    """
    genres: list[str] = await genre_balance["genres"].data()

    sample = await create_object(
        Schema(
            fieldtype=FIELDTYPE_ARRAY,
            columns=_SAMPLE_COLUMNS,
            engine=ENGINE_AGGREGATING_MERGE_TREE,
            order_by="tconst",
        )
    )

    for g in genres:
        top = await (
            plots.where(f"has(genres, {_ch_quote(g)})")
                 .view(order_by="length(plot) DESC", limit=PER_GENRE_LIMIT)
                 .copy()
        )
        await sample.insert(top)

    deduped = await sample.group_by("tconst").agg({c: GB_ANY for c in _SAMPLE_NON_KEY_COLUMNS})
    return await deduped.copy()
```

- [ ] **Step 2: Commit**

```bash
git add aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/airtable.py
git commit -m "$(cat <<'EOF'
feature: add airtable.py with sample_for_airtable task

Per-genre top-10-by-plot-length sample, then dedup by tconst via the
AggregatingMergeTree + group_by/agg(any) idiom already used for the
Wikipedia enrichment. Pure aaiclick API; no new external dependencies.
EOF
)"
```

---

## Task 9: Add `publish_to_airtable` task + REST helpers to `airtable.py`

**Files:**
- Modify: `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/airtable.py`

- [ ] **Step 1: Add `time` to the standard-library imports**

Find the existing standard-library import block at the top of `airtable.py`:

```python
import asyncio
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterable, Iterator, Sequence
```

Add `import time` (alphabetical order, after `os`):

```python
import asyncio
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterable, Iterator, Sequence
```

- [ ] **Step 2: Import `ORIENT_DICT` next to the existing `aaiclick` imports**

Find:

```python
from aaiclick.data.data_context import create_object
```

Add directly above it:

```python
from aaiclick import ORIENT_DICT
```

- [ ] **Step 3: Append the REST helpers and the publish task**

Append to the end of `airtable.py`:

```python


def _airtable_request(
    method: str,
    url: str,
    api_key: str,
    *,
    body: dict | None = None,
) -> dict:
    """One Airtable REST call with exponential backoff on 429 / 5xx / network error.

    Runs synchronously; callers wrap it in ``asyncio.to_thread`` (see ``_arequest``).
    """
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    last_exc: Exception | None = None
    for backoff in (0,) + AIRTABLE_BACKOFF_SECONDS:
        if backoff:
            time.sleep(backoff)
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = resp.read().decode("utf-8")
                return json.loads(payload) if payload else {}
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="replace")
            if e.code == 429 or 500 <= e.code < 600:
                last_exc = RuntimeError(f"Airtable {e.code}: {body_text}")
                continue
            raise RuntimeError(f"Airtable {e.code}: {body_text}") from e
        except urllib.error.URLError as e:
            last_exc = e
            continue
    raise RuntimeError(f"Airtable request failed after retries: {last_exc}")


async def _arequest(method: str, url: str, api_key: str, *, body: dict | None = None) -> dict:
    """Async wrapper that runs the blocking request in a thread."""
    return await asyncio.to_thread(_airtable_request, method, url, api_key, body=body)


def _table_url(base_id: str, table: str) -> str:
    return f"{AIRTABLE_API_BASE}/{base_id}/{urllib.parse.quote(table, safe='')}"


async def _list_all_record_ids(api_key: str, base_id: str, table: str) -> list[str]:
    """Page through every record id in the table (Airtable returns up to 100/page)."""
    ids: list[str] = []
    offset: str | None = None
    while True:
        url = _table_url(base_id, table) + "?pageSize=100"
        if offset:
            url += "&offset=" + urllib.parse.quote(offset, safe="")
        payload = await _arequest("GET", url, api_key)
        ids.extend(rec["id"] for rec in payload.get("records", []))
        offset = payload.get("offset")
        if not offset:
            return ids
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)


async def _delete_records(api_key: str, base_id: str, table: str, ids: Iterable[str]) -> None:
    """Delete a batch of up to 10 record ids."""
    qs = "&".join(f"records[]={urllib.parse.quote(rid, safe='')}" for rid in ids)
    url = _table_url(base_id, table) + "?" + qs
    await _arequest("DELETE", url, api_key)


async def _create_records(api_key: str, base_id: str, table: str, records: list[dict]) -> None:
    """Create a batch of up to 10 records."""
    body = {"records": records, "typecast": True}
    await _arequest("POST", _table_url(base_id, table), api_key, body=body)


@task
async def publish_to_airtable(sample: Object) -> AirtablePublishResult:
    """Replace the configured Airtable table's contents with the showcase sample.

    Gating mirrors ``publish_to_huggingface``: ``AIRTABLE_API_KEY`` and
    ``AIRTABLE_BASE_ID`` are required; missing either returns a skipped
    result. ``AIRTABLE_TABLE_NAME`` defaults to ``"IMDB"``.
    """
    api_key = os.environ.get("AIRTABLE_API_KEY")
    base_id = os.environ.get("AIRTABLE_BASE_ID")
    table = os.environ.get("AIRTABLE_TABLE_NAME", "IMDB")
    if not (api_key and base_id):
        return AirtablePublishResult(
            status="skipped",
            reason="AIRTABLE_API_KEY/BASE_ID not set",
            table=table,
        )

    rows = await sample.data(orient=ORIENT_DICT)
    n = len(rows["tconst"])
    records = [
        {"fields": {col: rows[col][i] for col in _SAMPLE_COLUMNS}}
        for i in range(n)
    ]

    existing_ids = await _list_all_record_ids(api_key, base_id, table)
    for batch in _chunks(existing_ids, AIRTABLE_BATCH):
        await _delete_records(api_key, base_id, table, batch)
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)

    for batch in _chunks(records, AIRTABLE_BATCH):
        await _create_records(api_key, base_id, table, batch)
        await asyncio.sleep(AIRTABLE_THROTTLE_SECONDS)

    return AirtablePublishResult(
        status="published",
        base=base_id,
        table=table,
        rows=n,
    )
```

- [ ] **Step 4: Commit**

```bash
git add aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/airtable.py
git commit -m "$(cat <<'EOF'
feature: add publish_to_airtable task with replace-mode upload

REST helpers (list / delete / create) using urllib.request, batched
in groups of 10 with 0.2s throttle between calls (Airtable's 5 req/sec
per-base limit), exponential backoff on 429 / 5xx / network errors.
Gated on AIRTABLE_API_KEY + AIRTABLE_BASE_ID; AIRTABLE_TABLE_NAME
defaults to "IMDB".
EOF
)"
```

---

## Task 10: Wire Airtable tasks into the DAG and pass the result to the report

**Files:**
- Modify: `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/__init__.py`

- [ ] **Step 1: Import the new tasks**

Find the `from .wikipedia import (...)` block and add a sibling import directly after it:

```python
from .airtable import publish_to_airtable, sample_for_airtable
```

- [ ] **Step 2: Wire the two tasks into the pipeline**

Find this block in `imdb_dataset_pipeline`:

```python
    hf_result = publish_to_huggingface(enriched=plots) if os.environ.get("HF_TOKEN") else None

    export_formats = [f.strip().lower() for f in os.environ.get("IMDB_DATASET_EXPORTS", "").split(",") if f.strip()]
    exports = (
        export_dataset(enriched=plots, formats=export_formats, out_dir=os.environ.get("IMDB_OUT_DIR", "./tmp"))
        if export_formats
        else None
    )

    return generate_report(
```

Replace with:

```python
    hf_result = publish_to_huggingface(enriched=plots) if os.environ.get("HF_TOKEN") else None

    airtable_sample = sample_for_airtable(plots=plots, genre_balance=genre_balance)
    airtable_result = publish_to_airtable(sample=airtable_sample)

    export_formats = [f.strip().lower() for f in os.environ.get("IMDB_DATASET_EXPORTS", "").split(",") if f.strip()]
    exports = (
        export_dataset(enriched=plots, formats=export_formats, out_dir=os.environ.get("IMDB_OUT_DIR", "./tmp"))
        if export_formats
        else None
    )

    return generate_report(
```

- [ ] **Step 3: Pass `airtable_result` into `generate_report`**

Find the `generate_report(...)` call:

```python
    return generate_report(
        raw=raw,
        movies=movies,
        clean=clean,
        genre_balance=genre_balance,
        plots=plots,
        wiki=wiki,
        profile=profile,
        quality_issues=quality_issues,
        hf_result=hf_result,
        exports=exports,
        enrichment_stats=enrichment_stats,
    )
```

Add the new kwarg:

```python
    return generate_report(
        raw=raw,
        movies=movies,
        clean=clean,
        genre_balance=genre_balance,
        plots=plots,
        wiki=wiki,
        profile=profile,
        quality_issues=quality_issues,
        hf_result=hf_result,
        exports=exports,
        enrichment_stats=enrichment_stats,
        airtable_result=airtable_result,
    )
```

- [ ] **Step 4: Commit**

```bash
git add aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/__init__.py
git commit -m "feature: wire sample_for_airtable + publish_to_airtable into the IMDb DAG"
```

---

## Task 11: Render the Airtable result in the report

**Files:**
- Modify: `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/report.py`

- [ ] **Step 1: Import the new model**

Find:

```python
from .models import EnrichmentStats, HFPublishResult, QualityIssues, RawProfile
```

Replace with:

```python
from .models import AirtablePublishResult, EnrichmentStats, HFPublishResult, QualityIssues, RawProfile
```

- [ ] **Step 2: Add `airtable_result` to `ReportContent`**

Find the `ReportContent` dataclass:

```python
@dataclass
class ReportContent:
    """Pre-rendered report sections passed into ``_print_report``."""

    profile: RawProfile
    quality_issues: QualityIssues
    hf_result: HFPublishResult | None
    raw_md: str
    clean_md: str
    genre_md: str
    genre_distinct: int
    genre_total: int
    exports: dict[str, str] | None
    enrichment_stats: EnrichmentStats
    plots_md: str
    wiki_total: int
    wiki_sample_md: str
```

Add a field:

```python
@dataclass
class ReportContent:
    """Pre-rendered report sections passed into ``_print_report``."""

    profile: RawProfile
    quality_issues: QualityIssues
    hf_result: HFPublishResult | None
    airtable_result: AirtablePublishResult | None
    raw_md: str
    clean_md: str
    genre_md: str
    genre_distinct: int
    genre_total: int
    exports: dict[str, str] | None
    enrichment_stats: EnrichmentStats
    plots_md: str
    wiki_total: int
    wiki_sample_md: str
```

- [ ] **Step 3: Add the Airtable section in `_print_report`**

Find the end of `_print_report`:

```python
    print("\n### Published\n")
    if hf_result is None:
        print("- Skipped: HF_TOKEN not set")
        print(f"- Set `HF_TOKEN` to publish to: https://huggingface.co/datasets/{HF_REPO_ID}")
    elif hf_result.status == "published":
        print(f"- Hugging Face: https://huggingface.co/datasets/{hf_result.repo}")
        print(f"- Rows published: {_fmt(hf_result.rows)}")
    else:
        print(f"- Status: {hf_result.status}")
```

Append directly after:

```python

    airtable = content.airtable_result
    print("\n### Airtable Showcase\n")
    if airtable is None or airtable.status == "skipped":
        reason = airtable.reason if airtable else "task did not run"
        print(f"- Skipped: {reason}")
        print("- Set `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID` to publish a sample to Airtable")
    elif airtable.status == "published":
        print(f"- Base: `{airtable.base}` Table: `{airtable.table}`")
        print(f"- Rows published: {_fmt(airtable.rows)}")
    else:
        print(f"- Status: {airtable.status}")
        if airtable.reason:
            print(f"- Reason: {airtable.reason}")
```

- [ ] **Step 4: Update `generate_report` signature and `ReportContent` instantiation**

Find:

```python
@task
async def generate_report(
    raw: Object,
    movies: Object,
    clean: Object,
    genre_balance: Object,
    plots: Object,
    wiki: Object,
    profile: RawProfile,
    quality_issues: QualityIssues,
    enrichment_stats: EnrichmentStats,
    hf_result: HFPublishResult | None = None,
    exports: dict[str, str] | None = None,
) -> dict:
```

Replace with:

```python
@task
async def generate_report(
    raw: Object,
    movies: Object,
    clean: Object,
    genre_balance: Object,
    plots: Object,
    wiki: Object,
    profile: RawProfile,
    quality_issues: QualityIssues,
    enrichment_stats: EnrichmentStats,
    hf_result: HFPublishResult | None = None,
    exports: dict[str, str] | None = None,
    airtable_result: AirtablePublishResult | None = None,
) -> dict:
```

Find the `ReportContent(...)` construction:

```python
        _print_report(
            ReportContent(
                profile=profile,
                quality_issues=quality_issues,
                hf_result=hf_result,
                raw_md=raw_md,
                clean_md=clean_md,
                genre_md=genre_md,
                genre_distinct=genre_distinct,
                genre_total=genre_total,
                exports=exports,
                enrichment_stats=enrichment_stats,
                plots_md=plots_md,
                wiki_total=wiki_total,
                wiki_sample_md=wiki_sample_md,
            )
        )
```

Replace with:

```python
        _print_report(
            ReportContent(
                profile=profile,
                quality_issues=quality_issues,
                hf_result=hf_result,
                airtable_result=airtable_result,
                raw_md=raw_md,
                clean_md=clean_md,
                genre_md=genre_md,
                genre_distinct=genre_distinct,
                genre_total=genre_total,
                exports=exports,
                enrichment_stats=enrichment_stats,
                plots_md=plots_md,
                wiki_total=wiki_total,
                wiki_sample_md=wiki_sample_md,
            )
        )
```

- [ ] **Step 5: Update the return dict to include the airtable status**

Find:

```python
    return {
        "total_titles": profile.total_titles,
        "total_movies": quality_issues.total_movies,
        "hf_status": hf_result.status if hf_result is not None else "skipped",
        "enrichment_plots_usable": enrichment_stats.plots_usable,
    }
```

Replace with:

```python
    return {
        "total_titles": profile.total_titles,
        "total_movies": quality_issues.total_movies,
        "hf_status": hf_result.status if hf_result is not None else "skipped",
        "airtable_status": airtable_result.status if airtable_result is not None else "skipped",
        "enrichment_plots_usable": enrichment_stats.plots_usable,
    }
```

- [ ] **Step 6: Commit**

```bash
git add aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/report.py
git commit -m "feature: render Airtable publish result in the IMDb pipeline report"
```

---

## Task 12: Update `__init__.py` module docstring with the new env vars

**Files:**
- Modify: `aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/__init__.py` — module docstring + `imdb_dataset_pipeline` docstring

- [ ] **Step 1: Update the module-level "Environment variables" list**

Find the module docstring block:

```python
Environment variables:
    HF_TOKEN           — Hugging Face token for dataset publishing (optional)
    IMDB_URL           — Override IMDb data URL (useful for local testing)
    IMDB_WIKI_SNAPSHOT — Wikipedia snapshot date (default 20231101)
    IMDB_WIKI_SHARDS   — number of Parquet shards to load (default 41)
    IMDB_SPARQL_BATCH  — IDs per SPARQL batch (default 400)
"""
```

Replace with:

```python
Environment variables:
    HF_TOKEN             — Hugging Face token for dataset publishing (optional)
    AIRTABLE_API_KEY     — Airtable personal access token (optional, gates Airtable upload)
    AIRTABLE_BASE_ID     — Airtable base id, e.g. appXXXXXXXX (optional, gates Airtable upload)
    AIRTABLE_TABLE_NAME  — Airtable table name (default "IMDB")
    IMDB_URL             — Override IMDb data URL (useful for local testing)
    IMDB_WIKI_SNAPSHOT   — Wikipedia snapshot date (default 20231101)
    IMDB_WIKI_SHARDS     — number of Parquet shards to load (default 41)
    IMDB_SPARQL_BATCH    — IDs per SPARQL batch (default 400)
"""
```

- [ ] **Step 2: Update the `imdb_dataset_pipeline` docstring's env-var block**

Find:

```python
    Environment variables:
        HF_TOKEN              — publish curated dataset to Hugging Face Hub
        IMDB_WIKI_SNAPSHOT    — Wikipedia snapshot date (default 20231101)
        IMDB_WIKI_SHARDS      — number of Parquet shards to load (default 41)
        IMDB_SPARQL_BATCH     — IDs per SPARQL batch (default 400)
        IMDB_DATASET_EXPORTS  — comma-separated export formats (parquet,csv,...)
    """
```

Replace with:

```python
    Environment variables:
        HF_TOKEN              — publish curated dataset to Hugging Face Hub
        AIRTABLE_API_KEY      — publish a 200-row showcase to Airtable
        AIRTABLE_BASE_ID      — Airtable base id (required when AIRTABLE_API_KEY is set)
        AIRTABLE_TABLE_NAME   — Airtable table name (default "IMDB")
        IMDB_WIKI_SNAPSHOT    — Wikipedia snapshot date (default 20231101)
        IMDB_WIKI_SHARDS      — number of Parquet shards to load (default 41)
        IMDB_SPARQL_BATCH     — IDs per SPARQL batch (default 400)
        IMDB_DATASET_EXPORTS  — comma-separated export formats (parquet,csv,...)
    """
```

- [ ] **Step 3: Update the "demonstrates" bullet list to mention Airtable**

Find:

```python
- Hugging Face Publishing (optional, requires HF_TOKEN env var)
```

Replace with:

```python
- Hugging Face Publishing (optional, requires HF_TOKEN env var)
- Airtable Showcase Publishing (optional, requires AIRTABLE_API_KEY + AIRTABLE_BASE_ID; ~200-row sample stratified by genre)
```

- [ ] **Step 4: Commit**

```bash
git add aaiclick/example_projects/imdb_dataset_builder/imdb_dataset_builder/__init__.py
git commit -m "docs: document AIRTABLE_* env vars in the imdb_dataset_builder module docstrings"
```

---

## Task 13: Update `README.md` to mention Airtable

**Files:**
- Modify: `aaiclick/example_projects/imdb_dataset_builder/README.md`

- [ ] **Step 1: Replace the closing line**

Find:

```markdown
Set `HF_TOKEN` to publish the curated dataset to Hugging Face Hub. See `SPEC.md` for design notes.
```

Replace with:

```markdown
Set `HF_TOKEN` to publish the curated dataset to Hugging Face Hub. Set `AIRTABLE_API_KEY` + `AIRTABLE_BASE_ID` (table defaults to `IMDB`) to publish a ~200-row stratified-by-genre showcase sample to Airtable. See `SPEC.md` for design notes.
```

- [ ] **Step 2: Commit**

```bash
git add aaiclick/example_projects/imdb_dataset_builder/README.md
git commit -m "docs: mention Airtable showcase publishing in the imdb_dataset_builder README"
```

---

## Task 14: Push and verify Phase 2 via the CI workflow

- [ ] **Step 1: Push the branch**

```bash
git push -u origin claude/review-imdb-example-SXAtK
```

- [ ] **Step 2: Run the CI workflow**

```bash
./.claude/skills/action-run/run.sh project-imdb-dataset-builder.yaml
```

Expected: workflow status `completed`, conclusion `success`. The job summary should now contain a new `### Airtable Showcase` section. Without `AIRTABLE_API_KEY`/`AIRTABLE_BASE_ID` configured in the CI environment, that section will read:

```
- Skipped: AIRTABLE_API_KEY/BASE_ID not set
- Set `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID` to publish a sample to Airtable
```

That's the expected outcome for an unconfigured CI run — the gating logic short-circuits before any HTTP call. The pipeline must still complete successfully.

- [ ] **Step 3: If the workflow fails**

Inspect the failed step output:

```bash
gh run view <RUN_ID> --repo kolodkin/aaiclick --log-failed
```

Common failure modes and fixes:

| Failure                                                                    | Likely cause                                                              | Fix                                                                                |
|----------------------------------------------------------------------------|---------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `ImportError: cannot import name 'AirtablePublishResult'`                  | Task 7 missed                                                             | Apply Task 7 and recommit.                                                         |
| `ModuleNotFoundError: imdb_dataset_builder.airtable`                       | Task 8 file not committed                                                 | `git add` and recommit.                                                            |
| `KeyError: 'genres'` in `sample_for_airtable`                              | Phase-1 `analyze_genre_balance` rename (Task 2 Step 2) was reverted        | Re-apply Task 2 Step 2.                                                            |
| `AttributeError: ReportContent has no field 'airtable_result'`             | Task 11 Step 2 missed                                                     | Apply Task 11 Step 2.                                                              |
| `TypeError: generate_report() got unexpected keyword 'airtable_result'`    | Task 11 Step 4 missed                                                     | Apply Task 11 Step 4.                                                              |
| Workflow successful but no `### Airtable Showcase` section in the summary  | Task 11 Step 3 missed (the print block)                                   | Apply Task 11 Step 3.                                                              |

After fixing, repeat Step 1 + Step 2.

- [ ] **Step 4: (Optional) live Airtable smoke test**

If you have an Airtable base and PAT handy, run the pipeline locally with the env vars set and confirm a fresh ~200-row sample lands in your `IMDB` table:

```bash
export AIRTABLE_API_KEY="patXXXXXXXXXXXXXX..."
export AIRTABLE_BASE_ID="appXXXXXXXXXXXXXX"
cd aaiclick/example_projects/imdb_dataset_builder
./imdb_dataset_builder.sh
```

The `### Airtable Showcase` report section should read `Status: published`, `Rows published: ~150-200`. The Airtable UI for the table should show the same row count after the run.

---

# Done

Phases 1 and 2 are independently shippable. After Task 6, `genres` is `Array(String)` end-to-end and the existing pipeline is the proof. After Task 14, the Airtable upload is wired in but skipped by default — turning it on is purely an environment-variable configuration change in the calling environment (e.g. selecting the DEV environment in the cloud worker, per the spec's "Run environment" note).
