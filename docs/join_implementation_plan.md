Join Implementation Plan
---

Phased rollout of the `Object.join()` operator specified in `docs/join.md`. Pattern mirrors the existing `concat()` implementation (`aaiclick/data/object/ingest.py` + `aaiclick/data/object/object.py`).

# Phase 1 — Key Resolution & Validation (pure Python)

**Objective**: lock the API shape with no SQL side-effects, so every error path is testable without touching ClickHouse.

**Tasks**:

- Add `aaiclick/data/object/join.py` with a pure helper:
  ```python
  class JoinKeys(NamedTuple):
      left: list[str]
      right: list[str]

  def resolve_join_keys(
      on: str | list[str] | None,
      left_on: str | list[str] | None,
      right_on: str | list[str] | None,
      how: str,
  ) -> JoinKeys: ...
  ```
  Handles: mutual exclusion of `on` vs `{left_on, right_on}`, equal-length key lists, `how="cross"` forbidding any keys, normalization of `str` → `[str]`.
- Define `JoinHow = Literal["inner", "left", "right", "full", "cross"]` and a `HOW_TO_SQL` mapping.

**Tests** (`aaiclick/data/object/test_join.py`):
- Every validation error path: conflicting `on`/`left_on`, `left_on` without `right_on`, key-length mismatch, `how="cross"` with keys, unknown `how`.
- Normalization: `"k"` and `["k"]` produce identical `JoinKeys`.

**Deliverable**: `resolve_join_keys` green, merged. No user-visible API yet.

# Phase 2 — Schema Builder (pure Python)

**Objective**: compute the result `Schema` deterministically from two input schemas, key lists, `how`, and `suffixes` — no SQL.

**Tasks**:

- `build_join_schema(left_cols, right_cols, keys, how, suffixes) -> Schema` in `join.py`.
  - Key dedup: if `left_keys == right_keys` (by name), one copy survives under the key name; else both appear.
  - Collision detection on non-key columns: raise `ValueError` when `suffixes=None`, else apply both suffixes (reject `("", …)` / `(…, "")`).
  - Nullable promotion table from the spec (LEFT → right nullable, RIGHT → left nullable, FULL → both).
  - Preserve `LowCardinality`, `array`, `description` on each column.
  - `aai_id` excluded from both inputs before projection; result table's own `aai_id` is added by `create_object`.
- Fieldtype: always `FIELDTYPE_DICT`.

**Tests**:
- Key dedup under `on=` vs `left_on=/right_on=`.
- Collision raises without `suffixes`, succeeds with `suffixes`.
- Nullable promotion for each `how`.
- `LowCardinality(String)` outer-side → `LowCardinality(Nullable(String))`.
- Array non-key columns passed through.
- Key-type incompatibility (`String` vs `Int64`) raises; `String` ↔ `FixedString` passes.

**Deliverable**: schema builder green, merged. Still no user-visible API.

# Phase 3 — SQL Generation & Materialization

**Objective**: wire `resolve_join_keys` + `build_join_schema` to `create_object` + a single `INSERT INTO ... SELECT ... JOIN ...`.

**Tasks**:

- `join_objects_db(left_info, right_info, *, left_keys, right_keys, how, suffixes, ch_client) -> Object` in `join.py`:
  1. Call schema builder → `schema`.
  2. `result = await create_object(schema)`.
  3. Build projection list (apply suffixes, cast to target types via `ch_type()`, drop `aai_id`). Self-join safe via `AS l` / `AS r` aliases.
  4. Emit `INSERT INTO {result.table} ({cols}) SELECT {projection} FROM {left.source} AS l {HOW} JOIN {right.source} AS r {USING (...) | ON ...}`.
  5. `oplog_record_sample(result.table, "join", kwargs={...})`.
- Expose `Object.join(...)` on `aaiclick/data/object/object.py`. Method resolves `on/left_on/right_on` → `JoinKeys` → calls `join_objects_db` with `self._get_ingest_query_info()` and `other._get_ingest_query_info()`.

**Tests** (round-trip via `.data()`):
- Each `how` value with a small hand-rolled dataset; assert row count and field values.
- `on="k"` USING form: result has one key column.
- `left_on`/`right_on` ON form: both key columns survive.
- Self-join: `a.join(a, on="id")` doesn't explode on alias collision.
- `suffixes=("_l", "_r")` renames on collision.
- `how="cross"` Cartesian product size check.
- Nullable promotion reaches the result: `LEFT JOIN` miss → `None` in `.data()`.
- `LowCardinality` key works end-to-end.
- Oplog entry present with the correct `kwargs`.
- Large-ish dataset (100k × 10k rows) sanity check — doesn't time out locally.

**Deliverable**: `basics.join(ratings, on="tconst", how="left")` works end-to-end on chdb.

# Phase 4 — Examples & Integration

**Objective**: make `join()` discoverable and exercised by the example suite.

**Tasks**:

- New example file `aaiclick/data/examples/basic_join.py` covering inner / left / cross with `# →` output annotations per the guideline in `CLAUDE.md`.
- Update an existing multi-table example where a join naturally replaces hand-rolled SQL (candidate: `imdb` or `nyc-taxi` in `aaiclick/example_projects/`) — gated on the example being a cleaner read after the change; skip if it isn't.
- Run `check-pr` skill after push.

**Deliverable**: `basic_join.py` runs clean; example output matches comments.

# Phase 5 — Migrate Spec Into `docs/object.md`

**Objective**: fold the freestanding `docs/join.md` into `docs/object.md` as a subsection, consistent with how `concat`, `insert`, `copy` live there today. Remove duplication; keep the spec as a living reference.

**Tasks**:

1. **Add Quick Reference row** to the table at `docs/object.md:16` (between the `Ingest` rows and `Data Retrieval`):
   ```markdown
   | `.join(other, on, how)` | Join | Join two Objects on key columns → dict Object | [join()](#join) |
   ```
2. **Add `## join()` section** under `# Ingest` (or a new `# Join` top-level section if the Ingest grouping feels wrong — decide during the edit). Content is the trimmed body of `docs/join.md`:
   - API signature table
   - Join Types table
   - Column Semantics (keys, collisions, `aai_id`, order, nullable/LC/array)
   - 2-3 copy-paste examples with inline `# →` outputs
   - Short "Distributed Considerations" paragraph
3. **Drop the Implementation Sketch section** — once the code exists, reference `aaiclick/data/object/join.py` → `join_objects_db` by name instead of duplicating skeleton SQL.
4. **Replace status**: remove `⚠️ NOT YET IMPLEMENTED` banner; add `**Implementation**: aaiclick/data/object/join.py` — see `join_objects_db`.
5. **Delete `docs/join.md`** after the move — single source of truth is `object.md`. Also remove the `## join() Operator` row from `docs/future.md`.
6. **Delete `docs/join_implementation_plan.md`** (this file) once all phases are ✅.
7. **Verify**: mkdocs build clean; no dangling links to `docs/join.md`.

**Deliverable**: `docs/object.md` contains the join documentation; `docs/join.md`, `docs/join_implementation_plan.md`, and the `future.md` entry are removed.

# Out of Scope (follow-ups)

- `how="semi" | "anti"` — left-only output schema.
- `how="asof"` — needs `order_by` + `tolerance`.
- `strategy=` kwarg for ClickHouse join-algorithm hints.
- Distributed `GLOBAL JOIN` path for the sharded backend.

Track in `docs/future.md` after Phase 5.
