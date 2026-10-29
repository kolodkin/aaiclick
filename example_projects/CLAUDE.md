# Example Projects Guidelines

**Example projects MUST use the aaiclick API exclusively.**

- Use `Object` methods: `concat()`, `insert()`, `group_by()`, `agg()`, `with_columns()`, `rename()`, `view()`, operators, etc.
- Use `create_object()`, `create_object_from_url()`, `create_object_from_value()`
- **Do NOT use raw ClickHouse queries** (`ch.command()`, `ch.query()`) unless there is absolutely no aaiclick API equivalent
- If raw SQL seems necessary, **ask the user for approval first** — it likely means the API needs extending
- Example projects serve as the public API showcase; they should demonstrate best practices, not internal workarounds

## README Convention

Each example project MUST have a `README.md` with exactly this structure:

```markdown
Project Title
---

One-paragraph description of what the project does and which aaiclick features it demonstrates.

\```bash
./<name>.sh
\```
```

- **Title**: setext heading (underline with `---`)
- **Description**: one paragraph, concise — what it does, what data it uses
- **Run command**: bash code block with shell script invocation, plus any flags or env vars if applicable
- No additional sections or headings — keep it minimal
- Design notes, architectural rationale, or trade-off discussion belong in an optional `SPEC.md` next to `README.md` — not inline in the README. The README may end with a one-line pointer (e.g. `See SPEC.md for design notes.`).

READMEs are included in the docs site via `docs/user_guide/example_projects.md` using `pymdownx.snippets`.

## SPEC.md (optional)

Projects with non-obvious design choices MAY include a `SPEC.md` alongside `README.md` to document them:

- Setext title (`---` underline), then `#`-level sections per topic
- Cover decisions a reader would otherwise question: why algorithm X over Y, why this engine, why this data layout
- Skip if the README's one-paragraph description fully explains the project

## Project Structure

Each example project is a standalone directory containing a nested Python package with the same name:

```
example_projects/<name>/
├── <name>/              # Python package (runnable via `python -m <name>`)
│   ├── __init__.py      # Main logic: @job/@task definitions or standalone async workflow
│   ├── __main__.py      # Entry point for `python -m <name>`
│   └── report.py        # Report rendering (rich tables, Object.markdown(), or print)
├── pyproject.toml       # Project metadata and dependencies
├── uv.lock              # Locked dependency versions
├── <name>.sh            # Shell runner: sets env vars, runs the job, manages workers
├── README.md            # Title, description, how to run (see README Convention above)
└── SPEC.md              # Optional: design notes / architectural rationale
```

- The nested `<name>/` folder is the Python package — the outer folder is the project directory
- `<name>.sh` is the user-facing entry point — `cd`s to its own directory, then runs the project
- Shell scripts use `PYTHON="${PYTHON:-uv run python}"` for dual-mode support (monorepo or standalone)
- `pyproject.toml` depends on `aaiclick` (or `aaiclick[distributed]` for distributed-mode projects)
  through a uv path source on the monorepo checkout — never a PyPI release:
  ```toml
  [tool.uv.sources]
  aaiclick = { path = "../..", editable = true }
  ```
  `uv run python` from the project directory therefore always exercises this tree; CI runs each
  project the same way, with no root sync.
- Each `.sh` supports exactly **one** deployment mode, named in its header comment and README:
  - **Local**: `unset AAICLICK_CH_URL AAICLICK_SQL_URL`, `python -m aaiclick setup`, then
    `python -m <name> "$@"`. `__main__.py` runs the job inline with
    `ajob_test(<job_func>, **params)` (`--params '<JSON>'`) and exits non-zero unless it completed.
  - **Distributed**: require `AAICLICK_CH_URL` / `AAICLICK_SQL_URL` (`: "${VAR:?...}"`), run
    `python -m aaiclick migrate upgrade head`, start `background` + `execution-worker`, then
    `python -m aaiclick run-job <name>.<job_func> --progress "$@"` (`--set KEY=VALUE` overrides).
    `--progress` blocks, prints the task table on each change, and exits non-zero on failure.
  - No custom flag parsing, poll loops, job-id scraping or log juggling — forward `"$@"`.
- Each example project should have a `report.py` file containing final report printout logic
- The `@job` function returns the terminal task directly (e.g. `return report`) — the framework auto-discovers all upstream tasks via the dependency graph; `report.py` is only responsible for the printout
- Always prefer `Object.markdown()` for rendering tables in `report.py` — avoid custom table rendering logic

## Standalone Usage

Projects can be copied out of the monorepo and run independently:

1. Copy the `<name>/` project directory
2. Delete the `[tool.uv.sources]` `aaiclick` line (it points at the monorepo) so `uv` resolves a
   PyPI release, and `uv lock`
3. `uv sync`, then `./name.sh`

## Task Return Values

- For low-scale data prefer a Pydantic model return value over `dict`
- For high-scale data prefer an `Object` return value

## Naming Results

Prefer the single-await idiom — `await obj.sum().data()`, not `await (await obj.sum()).data()`. Every value-column operator — binary, aggregation, unary transform, string/regex, `isin`, null check, `array_map` — returns a `LazyOperator` whose `.data()` auto-materializes.

Name materialized results only when they cross task boundaries or you'd want to open them during lineage / oplog inspection — otherwise the default `t_<snowflake>` is fine. Use `scope="job"` so the table outlives the producing task but is cleaned up with the job; `scope="global"` is for results that should survive past the job (rare in example projects).

API summary in `docs/user_guide/object.md` "Lazy Operator Results" (`.as_()` for operators/aggregations; `name=` / `scope=` kwargs on `copy` / `concat` / `join` / `group_by(...).agg`). Canonical examples: `docs/user_guide/object.md` "Control the table name — use .as_()" and `cyber_threat_feeds/consolidated.py` (`group_by(...).agg(..., name=..., scope="job")`).

## Report Output Format

**All example projects MUST output reports as markdown to stdout.**

Follow the pattern established in the `cyber_threat_feeds` project:

- Use markdown headings (`##`, `###`, `####`) for sections — not ASCII separators (`===`, `---`)
- Use markdown bullet points (`-`) for statistics — not indented plain text
- Use `Object.view(limit=N).markdown()` to render data as markdown tables
- Use `truncate` parameter for wide columns: `.markdown(truncate={"column": 40})`
- Use bold (`**text**`) for emphasis in list items where appropriate

Example structure:
```
## Report Title

### Section Name

- Metric one: value
- Metric two: value

#### Sample (first 5 rows)

| col_a | col_b |
|-------|-------|
| ...   | ...   |

#### Statistics

- Stat A: 1,234
- Stat B: 56.78%
```
