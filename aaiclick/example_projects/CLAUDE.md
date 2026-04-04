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

READMEs are included in the docs site via `docs/example_projects.md` using `pymdownx.snippets`.

## Project Structure

Each example project is a standalone directory containing a nested Python package with the same name:

```
example_projects/<name>/
├── <name>/              # Python package (runnable via `python -m <name>`)
│   ├── __init__.py      # Main logic: @job/@task definitions or standalone async workflow
│   ├── __main__.py      # Entry point for `python -m <name>`
│   ├── report.py        # Report rendering (rich tables, Object.markdown(), or print)
│   └── requirements.txt # Extra dependencies not in aaiclick core (optional)
├── <name>.sh            # Shell runner: sets env vars, calls python -m, manages workers
└── README.md            # Title, description, how to run (see README Convention above)
```

- The nested `<name>/` folder is the Python package — the outer folder is the project directory
- `__main__.py` imports and calls `main()` from `__init__.py`
- `<name>.sh` is the user-facing entry point — `cd`s to its own directory, runs `python -m <name>`
- Shell scripts use `PYTHON="${PYTHON:-uv run python}"` for dual-mode support (monorepo or standalone)
- Orchestration projects: `.sh` registers the job, starts worker, polls status, stops worker
- Each example project should have a `report.py` file containing final report printout logic
- The `@job` function returns the terminal task directly (e.g. `return report`) — the framework auto-discovers all upstream tasks via the dependency graph; `report.py` is only responsible for the printout
- Always prefer `Object.markdown()` for rendering tables in `report.py` — avoid custom table rendering logic

## Standalone Usage

Projects can be copied out of the monorepo and run independently:

1. Copy the `<name>/` project directory
2. `pip install aaiclick`
3. `./name.sh` (or `cd <name> && python -m <name>`)

## Task Return Values

- For low-scale data prefer a Pydantic model return value over `dict`
- For high-scale data prefer an `Object` return value

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
