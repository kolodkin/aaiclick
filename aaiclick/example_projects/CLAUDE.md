# Example Projects Guidelines

**Example projects MUST use the aaiclick API exclusively.**

- Use `Object` methods: `concat()`, `insert()`, `group_by()`, `agg()`, `with_columns()`, `rename()`, `view()`, operators, etc.
- Use `create_object()`, `create_object_from_url()`, `create_object_from_value()`
- **Do NOT use raw ClickHouse queries** (`ch.command()`, `ch.query()`) unless there is absolutely no aaiclick API equivalent
- If raw SQL seems necessary, **ask the user for approval first** — it likely means the API needs extending
- Example projects serve as the public API showcase; they should demonstrate best practices, not internal workarounds

## Project Structure

- Each example project should have a `report.py` file containing final report printout logic
- The `@job` function returns `TaskResult` with all tasks listed — report is always the last (finalization) task; `report.py` is only responsible for the printout

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
