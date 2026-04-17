# Example Notebooks Guidelines

**Example notebooks target Google Colab and MUST use the aaiclick API exclusively** — same rules as `example_projects/CLAUDE.md` (no raw `ch.command()`/`ch.query()`; ask before adding raw SQL).

## Project Structure

Each example is a standalone directory containing a single `.ipynb` with all logic inlined:

```
example_notebooks/<name>/
├── <name>.ipynb       # All code in cells — no sibling .py modules
└── README.md          # Title, description, Colab badge (see README Convention)
```

Colab opens a single `.ipynb` — it does **not** fetch sibling files. Inline the `@task`/`@job` definitions, report rendering, and `main()` directly in cells. If the same logic exists as a runnable package under `example_projects/<name>/`, the notebook is a Colab mirror, not a symlink.

## Notebook Layout

Cells, in order:

1. **Title markdown** — `# <Name>` plus one paragraph describing the pipeline.
2. **Setup cell** (single code cell) — combines everything a fresh Colab runtime needs:
   - `!pip install 'aaiclick[ai]'`
   - `!python -m aaiclick setup` (creates SQLite tables + chdb data dir)
   - Optional provider bootstrap (e.g. Ollama: `apt-get install zstd`, install script, `nohup ollama serve &`, `ollama pull <model>`)
   - `logging.basicConfig(level=logging.INFO, ...)` — aaiclick doesn't configure logging itself
   - `os.environ.setdefault("AAICLICK_AI_MODEL", ...)` with commented-out hosted-provider alternatives (Gemini / OpenAI / Anthropic) and a `google.colab.userdata` pointer for secrets
3. **Imports cell** — all `from aaiclick...` imports grouped at the top (after setup runs).
4. **Task / job / report cells** — inlined from the mirror project; split by logical section with markdown headings.
5. **`async def main()` cell** — same body as `example_projects/<name>/__init__.py::main`.
6. **Run cell** — a single `await main()`. Do **not** use `asyncio.run(main())`; Colab/Jupyter already have a running event loop.

## README Convention

Same setext-title format as `example_projects`, plus a Colab badge pointing at `main`:

```markdown
Project Title
---

One-paragraph description (what the pipeline does, which aaiclick features it demonstrates).

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/kolodkin/aaiclick/blob/main/aaiclick/example_notebooks/<name>/<name>.ipynb)
```

The badge URL targets `main`, so it only resolves after the branch is merged. On feature branches, share the equivalent `blob/<branch>/...` URL manually.

## Running Locally

A notebook opened in Jupyter still works — the setup cell is idempotent and `setdefault` leaves a pre-set `AAICLICK_AI_MODEL` alone. Colab-specific bits (`google.colab.userdata`) stay commented out so local runs don't break on missing imports.
