# Example Notebooks Guidelines

Colab-targeted examples. **Must use the aaiclick API exclusively** — same rules as `example_projects/CLAUDE.md`.

## Structure

```
example_notebooks/<name>/
├── <name>.ipynb       # All code inline — Colab only opens this file
└── README.md          # Setext title, one paragraph, Colab badge
```

When a mirror exists under `example_projects/<name>/`, the notebook copies the logic inline — Colab doesn't fetch sibling `.py` files.

## Notebook Cells

See `basic_lineage/basic_lineage.ipynb` for the canonical layout:

1. Title markdown
2. **Setup cell** — `!pip install 'aaiclick[ai]'`, `!python -m aaiclick setup`, optional Ollama bootstrap (`apt-get install zstd`, installer, `nohup ollama serve &`, `ollama pull <model>`), `logging.basicConfig(level=INFO)`, `os.environ.setdefault("AAICLICK_AI_MODEL", ...)` with commented hosted-provider alternatives (Gemini / OpenAI / Anthropic)
3. Imports
4. `@task` / `@job` / report functions
5. `async def main()` (mirrors `example_projects/<name>/__init__.py::main`)
6. `await main()` — never `asyncio.run(main())` (Jupyter/Colab already runs a loop)

`setdefault` keeps the cell idempotent across Colab and local Jupyter.

## README

```markdown
Project Title
---

One paragraph — what it does, which aaiclick features it demonstrates.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/kolodkin/aaiclick/blob/main/aaiclick/example_notebooks/<name>/<name>.ipynb)
```

Badge URL targets `main` — share `blob/<branch>/...` on feature branches.
