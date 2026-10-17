Task API
---

Typed packages for writing tasks, layered on the language-agnostic
[Task Interface](task_interface.md):

| Layer          | What                                          |
|----------------|-----------------------------------------------|
| Task API       | [Python](task_api_python.md), [Java](task_api_java.md) packages |
| Task Interface | JSON files `in.json` / `out.json` and the vocabulary |

Each API type is one vocabulary shape; the package builds and serializes it.
A language without a package speaks the Task Interface directly.
