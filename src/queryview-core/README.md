# queryview-core

Verbatim copy of QueryView's `frontend/src/core` (the backend-agnostic
rendering kernel), imported through the `@qv/core` alias.

- Source commit: `00b8cb683c3257f460182c1417b0152036f7f491`
- Never edit files here. A change goes to QueryView first, then the copy is
  refreshed: `cp -R <queryview>/frontend/src/core src/queryview-core`, update
  this commit, run `npm run check` and `npm test`.
- The `glass-*` classes its markup uses live in `src/styles/queryview-core.css`.

See `docs/designs/viewer.md`, "QueryView Coupling".
