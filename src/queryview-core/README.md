# queryview-core

Verbatim copy of QueryView's `frontend/src/core` (the backend-agnostic
rendering kernel), imported through the `@qv/core` alias.

- Source commit: `ecb23b48ca71dc091db010681bd3b8e4ec5c56dd`
- Never edit files here. A change goes to QueryView first, then the copy is
  refreshed: `cp -R <queryview>/frontend/src/core src/queryview-core`, update
  this commit, run `npm run check` and `npm test`.
- The `glass-*` classes its markup uses live in `src/styles/queryview-core.css`.

See `docs/designs/viewer.md`, "QueryView Coupling".
