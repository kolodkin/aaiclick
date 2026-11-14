# queryview-core

Verbatim copy of QueryView's `frontend/src/core` (the backend-agnostic
rendering kernel), imported through the `@qv/core` alias.

- Source commit: `f3461027282263210bde97e87deb367a98d84801`
- Never edit files here. A change goes to QueryView first, then the copy is
  refreshed: `cp -R <queryview>/frontend/src/core src/queryview-core`, update
  this commit, run `npm run check` and `npm test`.
- Its styles come with it (`kernel.css`, imported by `src/styles/globals.css`);
  the app themes it through the `--qv-*` variables set there.

See `docs/designs/viewer.md`, "QueryView Coupling".
