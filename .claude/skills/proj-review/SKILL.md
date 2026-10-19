---
name: proj-review
description: Full review pass over the current branch — runs code-review, simplify, ponytail-review and shortify in order. Use when the user asks for "proj-review" or a full review of their change.
---

# proj-review

Run these skills in order on the current branch (`git diff origin/main...HEAD` plus uncommitted changes). Pass any arguments the user gave to each one.

1. `code-review`
2. `simplify`
3. `ponytail:ponytail-review`
4. `devpowers:shortify`

Apply each skill's fixes before starting the next, so every pass reviews the already-fixed code. Run `devpowers:shortify` last.

Do not report findings between passes. End with one summary: what was fixed, and what was deferred (not fixed in this session) with the reason for each.
