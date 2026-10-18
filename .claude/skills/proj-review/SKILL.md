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
