---
name: pypi-publish
description: Trigger and monitor the PyPI publish GitHub Actions workflow. Use when user asks to publish, release, or deploy to PyPI.
---

# PyPI Publish Skill

Trigger and monitor the `publish` GitHub Actions workflow that builds, tests, and publishes the package to PyPI.

## Prerequisites

Ask the user for:
1. **Release tag** — must be in `vX.Y.Z` format (e.g. `v1.2.3`)
2. **Pre-release?** — optional, defaults to `false`

## Run the Publish Script

```bash
.claude/skills/pypi-publish/run-publish.sh --tag vX.Y.Z [--pre-release]
```

The script will automatically:
1. Install gh CLI if not available
2. Check authentication
3. Detect the GitHub repository
4. Check required permissions (write access needed to trigger workflows)
5. Check if the publish workflow is already running — if so, monitor it instead of re-triggering
6. Trigger `gh workflow run publish` with the provided tag and pre-release inputs
7. Poll and report per-job status until completion
8. On success: confirm package published to PyPI
9. On failure: print full failed job logs

## On Failure

1. Read the error logs printed by the script
2. Identify the root cause (build error, test failure, PyPI auth issue, tag conflict, etc.)
3. Fix the issue (e.g. update `pyproject.toml`, fix tests, correct tag format)
4. Commit and push the fix
5. Re-run the script with the same tag:
   ```bash
   .claude/skills/pypi-publish/run-publish.sh --tag vX.Y.Z
   ```

## Check an Already-Running Workflow

If you know a publish workflow is already in progress, run without `--tag` to monitor it:
```bash
.claude/skills/pypi-publish/run-publish.sh --monitor-only
```

## Common Errors and Fixes

| Error | Fix |
|-------|-----|
| Tag already exists on PyPI | Bump the version in `pyproject.toml`, use a new tag |
| Tag format invalid | Ensure tag matches `vX.Y.Z` exactly |
| Insufficient permissions | User needs write access to the repository |
| Test failures in workflow | Fix failing tests, commit, push, re-run |
| PyPI trusted publishing misconfigured | Check OIDC trusted publisher settings on PyPI project |
