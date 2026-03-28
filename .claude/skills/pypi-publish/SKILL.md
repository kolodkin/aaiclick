---
name: pypi-publish
description: Trigger and monitor the PyPI publish GitHub Actions workflow. Use when the user asks to publish, release, or deploy the package to PyPI.
---

# PyPI Publish Skill

You are a PROACTIVE release assistant. When triggered, run the publish script, monitor the workflow to completion, and fix any errors automatically before re-running.

## Required Input

Ask the user for the **release tag** if not provided. Format must be `vX.Y.Z` (e.g., `v1.2.3`).

Optionally ask if this is a **pre-release** (defaults to `false`).

## Run the Publish Script

```bash
.claude/skills/pypi-publish/run-publish.sh --tag vX.Y.Z
```

With pre-release flag:
```bash
.claude/skills/pypi-publish/run-publish.sh --tag vX.Y.Z --pre-release
```

The script automatically:
1. Installs gh CLI if not available
2. Checks authentication and required permissions
3. Detects if the publish workflow is already running — if so, monitors it instead of triggering a new run
4. Triggers the `publish.yaml` workflow with the given tag and pre-release inputs
5. Polls job status until completion
6. Reports SUCCESS with PyPI link, or FAILURE with full error logs

## On Failure

1. **Analyze the error logs** printed by the script
2. **Fix the root cause** (e.g., version already on PyPI, tag format wrong, test failures, missing PyPI trusted publisher config)
3. **Re-run the script** with the same or corrected arguments

## Common Errors

| Error                                    | Fix                                                                      |
|------------------------------------------|--------------------------------------------------------------------------|
| Tag already exists on PyPI               | Bump version in `pyproject.toml`, commit, push, use a new tag            |
| Tag format invalid                       | Ensure tag matches `vX.Y.Z` exactly                                      |
| Not authenticated                        | Run `gh auth login` or set `GH_TOKEN`                                    |
| Insufficient permission                  | Need write or admin access to the repository                             |
| Test failures in workflow                | Fix failing tests, commit, push, re-run                                  |
| PyPI trusted publishing not configured   | Check PyPI project settings for the GitHub Actions trusted publisher     |

## Getting Logs Manually

If you need to inspect a specific run:
```bash
gh run view RUN_ID --repo OWNER/REPO --log-failed
```

Be PROACTIVE: analyze errors and fix them without asking the user — only ask if the fix requires input (like choosing a new version number).
