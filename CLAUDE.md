# Agent Guidelines for aaiclick

This document contains guidelines for AI agents (like Claude Code) working on the aaiclick project.

## Testing Strategy

**IMPORTANT: Do NOT run tests in the Claude cloud environment.**

- All tests run automatically in GitHub Actions when code is pushed
- After pushing changes, check pull request status using the `gh-actions` skill
- Local test execution is unnecessary and should be avoided
- CI/CD pipeline handles all testing and validation


## Code Quality

- **Fail on warnings**: pytest is configured with `-W error` flag
- All warnings are treated as errors in tests
- Use `--strict-markers` for pytest marker validation
- Code coverage reporting is enabled via pytest-cov

## Environment Variables

ClickHouse connection (all optional with sensible defaults):
- `CLICKHOUSE_HOST` (default: "localhost")
- `CLICKHOUSE_PORT` (default: 8123)
- `CLICKHOUSE_USER` (default: "default")
- `CLICKHOUSE_PASSWORD` (default: "")
- `CLICKHOUSE_DB` (default: "default")

## Project Structure

```
aaiclick/
├── aaiclick/          # Main package
│   ├── adapter.py     # ClickHouse adapter and QueryResult wrapper
│   ├── client.py      # Global client instance management
│   └── __init__.py    # Package exports
├── tests/             # Test suite
│   └── test_backend_compatibility.py  # Adapter tests
├── pyproject.toml     # Project configuration
└── CLAUDE.md          # This file
```

## Making Changes

1. Read relevant files before editing
2. Make focused, minimal changes
3. Update tests if needed
4. Commit with descriptive messages
5. Push to feature branch
6. Check PR status via gh-actions skill (do NOT run tests locally)
