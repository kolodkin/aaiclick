# Agent Guidelines for aaiclick

This document contains guidelines for AI agents (like Claude Code) working on the aaiclick project.

## Test Execution Strategy

**IMPORTANT: Do NOT run tests in the Claude cloud environment.**

- All tests run automatically in GitHub Actions when code is pushed
- After pushing changes, check pull request status using the `review-pr-checks` skill or run `.claude/skills/review-pr-checks/review-pr-checks.sh`
- Local test execution is unnecessary and should be avoided
- CI/CD pipeline handles all testing and validation

## Testing Guidelines

- **Async tests**: Do NOT use `@pytest.mark.asyncio` decorator - it's not required
- pytest-asyncio is configured in `pyproject.toml` to automatically detect async test functions
- Simply define async test functions with `async def test_*():`


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
│   ├── client.py      # Global client instance management
│   ├── object.py      # Core Object class
│   ├── factories.py   # Factory functions for creating objects
│   └── __init__.py    # Package exports
├── tests/             # Test suite
├── pyproject.toml     # Project configuration
└── CLAUDE.md          # This file
```

## Making Changes

1. Read relevant files before editing
2. Make focused, minimal changes
3. Update tests if needed
4. Commit with descriptive messages
5. Push to feature branch
6. Check PR status via `review-pr-checks` skill (do NOT run tests locally)
