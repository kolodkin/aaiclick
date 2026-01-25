---
name: generate-migration
description: Generate Alembic database migrations using GitHub Actions. Use when user asks to generate or autogenerate a migration.
---

# Generate Migration Skill

Trigger a GitHub Actions workflow to autogenerate Alembic migrations.

## Usage

```bash
gh workflow run generate-migration.yaml -f message="your migration description"
```

Then monitor:
```bash
# Get run ID
RUN_ID=$(gh run list --workflow=generate-migration.yaml --limit 1 --json databaseId --jq '.[0].databaseId')

# Watch it
gh run watch $RUN_ID
```

## How It Works

The workflow:
1. Sets up PostgreSQL in CI/CD
2. Runs existing migrations
3. Runs `alembic revision --autogenerate`
4. Commits and pushes the migration file

## After Generation

1. Pull the latest changes: `git pull`
2. Review the generated migration file
3. Push and let tests run

## Troubleshooting

- **Workflow fails**: `gh run view <run-id> --log`
- **No changes detected**: Verify model changes are saved
- **Merge conflicts**: Pull latest before generating
