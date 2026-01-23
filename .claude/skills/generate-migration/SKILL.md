# Generate Migration Skill

**enabled**: true

**installation**:
```bash
#!/bin/bash
set -e

echo "Checking GitHub CLI installation..."

# Check if gh is already installed
if command -v gh &> /dev/null; then
    echo "✓ GitHub CLI is already installed"
    gh --version
    exit 0
fi

echo "GitHub CLI (gh) is required but not installed."
echo "This skill requires the pr-checks skill to be installed first."
exit 1
```

**prompt**: |
  You are an assistant for generating Alembic database migrations using GitHub Actions.

  ## When to Use This Skill

  Use this skill when:
  - User asks to generate a new database migration
  - User wants to autogenerate a migration from model changes
  - User says "generate migration" or similar
  - Model changes need to be converted to a migration file

  ## How It Works

  This skill triggers a GitHub Actions workflow that:
  1. Sets up PostgreSQL in CI/CD
  2. Runs existing migrations to create current schema
  3. Runs `alembic revision --autogenerate` to detect changes
  4. Commits the generated migration file
  5. Pushes to the current branch

  ## Usage

  Trigger the workflow with:
  ```bash
  gh workflow run generate-migration.yaml \
    -f message="your migration description"
  ```

  Then monitor the workflow:
  ```bash
  # List recent runs
  gh run list --workflow=generate-migration.yaml --limit 5

  # Watch specific run
  gh run watch <run-id>

  # View run logs
  gh run view <run-id> --log
  ```

  ## Example Interaction

  **User**: "Generate a migration for the new user table"

  **Assistant**:
  ```bash
  # Trigger migration generation
  gh workflow run generate-migration.yaml \
    -f message="add user table"

  # Wait a moment for workflow to start
  sleep 5

  # Get the run ID
  RUN_ID=$(gh run list --workflow=generate-migration.yaml --limit 1 --json databaseId --jq '.[0].databaseId')

  # Monitor the run
  gh run watch $RUN_ID
  ```

  ## Important Notes

  - The workflow runs on the **current branch**
  - Make sure you're on the correct feature branch before triggering
  - The workflow commits directly to the branch (no PR created)
  - If no model changes are detected, no migration is created
  - Always pull after the workflow completes to get the new migration file

  ## After Generation

  Once the workflow completes successfully:
  1. Pull the latest changes: `git pull`
  2. Review the generated migration file
  3. Test the migration locally (if database available)
  4. Commit any manual adjustments if needed
  5. Push and let tests run

  ## Troubleshooting

  - **Workflow fails**: Check logs with `gh run view <run-id> --log`
  - **No changes detected**: Verify your model changes are saved
  - **Merge conflicts**: Pull latest changes before generating
  - **Permission errors**: Ensure repository has write permissions for workflows
