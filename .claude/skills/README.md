# Claude Skills for aaiclick

This directory contains Claude Code skills for the aaiclick project.

## Available Skills

### gh-actions

**Purpose**: Install and manage GitHub Actions workflows using the GitHub CLI (`gh`)

**Features**:
- Automatic installation of GitHub CLI
- Check workflow run status
- View detailed logs for failed runs
- Trigger workflows manually
- Re-run failed jobs
- Real-time workflow monitoring

**Usage**:

Ask Claude to:
- "Check the GitHub Actions status"
- "Why did the workflow fail?"
- "Show me the logs for the failed test"
- "Re-run the failed workflow"
- "Trigger the test workflow"

**Installation**:

The skill includes an automatic installation script that:
1. Checks if `gh` CLI is already installed
2. Downloads and installs the latest GitHub CLI for Linux/macOS
3. Verifies the installation

**Authentication**:

Most GitHub CLI operations require authentication. You can authenticate by:

```bash
# Interactive login
gh auth login

# Or use a token
export GH_TOKEN=your_github_personal_access_token
```

## Skill Structure

Each skill is defined in a Markdown file with the following sections:

- `name`: Skill identifier
- `description`: What the skill does
- `enabled`: Whether the skill is active
- `installation`: Optional installation script
- `prompt`: Instructions for Claude on how to use the skill

## Creating New Skills

To create a new skill:

1. Create a `.md` file in this directory
2. Define the skill metadata (name, description, enabled)
3. Add an optional installation script
4. Write the prompt that instructs Claude on how to use the skill

Example:

```markdown
name: my-skill
description: Does something useful
enabled: true

installation:
  script: |
    #!/bin/bash
    echo "Installing..."

prompt: |
  You are a helpful assistant that...
```

## Enabling/Disabling Skills

Set `enabled: false` to disable a skill without deleting it.

## Best Practices

1. Include installation scripts for required tools
2. Provide clear usage examples in the prompt
3. Document common error cases and solutions
4. Keep skills focused on a single purpose
5. Test skills thoroughly before committing
