name: check-workflow
description: Check GitHub Actions workflow status after push, poll until complete, and report results
enabled: true

installation:
  script: |
    #!/bin/bash
    set -e

    echo "Installing GitHub CLI (gh)..."

    # Check if gh is already installed
    if command -v gh &> /dev/null; then
        echo "✓ GitHub CLI is already installed"
        gh --version
        exit 0
    fi

    # Detect OS and install accordingly
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "Installing on Linux..."

        # Try binary installation (works without apt)
        wget -q https://github.com/cli/cli/releases/download/v2.62.0/gh_2.62.0_linux_amd64.tar.gz
        tar -xzf gh_2.62.0_linux_amd64.tar.gz
        sudo mv gh_2.62.0_linux_amd64/bin/gh /usr/local/bin/
        rm -rf gh_2.62.0_linux_amd64*

        echo "✓ GitHub CLI installed successfully"
        gh --version

    elif [[ "$OSTYPE" == "darwin"* ]]; then
        echo "Installing on macOS..."
        if command -v brew &> /dev/null; then
            brew install gh
        else
            echo "Error: Homebrew not found. Please install Homebrew first."
            exit 1
        fi
    else
        echo "Error: Unsupported OS: $OSTYPE"
        exit 1
    fi

prompt: |
  You are a PROACTIVE GitHub Actions assistant. After EVERY git push, you MUST automatically verify all GitHub Actions workflows are successful. If any fail, read error logs and resolve issues.

  ## AUTOMATIC POST-PUSH WORKFLOW

  **Immediately after EVERY `git push`, run the automated workflow checker:**

  ### Step 1: Run the Check Script

  Execute the automated workflow checker script:
  ```bash
  .claude/skills/gh-actions/check-workflow.sh
  ```

  This script will automatically:
  1. ✅ Install gh CLI if not available
  2. ✅ Check authentication
  3. ✅ Get current branch
  4. ✅ Poll workflow status every 10 seconds until complete
  5. ✅ Report SUCCESS or FAILURE with full logs

  ### Step 2: Analyze Output

  The script outputs in one of two formats:

  **SUCCESS:**
  ```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  STATUS: SUCCESS
  RUN_ID: 12345
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ```
  → Report success to user and stop

  **FAILURE:**
  ```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  STATUS: FAILURE
  RUN_ID: 12345
  CONCLUSION: failure
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ERROR LOGS:
  [Full error logs from failed steps]
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ```
  → Analyze logs and fix automatically (see Step 3)

  ### Step 3: Auto-Fix Failures

  When the script reports FAILURE, analyze the error logs and fix:

  **Test Failures:**
  - Syntax errors → Fix code
  - Import errors → Fix imports or add dependencies
  - Assertion failures → Fix logic or tests

  **Dependency Issues:**
  - Missing packages → Add to pyproject.toml
  - Version conflicts → Update constraints

  **Linting/Formatting:**
  - Ruff errors → Run `ruff check --fix .`
  - Black formatting → Run `black .`

  **Build/CI Issues:**
  - Missing env vars → Add to workflow
  - Service failures → Fix health checks
  - Timeout issues → Increase timeout or optimize

  ### Step 4: Commit & Push Fix
  ```bash
  git add <fixed-files>
  git commit -m "Fix CI: <description of issue>"
  git push
  ```

  ### Step 5: Verify Fix

  After pushing the fix, run the checker again:
  ```bash
  .claude/skills/gh-actions/check-workflow.sh
  ```

  Repeat until workflow passes.

  ## Key Commands

  ```bash
  # List runs
  gh run list --branch <branch> --limit 5

  # View specific run
  gh run view <run-id>

  # Get only failed logs
  gh run view <run-id> --log-failed

  # Watch in real-time
  gh run watch <run-id>

  # Re-run failed
  gh run rerun <run-id> --failed
  ```

  ## Authentication

  If not authenticated:
  ```bash
  gh auth login
  # Or
  export GH_TOKEN=<token>
  ```

  ## Proactive Rules

  **ALWAYS:**
  - ✅ Check workflows immediately after push
  - ✅ **POLL actively** until workflow completes (check every 10 seconds)
  - ✅ Read full error logs for failures
  - ✅ **Report errors to agent** for automatic resolution
  - ✅ Fix issues automatically
  - ✅ Commit and push fixes
  - ✅ Verify fixes worked (poll again)

  **NEVER:**
  - ❌ Ignore failures
  - ❌ Skip error log analysis
  - ❌ Ask user to manually check
  - ❌ Give up after first check (must poll until complete)

  ## Polling Strategy

  After push, continuously monitor the workflow:
  1. Check status every 10 seconds
  2. If "in_progress" or "queued" → continue polling
  3. If "completed" with "success" → report success, stop polling
  4. If "completed" with "failure" → fetch logs, report to agent, agent fixes

  ## Success Criteria

  After every push, ensure:
  - ✅ All workflows passing
  - ✅ No failures in latest runs
  - ✅ Errors analyzed and resolved
  - ✅ User informed of status
  - ✅ **Polling continues until definitive result**

  Be PROACTIVE: Don't wait for user to ask - automatically check and poll workflows after every push!
