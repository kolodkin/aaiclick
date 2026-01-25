---
name: pr-check
description: Check PR status and verify GitHub Actions CI workflows. Use after git push or when user asks to check PR/CI status.
---

# PR Check Skill

You are a PROACTIVE GitHub Actions assistant. After EVERY git push, you MUST automatically verify all GitHub Actions workflows are successful. If any fail, read error logs and resolve issues.

## Run the Check Script

Execute the automated workflow checker script:
```bash
.claude/skills/pr-check/run-workflow-check.sh
```

This script will automatically:
1. Install gh CLI if not available
2. Check authentication
3. Get current branch
4. Poll workflow status every 10 seconds until complete
5. Report SUCCESS or FAILURE with full logs

## On Failure

Analyze the error logs and fix:

- **Test Failures:** Fix code, imports, or test logic
- **Dependency Issues:** Add to pyproject.toml
- **Linting:** Run `ruff check --fix .`

Then commit, push, and run the script again until workflow passes.

## Address PR Review Comments

The script displays unresolved comments with their IDs. For each comment:

**Option A: Fix the issue**
1. Fix and commit
2. Push changes
3. Reply: `✅ Agent Addressed: <what was fixed>`

**Option B: Ask for clarification**
- Reply: `❓ Agent Question: <your question>`

**Reply command:**
```bash
gh api -X POST repos/OWNER/REPO/pulls/PR_NUMBER/comments \
  -f body="✅ Agent Addressed: <description>" -F in_reply_to=COMMENT_ID
```

**IMPORTANT:** Always prefix replies with "Agent" (✅ Agent Addressed / ❓ Agent Question) to identify agent comments.

Be PROACTIVE: Check and poll workflows after every push!
