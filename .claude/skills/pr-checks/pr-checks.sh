#!/bin/bash
#
# GitHub Actions Workflow Checker
# Installs gh CLI if needed, polls workflow status, and reports results
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# GitHub CLI version for installation
GH_VERSION="2.62.0"
GH_ARCHIVE="gh_${GH_VERSION}_linux_amd64"

echo -e "${BLUE}🔍 GitHub Actions Workflow Checker${NC}"
echo ""

# Step 1: Install gh CLI if not available
install_gh() {
    if command -v gh &> /dev/null; then
        echo -e "${GREEN}✓ GitHub CLI already installed${NC}"
        gh --version
        return 0
    fi

    echo -e "${YELLOW}⚠️  GitHub CLI not found. Installing...${NC}"

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "Installing on Linux..."
        wget -q "https://github.com/cli/cli/releases/download/v${GH_VERSION}/${GH_ARCHIVE}.tar.gz"
        tar -xzf "${GH_ARCHIVE}.tar.gz"
        # Try system-wide install first, fall back to local install
        if sudo mv "${GH_ARCHIVE}/bin/gh" /usr/local/bin/ 2>/dev/null; then
            rm -rf "${GH_ARCHIVE}"*
            echo -e "${GREEN}✓ GitHub CLI installed successfully (system-wide)${NC}"
            gh --version
        else
            # Fall back to local installation
            mkdir -p ~/.local/bin
            mv "${GH_ARCHIVE}/bin/gh" ~/.local/bin/
            rm -rf "${GH_ARCHIVE}"*
            export PATH="$HOME/.local/bin:$PATH"
            echo -e "${GREEN}✓ GitHub CLI installed successfully (local: ~/.local/bin)${NC}"
            gh --version
        fi

    elif [[ "$OSTYPE" == "darwin"* ]]; then
        echo "Installing on macOS..."
        if command -v brew &> /dev/null; then
            brew install gh
            echo -e "${GREEN}✓ GitHub CLI installed successfully${NC}"
        else
            echo -e "${RED}❌ Error: Homebrew not found${NC}"
            echo "Please install Homebrew first: https://brew.sh"
            exit 1
        fi
    else
        echo -e "${RED}❌ Error: Unsupported OS: $OSTYPE${NC}"
        exit 1
    fi
}

# Step 2: Check authentication
check_auth() {
    echo ""
    echo -e "${BLUE}Checking authentication...${NC}"

    if ! gh auth status &> /dev/null; then
        echo -e "${RED}❌ Not authenticated with GitHub CLI${NC}"
        echo ""
        echo "Please authenticate:"
        echo "  1. Run: gh auth login"
        echo "  2. Or set: export GH_TOKEN=<your-token>"
        exit 1
    fi

    echo -e "${GREEN}✓ Authenticated${NC}"
}

# Step 3: Detect GitHub repository
detect_repo() {
    echo ""
    echo -e "${BLUE}Detecting GitHub repository...${NC}"

    # Try to get repo from git remote
    REMOTE_URL=$(git remote get-url origin 2>/dev/null)

    if [ -z "$REMOTE_URL" ]; then
        echo -e "${RED}❌ Error: No git remote found${NC}"
        exit 1
    fi

    # Extract owner/repo from various remote URL formats
    # Handle: http://*/git/owner/repo, https://github.com/owner/repo, git@github.com:owner/repo
    if [[ "$REMOTE_URL" =~ github\.com[:/]([^/]+)/([^/.]+) ]]; then
        REPO="${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
    elif [[ "$REMOTE_URL" =~ /git/([^/]+)/([^/.]+) ]]; then
        # Handle local proxy format: http://local_proxy@127.0.0.1:*/git/owner/repo
        REPO="${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
    else
        echo -e "${RED}❌ Error: Could not parse repository from remote URL: $REMOTE_URL${NC}"
        echo "Please set manually: export GITHUB_REPOSITORY=owner/repo"
        exit 1
    fi

    echo -e "${GREEN}✓ Repository: ${NC}$REPO"
}

# Step 4: Get current branch
get_branch() {
    BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
    if [ -z "$BRANCH" ]; then
        echo -e "${RED}❌ Error: Not a git repository${NC}"
        exit 1
    fi
    echo -e "${BLUE}📝 Branch: ${NC}$BRANCH"
}

# Step 4.5: Check if PR exists for this branch
check_pr_exists() {
    echo ""
    echo -e "${BLUE}Checking for pull request...${NC}"

    # Check if PR exists for this branch
    PR_DATA=$(gh pr list --repo "$REPO" --head "$BRANCH" --json number,state,url 2>/dev/null)

    if [ -z "$PR_DATA" ] || [ "$PR_DATA" = "[]" ]; then
        echo -e "${YELLOW}⚠️  No pull request found for branch '$BRANCH'${NC}"
        echo ""
        echo -e "${YELLOW}ℹ️  Workflow runs only trigger for:${NC}"
        echo "  • Pushes to main/master branches"
        echo "  • Pull requests targeting main/master"
        echo ""
        echo -e "${YELLOW}💡 To trigger CI checks, create a pull request:${NC}"
        echo "  gh pr create --repo $REPO --head $BRANCH --base main --fill"
        echo ""
        echo -e "${YELLOW}Or push to main/master branch (if you have permissions)${NC}"
        echo ""
        return 1
    else
        PR_NUMBER=$(echo "$PR_DATA" | jq -r '.[0].number')
        PR_URL=$(echo "$PR_DATA" | jq -r '.[0].url')
        echo -e "${GREEN}✓ Pull request found: ${NC}#$PR_NUMBER"
        echo -e "${BLUE}  URL: ${NC}$PR_URL"
        return 0
    fi
}

# Step 5: Check for PR review comments
check_review_comments() {
    echo ""
    echo -e "${BLUE}🔍 Checking for unresolved PR review comments...${NC}"
    echo ""

    # Get PR review decision
    REVIEW_DATA=$(gh pr view "$PR_NUMBER" --repo "$REPO" --json reviewDecision 2>/dev/null)

    if [ -z "$REVIEW_DATA" ]; then
        echo -e "${YELLOW}⚠️  Could not fetch PR review data${NC}"
        return 0
    fi

    REVIEW_DECISION=$(echo "$REVIEW_DATA" | jq -r '.reviewDecision')
    echo -e "${BLUE}Review Status: ${NC}$REVIEW_DECISION"

    # Get unresolved review threads using GraphQL API
    UNRESOLVED_THREADS=$(gh api graphql -f query='
    query($owner: String!, $repo: String!, $number: Int!) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $number) {
          reviewThreads(first: 100) {
            nodes {
              isResolved
              comments(first: 1) {
                nodes {
                  path
                  body
                  line
                }
              }
            }
          }
        }
      }
    }' -f owner="${REPO%/*}" -f repo="${REPO#*/}" -F number="$PR_NUMBER" 2>/dev/null)

    # Count unresolved threads
    UNRESOLVED_COUNT=$(echo "$UNRESOLVED_THREADS" | jq -r '[.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false)] | length' 2>/dev/null || echo "0")

    if [ "$UNRESOLVED_COUNT" -gt 0 ]; then
        echo ""
        echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${YELLOW}📝 PR HAS $UNRESOLVED_COUNT UNRESOLVED REVIEW THREAD(S)${NC}"
        echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo ""

        # Display unresolved comments
        echo "$UNRESOLVED_THREADS" | jq -r '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false) | .comments.nodes[0] | "File: \(.path)\nLine: \(.line // "N/A")\nComment: \(.body)\n---"' 2>/dev/null

        echo ""
        echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo ""
        echo -e "${YELLOW}💡 Agent should address unresolved review comments${NC}"
        echo ""
        echo "Commands to help:"
        echo "  • View all comments: gh pr view $PR_NUMBER --comments"
        echo "  • Reply to comment: gh pr comment $PR_NUMBER --body \"response\""
        echo "  • View diff: gh pr diff $PR_NUMBER"
        echo ""
    else
        echo -e "${GREEN}✅ No unresolved review comments${NC}"
        echo ""
    fi

    # Check review decision
    if [ "$REVIEW_DECISION" = "CHANGES_REQUESTED" ]; then
        echo -e "${YELLOW}⚠️  Changes requested by reviewers${NC}"
        echo -e "${YELLOW}Please address all feedback and push changes${NC}"
        echo ""
        return 1
    elif [ "$REVIEW_DECISION" = "APPROVED" ]; then
        echo -e "${GREEN}✅ PR is approved!${NC}"
        echo ""
        return 0
    elif [ "$REVIEW_DECISION" = "REVIEW_REQUIRED" ]; then
        echo -e "${YELLOW}⏳ PR is awaiting review${NC}"
        if [ "$UNRESOLVED_COUNT" -gt 0 ]; then
            echo -e "${YELLOW}But unresolved comments exist - consider addressing them${NC}"
        fi
        echo ""
        return 0
    else
        echo -e "${BLUE}ℹ️  Review status: $REVIEW_DECISION${NC}"
        echo ""
        return 0
    fi
}

# Step 6: Poll latest workflow until complete
poll_workflow() {
    echo ""
    echo -e "${BLUE}⏳ Waiting for GitHub to process push...${NC}"
    sleep 3

    echo -e "${BLUE}🔄 Polling workflow status...${NC}"
    echo ""

    local max_polls=120  # Max 20 minutes (120 * 10 seconds)
    local poll_count=0

    while [ $poll_count -lt $max_polls ]; do
        # Get latest run for this branch (using --repo flag)
        LATEST=$(gh run list --repo "$REPO" --branch "$BRANCH" --limit 1 --json databaseId,status,conclusion,name,displayTitle,createdAt 2>/dev/null)

        if [ -z "$LATEST" ] || [ "$LATEST" = "[]" ]; then
            echo -e "${YELLOW}⚠️  No workflow runs found for branch $BRANCH${NC}"
            exit 0
        fi

        RUN_ID=$(echo "$LATEST" | jq -r '.[0].databaseId')
        RUN_STATUS=$(echo "$LATEST" | jq -r '.[0].status')
        RUN_CONCLUSION=$(echo "$LATEST" | jq -r '.[0].conclusion')
        RUN_NAME=$(echo "$LATEST" | jq -r '.[0].name')
        RUN_TITLE=$(echo "$LATEST" | jq -r '.[0].displayTitle')

        echo -e "${BLUE}Run #$RUN_ID:${NC} $RUN_NAME - $RUN_TITLE"
        echo -e "${BLUE}Status:${NC} $RUN_STATUS"

        # Check if completed
        if [ "$RUN_STATUS" = "completed" ]; then
            echo ""
            if [ "$RUN_CONCLUSION" = "success" ]; then
                echo -e "${GREEN}✅ Workflow PASSED!${NC}"
                echo ""
                echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                echo -e "${GREEN}STATUS: SUCCESS${NC}"
                echo -e "${GREEN}RUN_ID: $RUN_ID${NC}"
                echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

                # Check for PR review comments after CI passes
                check_review_comments
                exit 0

            else
                echo -e "${RED}❌ Workflow FAILED!${NC}"
                echo -e "${RED}Conclusion: $RUN_CONCLUSION${NC}"
                echo ""
                echo -e "${YELLOW}📋 Fetching error logs...${NC}"
                echo ""

                # Get failed logs (using --repo flag for non-standard remotes)
                LOGS=$(gh run view "$RUN_ID" --repo "$REPO" --log-failed 2>&1)

                echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                echo -e "${RED}STATUS: FAILURE${NC}"
                echo -e "${RED}RUN_ID: $RUN_ID${NC}"
                echo -e "${RED}CONCLUSION: $RUN_CONCLUSION${NC}"
                echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                echo ""
                echo -e "${YELLOW}ERROR LOGS:${NC}"
                echo "$LOGS"
                echo ""
                echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                echo ""
                echo -e "${YELLOW}💡 Agent should analyze and fix these errors${NC}"
                exit 1
            fi
        fi

        # Still running
        echo -e "${YELLOW}⏳ Workflow still running... (poll $((poll_count + 1))/$max_polls)${NC}"
        echo ""
        sleep 10
        poll_count=$((poll_count + 1))
    done

    echo -e "${RED}❌ Timeout: Workflow did not complete within 20 minutes${NC}"
    exit 1
}

# Main execution
main() {
    install_gh
    check_auth
    detect_repo
    get_branch
    check_pr_exists
    poll_workflow
}

main
