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
        wget -q https://github.com/cli/cli/releases/download/v2.62.0/gh_2.62.0_linux_amd64.tar.gz
        tar -xzf gh_2.62.0_linux_amd64.tar.gz
        sudo mv gh_2.62.0_linux_amd64/bin/gh /usr/local/bin/
        rm -rf gh_2.62.0_linux_amd64*
        echo -e "${GREEN}✓ GitHub CLI installed successfully${NC}"
        gh --version

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

# Step 5: Poll latest workflow until complete
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
                exit 0

            else
                echo -e "${RED}❌ Workflow FAILED!${NC}"
                echo -e "${RED}Conclusion: $RUN_CONCLUSION${NC}"
                echo ""
                echo -e "${YELLOW}📋 Fetching error logs...${NC}"
                echo ""

                # Get failed logs
                LOGS=$(gh run view "$RUN_ID" --log-failed 2>&1)

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
    poll_workflow
}

main
