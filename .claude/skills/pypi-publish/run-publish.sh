#!/bin/bash
#
# PyPI Publish Action Runner
# Triggers and monitors the PyPI publish GitHub Actions workflow
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

GH_VERSION="2.62.0"
GH_ARCHIVE="gh_${GH_VERSION}_linux_amd64"
WORKFLOW_NAME="publish"

TAG=""
PRE_RELEASE="false"
ACTIVE_RUN_ID=""

echo -e "${BLUE}🚀 PyPI Publish Action Runner${NC}"
echo ""

# Parse arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --tag)
                TAG="$2"
                shift 2
                ;;
            --pre-release)
                PRE_RELEASE="true"
                shift
                ;;
            --monitor-only)
                MONITOR_ONLY="true"
                shift
                ;;
            *)
                echo -e "${RED}❌ Unknown argument: $1${NC}"
                echo "Usage: $0 --tag vX.Y.Z [--pre-release] [--monitor-only]"
                exit 1
                ;;
        esac
    done

    if [ -z "$MONITOR_ONLY" ] && [ -z "$TAG" ]; then
        echo -e "${RED}❌ Error: --tag is required${NC}"
        echo "Usage: $0 --tag vX.Y.Z [--pre-release]"
        exit 1
    fi

    if [ -n "$TAG" ]; then
        if ! echo "$TAG" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
            echo -e "${RED}❌ Error: tag '$TAG' does not match vX.Y.Z format${NC}"
            exit 1
        fi
        echo -e "${BLUE}📦 Tag: ${NC}$TAG"
        echo -e "${BLUE}🔖 Pre-release: ${NC}$PRE_RELEASE"
    fi
}

# Install gh CLI if not available
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
        mkdir -p ~/.local/bin
        mv "${GH_ARCHIVE}/bin/gh" ~/.local/bin/
        rm -rf "${GH_ARCHIVE}"*
        export PATH="$HOME/.local/bin:$PATH"
        echo -e "${GREEN}✓ GitHub CLI installed (~/.local/bin)${NC}"
        gh --version
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        if command -v brew &> /dev/null; then
            brew install gh
            echo -e "${GREEN}✓ GitHub CLI installed${NC}"
        else
            echo -e "${RED}❌ Homebrew not found. Install from https://brew.sh${NC}"
            exit 1
        fi
    else
        echo -e "${RED}❌ Unsupported OS: $OSTYPE${NC}"
        exit 1
    fi
}

# Check authentication
check_auth() {
    echo ""
    echo -e "${BLUE}Checking authentication...${NC}"

    if ! gh auth status &> /dev/null; then
        echo -e "${RED}❌ Not authenticated with GitHub CLI${NC}"
        echo "Run: gh auth login"
        echo "Or set: export GH_TOKEN=<your-token>"
        exit 1
    fi

    echo -e "${GREEN}✓ Authenticated${NC}"
}

# Detect GitHub repository
detect_repo() {
    echo ""
    echo -e "${BLUE}Detecting GitHub repository...${NC}"

    REMOTE_URL=$(git remote get-url origin 2>/dev/null)
    if [ -z "$REMOTE_URL" ]; then
        echo -e "${RED}❌ No git remote found${NC}"
        exit 1
    fi

    if [[ "$REMOTE_URL" =~ github\.com[:/]([^/]+)/([^/.]+) ]]; then
        REPO="${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
    elif [[ "$REMOTE_URL" =~ /git/([^/]+)/([^/.]+) ]]; then
        REPO="${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
    else
        echo -e "${RED}❌ Could not parse repository from: $REMOTE_URL${NC}"
        exit 1
    fi

    REPO_OWNER=$(echo "$REPO" | cut -d'/' -f1)
    REPO_NAME=$(echo "$REPO" | cut -d'/' -f2)

    echo -e "${GREEN}✓ Repository: ${NC}$REPO"
}

# Check required permissions
check_permissions() {
    echo ""
    echo -e "${BLUE}Checking permissions...${NC}"

    # Verify workflow file exists
    if [ ! -f ".github/workflows/${WORKFLOW_NAME}.yaml" ]; then
        echo -e "${RED}❌ Workflow file not found: .github/workflows/${WORKFLOW_NAME}.yaml${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Workflow file exists${NC}"

    # Check authenticated user's repo permission
    AUTH_USER=$(gh api user --jq '.login' 2>/dev/null || echo "")
    if [ -n "$AUTH_USER" ]; then
        USER_PERM=$(gh api "repos/$REPO/collaborators/$AUTH_USER/permission" \
            --jq '.permission' 2>/dev/null || echo "unknown")
        echo -e "${BLUE}  User '$AUTH_USER' permission: ${NC}$USER_PERM"
        if [[ "$USER_PERM" == "read" || "$USER_PERM" == "none" ]]; then
            echo -e "${RED}❌ Insufficient permissions to trigger workflows (need write or above)${NC}"
            exit 1
        fi
        echo -e "${GREEN}✓ Permission sufficient ($USER_PERM)${NC}"
    fi
}

# Check if publish workflow is already running; sets ACTIVE_RUN_ID if found
check_running() {
    echo ""
    echo -e "${BLUE}Checking for active publish runs...${NC}"

    ACTIVE_RUNS=$(gh run list --repo "$REPO" --workflow "${WORKFLOW_NAME}.yaml" \
        --json databaseId,status,createdAt,displayTitle \
        --jq '[.[] | select(.status == "in_progress" or .status == "queued" or .status == "waiting")]' \
        2>/dev/null || echo "[]")

    COUNT=$(echo "$ACTIVE_RUNS" | jq 'length')

    if [ "$COUNT" -gt 0 ]; then
        ACTIVE_RUN_ID=$(echo "$ACTIVE_RUNS" | jq -r '.[0].databaseId')
        RUN_STATUS=$(echo "$ACTIVE_RUNS" | jq -r '.[0].status')
        RUN_TITLE=$(echo "$ACTIVE_RUNS" | jq -r '.[0].displayTitle')
        echo -e "${YELLOW}⚠️  Active publish run found: ${NC}#$ACTIVE_RUN_ID"
        echo -e "${YELLOW}   Status: $RUN_STATUS | $RUN_TITLE${NC}"
        echo -e "${YELLOW}   Monitoring existing run instead of triggering new one...${NC}"
        return 0
    fi

    echo -e "${GREEN}✓ No active publish runs${NC}"
    return 1
}

# Trigger the workflow
trigger_workflow() {
    echo ""
    echo -e "${BLUE}🚀 Triggering publish workflow...${NC}"
    echo -e "${BLUE}  Workflow: ${NC}$WORKFLOW_NAME"
    echo -e "${BLUE}  Tag:      ${NC}$TAG"
    echo -e "${BLUE}  Pre-rel:  ${NC}$PRE_RELEASE"
    echo ""

    gh workflow run "$WORKFLOW_NAME" \
        --repo "$REPO" \
        -f tag="$TAG" \
        -f pre-release="$PRE_RELEASE"

    echo -e "${GREEN}✓ Workflow triggered${NC}"
    echo -e "${BLUE}⏳ Waiting for run to appear...${NC}"
    sleep 6

    # Retry up to 5 times to get the new run ID
    for i in {1..5}; do
        ACTIVE_RUN_ID=$(gh run list --repo "$REPO" --workflow "${WORKFLOW_NAME}.yaml" \
            --json databaseId,createdAt \
            --jq 'sort_by(.createdAt) | reverse | .[0].databaseId' 2>/dev/null || echo "")
        if [ -n "$ACTIVE_RUN_ID" ]; then
            echo -e "${GREEN}✓ Run ID: ${NC}#$ACTIVE_RUN_ID"
            return 0
        fi
        echo -e "${YELLOW}  Waiting for run ID (attempt $i/5)...${NC}"
        sleep 5
    done

    echo -e "${RED}❌ Could not retrieve run ID after triggering workflow${NC}"
    exit 1
}

# Monitor workflow run until completion
monitor_run() {
    local RUN_ID="$1"

    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}Monitoring run #$RUN_ID${NC}"
    echo -e "${BLUE}https://github.com/$REPO/actions/runs/$RUN_ID${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""

    while true; do
        RUN_DATA=$(gh run view "$RUN_ID" --repo "$REPO" --json status,conclusion,jobs 2>/dev/null)
        STATUS=$(echo "$RUN_DATA" | jq -r '.status')
        CONCLUSION=$(echo "$RUN_DATA" | jq -r '.conclusion')

        if [ "$STATUS" = "completed" ]; then
            break
        fi

        # Show per-job progress
        echo -e "${BLUE}Status: $STATUS${NC}"
        echo "$RUN_DATA" | jq -r '.jobs[] | "  \(.name): \(.status)\(if .conclusion and .conclusion != "null" then " (\(.conclusion))" else "" end)"' \
            2>/dev/null || true
        echo ""
        sleep 20
    done

    # Report final result
    if [ "$CONCLUSION" = "success" ]; then
        echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${GREEN}STATUS: SUCCESS${NC}"
        echo -e "${GREEN}📦 Package published to PyPI${NC}"
        if [ -n "$TAG" ]; then
            echo -e "${GREEN}🏷️  Tag: $TAG${NC}"
        fi
        echo -e "${GREEN}🔗 https://github.com/$REPO/actions/runs/$RUN_ID${NC}"
        echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        exit 0
    else
        echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${RED}STATUS: FAILURE (conclusion: $CONCLUSION)${NC}"
        echo -e "${RED}🔗 https://github.com/$REPO/actions/runs/$RUN_ID${NC}"
        echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo ""
        echo -e "${YELLOW}📋 Failed job logs:${NC}"
        echo ""
        gh run view "$RUN_ID" --repo "$REPO" --log-failed 2>&1 || true
        echo ""
        echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${YELLOW}💡 Agent: analyze errors above, fix issues, then re-run this skill${NC}"
        exit 1
    fi
}

# Main
main() {
    parse_args "$@"
    install_gh
    check_auth
    detect_repo
    check_permissions

    if check_running; then
        monitor_run "$ACTIVE_RUN_ID"
    else
        trigger_workflow
        monitor_run "$ACTIVE_RUN_ID"
    fi
}

main "$@"
