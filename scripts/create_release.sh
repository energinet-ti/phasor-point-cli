#!/bin/bash

# Helper script to create a release branch
# Usage: ./scripts/create_release.sh 0.0.1 "Release message"

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if version argument is provided
if [ -z "$1" ]; then
    echo -e "${RED}Error: Version number required${NC}"
    echo "Usage: $0 <version> [message]"
    echo "Example: $0 0.0.1 'Initial release'"
    exit 1
fi

VERSION=$1
MESSAGE=${2:-"Release version $VERSION"}
TAG="v$VERSION"
RELEASE_BRANCH="release/$VERSION"
TODAY=$(date +%Y-%m-%d)

echo -e "${YELLOW}Creating release branch for $TAG${NC}"
echo ""

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo -e "${RED}Error: Not in a git repository${NC}"
    exit 1
fi

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}Warning: You have uncommitted changes${NC}"
    read -p "Do you want to continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check if branch already exists
if git rev-parse --verify "$RELEASE_BRANCH" >/dev/null 2>&1; then
    echo -e "${RED}Error: Branch $RELEASE_BRANCH already exists${NC}"
    echo "Use: git branch -D $RELEASE_BRANCH to remove it locally"
    exit 1
fi

# Check if CHANGELOG.md exists
if [ ! -f "CHANGELOG.md" ]; then
    echo -e "${RED}Error: CHANGELOG.md not found${NC}"
    echo "Please create CHANGELOG.md before creating a release"
    exit 1
fi

# Create release branch
echo -e "${YELLOW}Creating branch: $RELEASE_BRANCH${NC}"
git checkout -b "$RELEASE_BRANCH"

# Update CHANGELOG.md
echo -e "${YELLOW}Updating CHANGELOG.md${NC}"

# Replace [Unreleased] header with version and date using awk for portability
awk -v version="$VERSION" -v today="$TODAY" '
/^## \[Unreleased\]$/ {
    print "## [Unreleased]"
    print ""
    print "### Added"
    print "- Nothing yet"
    print ""
    print "## [" version "] - " today
    next
}
{ print }
' CHANGELOG.md > CHANGELOG.md.tmp && mv CHANGELOG.md.tmp CHANGELOG.md

# Show the changes
echo -e "${GREEN}Updated CHANGELOG.md:${NC}"
head -n 15 CHANGELOG.md
echo ""

# Ask for confirmation
echo -e "${YELLOW}Ready to create release branch:${NC}"
echo "  Version: $VERSION"
echo "  Tag: $TAG (to be created after PR merge)"
echo "  Branch: $RELEASE_BRANCH"
echo "  Message: $MESSAGE"
echo ""
read -p "Proceed? (y/n) " -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${RED}Release cancelled${NC}"
    echo "Cleaning up branch..."
    git checkout -
    git branch -D "$RELEASE_BRANCH"
    exit 1
fi

# Commit CHANGELOG changes
echo -e "${YELLOW}Committing CHANGELOG${NC}"
git add CHANGELOG.md
git commit -m "Prepare release $VERSION"

# Push release branch
echo -e "${YELLOW}Pushing branch $RELEASE_BRANCH${NC}"
git push -u origin "$RELEASE_BRANCH"

echo ""
echo -e "${GREEN}Release branch $RELEASE_BRANCH created successfully!${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "  1. Create PR: $RELEASE_BRANCH → main"
echo "     - Title: 'Release $TAG'"
echo "     - Description: '$MESSAGE'"
echo ""
echo "  2. Review and merge the PR"
echo ""
echo "  3. After merge, create tag on main:"
echo "     Option A - Via GitHub UI:"
echo "       - Go to: https://github.com/energinet-ti/phasor-point-cli/releases/new"
echo "       - Tag: $TAG (create on main)"
echo "       - Title: '$TAG'"
echo "       - Description: Copy from CHANGELOG.md"
echo ""
echo "     Option B - Via Git CLI:"
echo "       git checkout main && git pull"
echo "       git tag -a $TAG -m 'Release $TAG'"
echo "       git push origin $TAG"
echo ""
echo "     The release.yml workflow will automatically:"
echo "       - Build the package"
echo "       - Create GitHub Release"
echo "       - Upload distribution files"
echo ""
echo "  4. Manually trigger PyPI publishing:"
echo "     - Go to: https://github.com/energinet-ti/phasor-point-cli/actions"
echo "     - Select 'Publish to PyPI' workflow"
echo "     - Click 'Run workflow'"
echo "     - Choose 'pypi-test' first to test, then 'pypi' for production"
echo ""
echo -e "${BLUE}Note:${NC} Version $VERSION will be automatically set by setuptools-scm from the git tag"
echo ""
