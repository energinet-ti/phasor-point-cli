#!/bin/bash

# Helper script to create a new release
# Usage: ./scripts/create_release.sh 1.0.1 "Release message"

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if version argument is provided
if [ -z "$1" ]; then
    echo -e "${RED}Error: Version number required${NC}"
    echo "Usage: $0 <version> [message]"
    echo "Example: $0 1.0.1 'Bug fixes and improvements'"
    exit 1
fi

VERSION=$1
MESSAGE=${2:-"Release version $VERSION"}
TAG="v$VERSION"

echo -e "${YELLOW}Creating release $TAG${NC}"

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

# Check if tag already exists
if git rev-parse "$TAG" >/dev/null 2>&1; then
    echo -e "${RED}Error: Tag $TAG already exists${NC}"
    echo "Use: git tag -d $TAG && git push origin :refs/tags/$TAG to remove it"
    exit 1
fi

# Update version in pyproject.toml
echo -e "${YELLOW}Updating version in pyproject.toml${NC}"
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' "s/version = \".*\"/version = \"$VERSION\"/" pyproject.toml
else
    # Linux
    sed -i "s/version = \".*\"/version = \"$VERSION\"/" pyproject.toml
fi

# Show the change
echo -e "${GREEN}Version updated to $VERSION${NC}"
grep "version = " pyproject.toml

# Ask for confirmation
echo ""
echo -e "${YELLOW}Ready to create release:${NC}"
echo "  Version: $VERSION"
echo "  Tag: $TAG"
echo "  Message: $MESSAGE"
echo ""
read -p "Proceed with release? (y/n) " -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${RED}Release cancelled${NC}"
    exit 1
fi

# Commit version change
echo -e "${YELLOW}Committing version change${NC}"
git add pyproject.toml
git commit -m "Bump version to $VERSION" || echo "No changes to commit"

# Push changes
echo -e "${YELLOW}Pushing changes${NC}"
git push origin $(git branch --show-current)

# Create and push tag
echo -e "${YELLOW}Creating tag $TAG${NC}"
git tag -a "$TAG" -m "$MESSAGE"

echo -e "${YELLOW}Pushing tag $TAG${NC}"
git push origin "$TAG"

echo ""
echo -e "${GREEN}Release $TAG created successfully!${NC}"
echo ""
echo "GitHub Actions will now:"
echo "  1. Build the package"
echo "  2. Create a GitHub release"
echo "  3. Upload distribution files"
echo ""
echo "Check the progress at:"
echo "  https://github.com/energinet-ti/phasor-point-cli/actions"
echo ""
echo "Once complete, the release will be available at:"
echo "  https://github.com/energinet-ti/phasor-point-cli/releases/tag/$TAG"

