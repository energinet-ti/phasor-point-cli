#!/bin/bash

# Linting and Formatting Check Script
# This script checks code quality and formatting compliance
# Run this during development to ensure code meets quality standards

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting code quality checks...${NC}\n"

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Change to project root
cd "$PROJECT_ROOT"

# Activate virtual environment if it exists
if [ -d "venv/bin" ]; then
    source venv/bin/activate
elif [ -d ".venv/bin" ]; then
    source .venv/bin/activate
fi

# Check if ruff is installed
if ! command -v ruff &> /dev/null; then
    echo -e "${RED}Error: ruff is not installed${NC}"
    echo "Please install dev dependencies: pip install -e '.[dev]'"
    exit 1
fi

# Track if any checks fail
FAILED=0

echo -e "${YELLOW}[1/3] Running ruff linter...${NC}"
if ruff check --no-cache src/ tests/ --diff; then
    echo -e "${GREEN}Linting passed!${NC}\n"
else
    echo -e "${RED}Linting failed!${NC}\n"
    FAILED=1
fi

echo -e "${YELLOW}[2/3] Checking code formatting...${NC}"
if ruff format --check src/ tests/; then
    echo -e "${GREEN}Format check passed!${NC}\n"
else
    echo -e "${RED}Format check failed!${NC}\n"
    echo -e "${YELLOW}Tip: Run 'ruff format src/ tests/' to auto-format${NC}\n"
    FAILED=1
fi

echo -e "${YELLOW}[3/3] Running tests...${NC}"
if pytest; then
    echo -e "${GREEN}Tests passed!${NC}\n"
else
    echo -e "${RED}Tests failed!${NC}\n"
    FAILED=1
fi

# Summary
echo -e "${YELLOW}===========================================${NC}"
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All checks passed! Code is ready.${NC}"
    exit 0
else
    echo -e "${RED}Some checks failed. Please fix the issues above.${NC}"
    echo -e "\n${YELLOW}Quick fixes:${NC}"
    echo "  - Auto-fix linting issues: ruff check --no-cache --fix src/ tests/"
    echo "  - Auto-format code: ruff format src/ tests/"
    exit 1
fi

