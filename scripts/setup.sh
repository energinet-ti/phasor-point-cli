#!/bin/bash
set -e

echo "=== PhasorPoint CLI - Development Environment Setup ==="
echo ""

# Check Python version
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found. Please install Python 3.8 or higher."
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
echo "Found Python $PYTHON_VERSION"

# Check if Python version is 3.8 or higher
PYTHON_MAJOR=$(python3 -c 'import sys; print(sys.version_info.major)')
PYTHON_MINOR=$(python3 -c 'import sys; print(sys.version_info.minor)')

if [ "$PYTHON_MAJOR" -lt 3 ] || { [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 8 ]; }; then
    echo "Error: Python 3.8+ required (found $PYTHON_VERSION)"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "Virtual environment created"
else
    echo "Virtual environment already exists"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip --quiet

# Install package in editable mode with dev dependencies
echo "Installing package in editable mode with dev dependencies..."
pip install -e .[dev]

echo ""
echo "=== Setup Complete ==="
echo ""
echo "To activate the environment in the future, run:"
echo "  source venv/bin/activate"
echo ""
echo "Available commands:"
echo "  python -m phasor_point_cli --help     # Run the CLI"
echo "  pytest                # Run tests"
echo "  pytest --cov          # Run tests with coverage"
echo "  make check            # Run all checks (lint + tests)"
echo ""
echo "Note: PhasorPoint ODBC driver required for database connectivity"
echo ""
echo "Verify your setup by running:"
echo "  make check"

