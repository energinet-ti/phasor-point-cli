# PhasorPoint CLI - Development Environment Setup (Windows)
# Requires PowerShell 5.1 or higher

$ErrorActionPreference = "Stop"

Write-Host "=== PhasorPoint CLI - Development Environment Setup ===" -ForegroundColor Cyan
Write-Host ""

# Check Python version
try {
    $pythonVersion = python --version 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Python not found"
    }
    Write-Host "Found $pythonVersion"
    
    # Extract version numbers
    $versionMatch = $pythonVersion -match "Python (\d+)\.(\d+)"
    if (-not $versionMatch) {
        throw "Could not parse Python version"
    }
    
    $majorVersion = [int]$matches[1]
    $minorVersion = [int]$matches[2]
    
    if ($majorVersion -lt 3 -or ($majorVersion -eq 3 -and $minorVersion -lt 8)) {
        Write-Host "Error: Python 3.8+ required (found Python $majorVersion.$minorVersion)" -ForegroundColor Red
        exit 1
    }
}
catch {
    Write-Host "Error: python not found. Please install Python 3.8 or higher." -ForegroundColor Red
    Write-Host "Download from: https://www.python.org/downloads/" -ForegroundColor Yellow
    exit 1
}

# Create virtual environment if it doesn't exist
if (-not (Test-Path "venv")) {
    Write-Host "Creating virtual environment..."
    python -m venv venv
    Write-Host "Virtual environment created"
}
else {
    Write-Host "Virtual environment already exists"
}

# Activate virtual environment
Write-Host "Activating virtual environment..."
& ".\venv\Scripts\Activate.ps1"

# Upgrade pip
Write-Host "Upgrading pip..."
python -m pip install --upgrade pip --quiet

# Install package in editable mode with dev dependencies
Write-Host "Installing package in editable mode with dev dependencies..."
pip install -e .[dev]

Write-Host ""
Write-Host "=== Setup Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "To activate the environment in the future, run:"
Write-Host "  .\venv\Scripts\Activate.ps1"
Write-Host ""
Write-Host "Available commands:"
Write-Host "  python -m phasor_point_cli --help     # Run the CLI"
Write-Host "  pytest                # Run tests"
Write-Host "  pytest --cov          # Run tests with coverage"
Write-Host "  make check            # Run all checks (lint + tests)"
Write-Host ""
Write-Host "Note: PhasorPoint ODBC driver required for database connectivity"
Write-Host ""
Write-Host "Verify your setup by running:"
Write-Host "  make check"

