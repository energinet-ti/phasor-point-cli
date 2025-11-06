# AGENTS.md

## Project Overview

PhasorPoint CLI is a Python command-line tool for extracting and processing PMU (Phasor Measurement Unit) data from PhasorPoint databases. The tool provides flexible time-range extraction, automatic power calculations, performance optimization features, and data quality validation.

## Setup Commands

### Initial Setup

```bash
# Run setup script (creates venv and installs dependencies)
./scripts/setup.sh           # Linux/macOS
# .\scripts\setup.ps1         # Windows

# Or manually:
python3 -m venv venv
source venv/bin/activate
pip install -e .[dev]
```

### Development Commands

```bash
# Install package in development mode
make dev

# Run all quality checks (lint + format + validate + tests)
make check

# Run tests
make test

# Run tests with coverage report
make coverage

# Run linter (check only)
make lint

# Auto-format code
make format

# Auto-fix linting issues and format code
make fix

# Run type checker (Pyright)
make type-check

# Validate pyproject.toml
make validate-pyproject

# Build wheel distribution package
make build

# Clean build artifacts and cache files
make clean
```

## General Code Style

- NEVER use emojis in code, comments, docstrings, or output messages
- Follow Python best practices and PEP 8
- Use type hints where appropriate
- Keep functions focused and modular

## Python Version Compatibility

- Always check `pyproject.toml` for supported Python versions (currently >=3.8)
- All code, including tests, must be compatible with Python 3.8+
- Avoid Python 3.9+ features like `str.removeprefix()` and `str.removesuffix()`
- Avoid Python 3.10+ features like union type syntax (`str | None`) - use `Optional[str]` instead
- Avoid Python 3.10+ structural pattern matching (`match`/`case` statements)
- Use `from __future__ import annotations` in source files for forward compatibility
- When writing tests with inline class definitions, use `Optional[T]` not `T | None`
- If you're unsure about a feature's Python version, check the documentation first

## Documentation

- Keep README files short and concise
- Avoid bloat like detailed project structures that require maintenance
- Focus on essential information: what, why, how to install, how to use
- Let the code and file structure speak for itself
- No repetitive explanations or over-detailed examples

## Code Quality

- Use ruff for linting and formatting (already configured)
- Run quality checks before committing
- Write meaningful commit messages
- Keep dependencies minimal and justified

## Type Checking

- Fix type errors properly, don't use `# type: ignore` as a shortcut
- Only use `# type: ignore` as a last resort when:
  - The library has incorrect type stubs
  - There's a known mypy limitation with a specific pattern
  - The fix would require major refactoring that's out of scope
- When using `# type: ignore`, add a comment explaining why it's necessary
- Prefer proper type annotations, type narrowing, and refactoring over ignoring errors

## Testing

- Write tests for new functionality
- Keep tests simple and focused
- Use pytest conventions
- Always use AAA pattern for the tests

## Communication

- Be direct and technical
- Avoid unnecessary verbosity
- Focus on actionable information
- No marketing language or hype

## Git Workflow

- Don't make commits unless explicitly asked
- Respect existing branch structure
- Keep changes atomic and logical

## Before Submitting Changes

Run checks in this order:

1. **Auto-fix issues:** `make fix` - Fixes linting and formatting automatically
2. **Run comprehensive checks:** `make check` - Validates config, tests build, checks linting/formatting, runs tests
3. **Type check:** `make type-check` - Run Pyright type checker (not included in `make check`)

Or run each step individually:
- `make validate-pyproject` - Verify pyproject.toml syntax
- `make lint` - Check for linting issues (read-only)
- `make format` - Auto-format code
- `make test` - Run test suite
- `make type-check` - Check type annotations

Note: `make check` does NOT auto-fix issues or run type checking. Always run `make fix` first, then `make check` and `make type-check`.

## SQL Schema Reference

**CRITICAL**: Before making ANY SQL-related changes, modifications, or additions to this codebase, you MUST first consult the PhasorPoint SQL schema documentation located in `docs/phasor-point-sql/`.

### Documentation Location

All schema documentation is located at: `docs/phasor-point-sql/`

Key files:
- `README.md` - Overview of all tables, naming conventions, and query syntax
- Individual table schema files (e.g., `pmu_data_tables.md`, `bus_measurement_group.md`, etc.)

### Required Workflow for SQL Changes

1. **READ** the relevant schema documentation file(s) from `docs/phasor-point-sql/`
2. **VERIFY** table names, column names, and data types match the documentation
3. **CONFIRM** query patterns follow PhasorPoint SQL supported syntax
4. **IMPLEMENT** changes based on documented schema
5. **TEST** with knowledge of custom JDBC limitations

### Key Schema Conventions

**Table Naming Rules:**
1. Special characters: Spaces and special chars are replaced with underscores (e.g., "Zone 1" becomes "zone_1")
2. Sample Rate Suffix: Many tables require `<X>` suffix (samples per second), e.g., `pmu_1234_50` for 50 Hz data
3. Case Sensitivity: Table names are typically lowercase with underscores

**Column Naming:**
- Timestamp: Always `ts` (local time)
- Resampled Indicator: `resampled` column shows if data rate was changed
- Refer to individual schema files for specific column names per table type

**Query Constraints:**
1. Time Filtering REQUIRED: All queries must filter on `ts` column
2. Supported: `SELECT`, `WHERE` (on `ts`), `ORDER BY`, basic `JOIN`
3. Unsupported: Aggregate functions (COUNT, MIN, MAX, AVG, SUM)

### Enforcement

Before submitting ANY code changes involving SQL:

1. Have you read the relevant schema documentation?
2. Do your table names match the documented naming patterns?
3. Do your column names match the documented schema?
4. Does your query syntax follow PhasorPoint SQL constraints?

**Always consult `docs/phasor-point-sql/` before making SQL-related changes. The documentation is the source of truth for all schema information.**

