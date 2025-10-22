.PHONY: help lint format check test clean install dev coverage

help:
	@echo "Available commands:"
	@echo "  make install     - Install package"
	@echo "  make dev         - Install package with dev dependencies"
	@echo "  make lint        - Run linter (check only)"
	@echo "  make format      - Auto-format code"
	@echo "  make fix         - Auto-fix linting issues and format code"
	@echo "  make check       - Run all checks (lint + format + tests)"
	@echo "  make test        - Run tests"
	@echo "  make coverage    - Run tests with coverage report"
	@echo "  make clean       - Remove build artifacts and cache files"

install:
	pip install -e .

dev:
	pip install -e '.[dev]'

lint:
	ruff check --no-cache src/ tests/

format:
	ruff format src/ tests/

fix:
	ruff check --no-cache --fix src/ tests/
	ruff format src/ tests/

check:
	@echo "Running comprehensive checks..."
	ruff check --no-cache src/ tests/
	ruff format --check src/ tests/
	pytest

test:
	pytest

coverage:
	@echo "Running tests with coverage report..."
	pytest --cov=src/phasor_point_cli --cov-report=term-missing --cov-report=html
	@echo ""
	@echo "HTML coverage report generated in htmlcov/index.html"

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/
	rm -rf .coverage
	rm -f coverage.xml
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

