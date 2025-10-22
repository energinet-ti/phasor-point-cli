.PHONY: help lint format type-check check test clean install dev coverage build

help:
	@echo "Available commands:"
	@echo "  make install     - Install package"
	@echo "  make dev         - Install package with dev dependencies"
	@echo "  make lint        - Run linter (check only)"
	@echo "  make format      - Auto-format code"
	@echo "  make type-check  - Run type checker (Pyright)"
	@echo "  make fix         - Auto-fix linting issues and format code"
	@echo "  make check       - Run all checks (lint + format + type-check + tests)"
	@echo "  make test        - Run tests"
	@echo "  make coverage    - Run tests with coverage report"
	@echo "  make build       - Build wheel distribution package"
	@echo "  make clean       - Remove build artifacts and cache files"

install:
	pip install -e .

dev:
	pip install -e '.[dev]'

lint:
	ruff check --no-cache src/ tests/

format:
	ruff format src/ tests/

type-check:
	@echo "Running type checker..."
	pyright src/ tests/

fix:
	ruff check --no-cache --fix src/ tests/
	ruff format src/ tests/

check:
	@echo "Running comprehensive checks..."
	ruff check --no-cache src/ tests/
	ruff format --check src/ tests/
	pytest
	@echo ""
	@echo "Note: Type checking available with 'make type-check'"

test:
	pytest

coverage:
	@echo "Running tests with coverage report..."
	pytest --cov=src/phasor_point_cli --cov-report=term-missing --cov-report=html
	@echo ""
	@echo "HTML coverage report generated in htmlcov/index.html"

build:
	@echo "Building wheel distribution package..."
	pip install --upgrade build
	python -m build
	@echo ""
	@echo "Build complete! Distribution files created in dist/"
	@ls -lh dist/

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

