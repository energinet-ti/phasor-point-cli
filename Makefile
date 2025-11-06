.PHONY: help lint format type-check check test clean install dev coverage build validate-pyproject

help:
	@echo "Available commands:"
	@echo "  make install             - Install package"
	@echo "  make setup               - Run setup script"
	@echo "  make dev                 - Install package with dev dependencies"
	@echo "  make lint                - Run linter (check only)"
	@echo "  make format              - Auto-format code"
	@echo "  make type-check          - Run type checker (Pyright)"
	@echo "  make validate-pyproject  - Validate pyproject.toml"
	@echo "  make fix                 - Auto-fix linting issues and format code"
	@echo "  make check               - Run all checks (lint + format + validate + tests)"
	@echo "  make test                - Run tests"
	@echo "  make coverage            - Run tests with coverage report"
	@echo "  make build               - Build wheel distribution package"
	@echo "  make clean               - Remove build artifacts and cache files"

# Run setup script
setup:
	@bash scripts/setup.sh

install:
	./venv/bin/pip install -e .

dev:
	./venv/bin/pip install -e '.[dev]'

lint:
	./venv/bin/ruff check --no-cache src/ tests/

format:
	./venv/bin/ruff format src/ tests/

type-check:
	@echo "Running type checker..."
	PYRIGHT_PYTHON_FORCE_VERSION=latest ./venv/bin/pyright src/ tests/

validate-pyproject:
	@echo "Validating pyproject.toml..."
	./venv/bin/validate-pyproject pyproject.toml

fix:
	./venv/bin/ruff check --no-cache --fix src/ tests/
	./venv/bin/ruff format src/ tests/

check:
	@echo "Running comprehensive checks..."
	@echo "1. Validating pyproject.toml..."
	./venv/bin/validate-pyproject pyproject.toml
	@echo "2. Testing package build with isolated env (matches CI behavior)..."
	./venv/bin/python -m build --wheel --outdir /tmp/phasor-build-check 2>&1 | tee /tmp/phasor-build.log
	@rm -rf /tmp/phasor-build-check /tmp/phasor-build.log
	@echo "3. Running linter..."
	./venv/bin/ruff check --no-cache src/ tests/
	@echo "4. Checking code formatting..."
	./venv/bin/ruff format --check src/ tests/
	@echo "5. Running tests..."
	./venv/bin/pytest
	@echo ""
	@echo "All checks passed!"
	@echo "Note: Type checking available with 'make type-check'"

test:
	./venv/bin/pytest

coverage:
	@echo "Running tests with coverage report..."
	./venv/bin/pytest --cov=src/phasor_point_cli --cov-report=term-missing --cov-report=html
	@echo ""
	@echo "HTML coverage report generated in htmlcov/index.html"

build:
	@echo "Building wheel distribution package..."
	./venv/bin/pip install --upgrade build
	./venv/bin/python -m build
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

