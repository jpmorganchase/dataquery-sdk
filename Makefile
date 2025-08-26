# Makefile for DATAQUERY SDK

.PHONY: help install install-dev test test-cov lint format type-check clean build dist publish publish-test docs examples security-test audit ci-check

# Default target
help:
	@echo "DATAQUERY SDK - Available Commands:"
	@echo ""
	@echo "Development:"
	@echo "  install      - Install package in development mode"
	@echo "  install-dev  - Install package with development dependencies"
	@echo "  test         - Run tests"
	@echo "  test-cov     - Run tests with coverage"
	@echo "  lint         - Run linting (flake8)"
	@echo "  format       - Format code (black + isort)"
	@echo "  type-check   - Run type checking (mypy)"
	@echo "  security-test - Run security tests"
	@echo ""
	@echo "Building:"
	@echo "  clean        - Clean build artifacts"
	@echo "  build        - Build package"
	@echo "  dist         - Create distribution files"
	@echo ""
	@echo "Publishing:"
	@echo "  publish-test - Publish to TestPyPI"
	@echo "  publish      - Publish to PyPI"
	@echo ""
	@echo "Documentation:"
	@echo "  docs         - Build documentation"
	@echo ""
	@echo "Examples:"
	@echo "  examples     - Run all examples"
	@echo ""
	@echo "Utilities:"
	@echo "  clean-all    - Clean all generated files"
	@echo "  check-all    - Run all checks (lint, type-check, test)"

# Development
install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	pytest tests/ -v

test-cov:
	pytest tests/ --cov=dataquery --cov-report=term-missing --cov-report=html

security-test:
	pytest tests/test_security.py -v

lint:
	flake8 dataquery/

format:
	black dataquery/ tests/ examples/
	isort dataquery/ tests/ examples/

type-check:
	mypy dataquery/

# Security audit using pip-audit
audit:
	pip-audit || true

# Building
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -f coverage.xml
	rm -f .coverage

clean-all: clean
	find . -type d -name '__pycache__' -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
	find . -type f -name '*.pyo' -delete
	find . -type f -name '*.log' -delete
	rm -rf downloads/
	rm -rf .tokens/
	rm -rf oauth_tokens/

build:
	python -m build

dist: clean build

# Publishing
publish-test: dist
	twine upload --repository testpypi dist/*

publish: dist
	twine upload dist/*

# Documentation
docs:
	cd docs && make html

# Examples
examples:
	@echo "Running examples..."
	@for example in examples/*.py; do \
		if [ -f "$$example" ] && [ "$$example" != "examples/run_all_examples.py" ]; then \
			echo "Running $$example..."; \
			python "$$example" --help || true; \
		fi; \
	done

# Quality checks
check-all: lint type-check audit test

# Continuous integration entry: run lint, type-check, security audit, and tests with coverage
ci-check: install-dev lint type-check audit test-cov

# UV commands (if using uv)
uv-sync:
	uv sync --dev

uv-install:
	uv pip install -e .

uv-dev:
	uv pip install -e ".[dev]"

uv-test:
	uv run pytest tests/ -v

uv-lint:
	uv run flake8 dataquery/ tests/ examples/

uv-format:
	uv run black dataquery/ tests/ examples/
	uv run isort dataquery/ tests/ examples/

uv-type-check:
	uv run mypy dataquery/

uv-build:
	uv build

uv-publish-test: uv-build
	uv run twine upload --repository testpypi dist/*

uv-publish: uv-build
	uv run twine upload dist/*

# Pre-release checks
pre-release: clean-all install-dev check-all security-test dist
	@echo "✅ Pre-release checks completed successfully!"
	@echo "📦 Distribution files created in dist/"
	@echo "🚀 Ready for publishing!"

# Release workflow
release: pre-release
	@echo "🎉 Release workflow completed!"
	@echo "📋 Next steps:"
	@echo "   1. Review the distribution files in dist/"
	@echo "   2. Test on TestPyPI: make publish-test"
	@echo "   3. Publish to PyPI: make publish"
	@echo "   4. Create a GitHub release"
	@echo "   5. Update version in pyproject.toml and __init__.py"

# Development workflow
dev-setup: install-dev
	@echo "✅ Development environment setup complete!"
	@echo "📋 Available commands:"
	@echo "   make test        - Run tests"
	@echo "   make lint        - Run linting"
	@echo "   make format      - Format code"
	@echo "   make type-check  - Run type checking"
	@echo "   make check-all   - Run all checks"

# Quick start
quick-start: dev-setup
	@echo "🚀 Quick start setup complete!"
	@echo "📝 Create a .env file with your configuration:"
	@echo "   cp examples/env.template .env"
	@echo "   # Edit .env with your API credentials"
	@echo ""
	@echo "🧪 Test your setup:"
	@echo "   python -c \"from dataquery import DataQuery; print('✅ SDK imported successfully!')\"" 