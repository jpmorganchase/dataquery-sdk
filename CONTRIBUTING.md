# Contributing to DataQuery SDK

Thank you for your interest in contributing to the DataQuery SDK! This document provides guidelines and information for contributors.

## Getting Started

### Prerequisites

- Python 3.10 or higher
- [uv](https://github.com/astral-sh/uv) package manager
- Git

### Development Setup

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/your-username/dataquery-sdk.git
   cd dataquery-sdk
   ```

3. Install the project in development mode:
   ```bash
   uv sync --all-extras --dev
   ```

4. Run the tests to ensure everything is working:
   ```bash
   uv run pytest tests/ -v
   ```

## Development Workflow

### Making Changes

1. Create a new branch for your feature or bugfix:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b bugfix/your-bugfix-name
   ```

2. Make your changes following the coding standards (see below)

3. Add tests for your changes:
   - Unit tests for new functionality
   - Integration tests for API changes
   - Update existing tests if needed

4. Run the test suite:
   ```bash
   uv run pytest tests/ -v
   ```

5. Run linting and formatting:
   ```bash
   uv run black .
   uv run isort .
   uv run flake8 dataquery/ tests/
   uv run mypy dataquery/
   ```

6. Commit your changes with a descriptive message:
   ```bash
   git add .
   git commit -m "Add feature: brief description of changes"
   ```

7. Push your branch and create a pull request

### Coding Standards

- **Python Style**: Follow PEP 8
- **Formatting**: Use Black for code formatting
- **Import Sorting**: Use isort for import organization
- **Linting**: Use flake8 for code quality checks
- **Type Hints**: Use mypy for type checking
- **Docstrings**: Follow Google docstring format

### Testing

- Write tests for all new functionality
- Maintain or improve test coverage
- Use descriptive test names
- Test both success and failure cases
- Mock external dependencies

### Documentation

- Update docstrings for new functions/classes
- Update README.md if adding new features
- Update CHANGELOG.md for user-facing changes
- Add examples for new functionality

## Pull Request Process

1. Ensure all tests pass
2. Ensure code follows style guidelines
3. Update documentation as needed
4. Add your changes to CHANGELOG.md
5. Create a pull request with a clear description
6. Request review from maintainers

### Pull Request Template

When creating a pull request, please fill out the template with:
- Description of changes
- Type of change (bug fix, feature, etc.)
- Testing performed
- Any breaking changes
- Additional notes

## Issue Reporting

When reporting bugs or requesting features:

- Use the provided issue templates
- Provide clear reproduction steps for bugs
- Include environment information
- Add code examples when relevant

## Release Process

Releases are handled automatically through GitHub Actions:

1. Create a git tag with version number (e.g., `v1.0.0`)
2. Push the tag to trigger the release workflow
3. The workflow will build, test, and publish to PyPI

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow
- Follow the project's coding standards

## Questions?

If you have questions about contributing, please:
- Open an issue with the "question" label
- Check existing issues and discussions
- Contact the maintainers

Thank you for contributing to DataQuery SDK!
