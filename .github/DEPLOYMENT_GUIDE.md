# GitHub Pages Deployment Guide

This guide explains how to configure GitHub Pages for the DataQuery SDK documentation.

## GitHub Repository Settings

To enable GitHub Pages deployment for this repository, follow these steps:

### 1. Enable GitHub Pages

1. Go to your repository on GitHub: `https://github.com/dataquery/dataquery-sdk`
2. Click on **Settings** tab
3. Scroll down to **Pages** section in the left sidebar
4. Under **Source**, select **GitHub Actions**
5. Save the configuration

### 2. Verify Deployment

After pushing changes to the `main` branch that affect documentation:

1. Go to the **Actions** tab in your repository
2. Look for the "Deploy Documentation" workflow
3. Verify it completes successfully
4. Visit your documentation site at: `https://dataquery.github.io/dataquery-sdk/`

## Automatic Deployment

The documentation will automatically deploy when:

- Changes are pushed to the `main` branch that affect:
  - Files in the `docs/` directory
  - The `mkdocs.yml` file
  - The documentation workflow file (`.github/workflows/docs.yml`)

## Manual Deployment

You can also trigger a manual deployment:

1. Go to the **Actions** tab
2. Select "Deploy Documentation" workflow
3. Click **Run workflow**
4. Select the `main` branch
5. Click **Run workflow**

## Troubleshooting

### Common Issues

1. **Workflow fails with permissions error**
   - Ensure the repository has Pages enabled
   - Verify that Actions have write permissions to deploy Pages

2. **Documentation not updating**
   - Check that the workflow completed successfully
   - Verify changes were pushed to the `main` branch
   - GitHub Pages may take a few minutes to update

3. **Links not working**
   - Ensure all internal links use relative paths
   - Check that all referenced files exist in the `docs/` directory

### Build Locally

To test the documentation build locally:

```bash
# Install dependencies
uv sync --dev

# Build and serve locally
uv run mkdocs serve

# Build for production (same as CI)
uv run mkdocs build --clean --strict
```

The documentation will be available at `http://127.0.0.1:8000` when serving locally.

## File Structure

```
sdk/
├── .github/
│   └── workflows/
│       └── docs.yml          # GitHub Actions workflow for deployment
├── docs/                     # Documentation source files
│   ├── index.md             # Homepage
│   ├── getting-started/     # Getting started guides
│   ├── user-guide/          # User guides
│   ├── api/                 # API reference
│   └── examples/            # Examples
├── mkdocs.yml               # MkDocs configuration
└── site/                    # Generated site (gitignored)
```

## Configuration Files

- **mkdocs.yml**: Main configuration for MkDocs with Material theme
- **.github/workflows/docs.yml**: GitHub Actions workflow for automated deployment
- **docs/**: All documentation source files in Markdown format

The documentation uses the Material for MkDocs theme with additional plugins for:
- Code syntax highlighting
- API documentation generation from docstrings
- Search functionality
- Social links and navigation