# Installation

## Requirements

- Python 3.9 or higher
- pip or uv package manager

## Install from PyPI

The recommended way to install DataQuery SDK is using pip:

```bash
pip install dataquery-sdk
```

## Install with uv

If you prefer using uv (faster and more reliable):

```bash
uv add dataquery-sdk
```

## Install from Source

For development or to get the latest features:

```bash
# Clone the repository
git clone https://github.com/your-org/dataquery-sdk.git
cd dataquery-sdk

# Install in development mode
pip install -e .

# Or with uv
uv sync --all-extras --dev
```

## Verify Installation

Test your installation:

```python
import dataquery
print(f"DataQuery SDK version: {dataquery.__version__}")
```

## Optional Dependencies

DataQuery SDK includes optional dependencies for enhanced functionality:

- **`[dev]`**: Development tools (testing, linting, documentation)
- **`[docs]`**: Documentation building tools
- **`[all]`**: All optional dependencies

Install with extras:

```bash
pip install dataquery-sdk[dev,docs]
# or
uv add "dataquery-sdk[dev,docs]"
```

## Docker

You can also run DataQuery SDK in a Docker container:

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "your_script.py"]
```

## Troubleshooting

### Common Issues

**ImportError: No module named 'dataquery'**
- Ensure you're using the correct Python environment
- Try reinstalling: `pip uninstall dataquery-sdk && pip install dataquery-sdk`

**Permission Denied**
- Use `--user` flag: `pip install --user dataquery-sdk`
- Or use a virtual environment

**SSL Certificate Issues**
- Update certificates: `pip install --upgrade certifi`
- Or use `--trusted-host` for internal repositories

### Getting Help

If you encounter issues:

1. Check the [configuration guide](configuration.md) for setup options
2. Search [existing issues](https://github.com/your-org/dataquery-sdk/issues)
3. Create a [new issue](https://github.com/your-org/dataquery-sdk/issues/new) with details
