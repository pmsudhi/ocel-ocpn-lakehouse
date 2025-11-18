# Installation Guide

## Installing the Package

### Using pip

```bash
# Install from PyPI (when published)
pip install ocel-ocpn-lakehouse

# Install from local source
cd lakehouse
pip install .

# Install in development mode
pip install -e .
```

### Using uv (Recommended)

```bash
# Install from PyPI (when published)
uv pip install ocel-ocpn-lakehouse

# Install from local source
cd lakehouse
uv pip install .

# Install in development mode
uv pip install -e .
```

### Building the Package

```bash
# Build source distribution
python -m build

# Build wheel
python -m build --wheel

# Both
python -m build
```

The built packages will be in the `dist/` directory.

## Publishing to PyPI

### Test Publishing (TestPyPI)

```bash
# Build the package
python -m build

# Upload to TestPyPI
python -m twine upload --repository testpypi dist/*

# Test installation
pip install --index-url https://test.pypi.org/simple/ ocel-ocpn-lakehouse
```

### Production Publishing

```bash
# Build the package
python -m build

# Upload to PyPI
python -m twine upload dist/*
```

## Development Setup

```bash
# Clone the repository
git clone <repository-url>
cd ProcessMining/lakehouse

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"

# Or with uv
uv pip install -e ".[dev]"
```

## Dependencies

### Core Dependencies

- `pm4py>=2.7.0` - Process mining library
- `pyiceberg>=0.10.0` - Apache Iceberg Python client
- `daft[pandas,aws]>=0.6.7` - Query engine
- `pandas>=1.5.0` - Data manipulation
- `numpy>=1.21.0` - Numerical computing

### Optional Dependencies

- `streamlit>=1.20.0` - For dashboard features (install with `[dashboard]`)
- Development tools (install with `[dev]`)

## Verification

After installation, verify the package:

```python
import lakehouse
print(lakehouse.__version__)

# Test imports
from lakehouse import (
    ProcessMiningQueryEngine,
    bootstrap_lakehouse,
    PM4PyAnalyticsWrapper
)
print("Package installed successfully!")
```

