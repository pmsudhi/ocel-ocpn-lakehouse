# Publishing Guide

This guide explains how to publish the `ocel-ocpn-lakehouse` package to PyPI.

## Prerequisites

1. **PyPI Account**: Create an account at https://pypi.org/account/register/
2. **TestPyPI Account**: Create an account at https://test.pypi.org/account/register/
3. **API Tokens**: Generate API tokens from your account settings
4. **Build Tools**: Install build and twine

```bash
pip install build twine
```

## Version Management

Before publishing, update the version in `pyproject.toml`:

```toml
[project]
version = "1.0.1"  # Increment as needed
```

Follow [Semantic Versioning](https://semver.org/):
- **MAJOR** (1.0.0 → 2.0.0): Breaking changes
- **MINOR** (1.0.0 → 1.1.0): New features, backward compatible
- **PATCH** (1.0.0 → 1.0.1): Bug fixes, backward compatible

## Building the Package

### 1. Clean Previous Builds

```bash
rm -rf dist/ build/ *.egg-info
```

### 2. Build Source Distribution and Wheel

```bash
python -m build
```

This creates:
- `dist/ocel-ocpn-lakehouse-1.0.0.tar.gz` (source distribution)
- `dist/ocel_ocpn_lakehouse-1.0.0-py3-none-any.whl` (wheel)

### 3. Verify the Build

```bash
# Check the contents
tar -tzf dist/ocel-ocpn-lakehouse-*.tar.gz

# Or install locally to test
pip install dist/ocel_ocpn_lakehouse-*.whl
python -c "import lakehouse; print(lakehouse.__version__)"
```

## Testing on TestPyPI

### 1. Upload to TestPyPI

```bash
python -m twine upload --repository testpypi dist/*
```

You'll be prompted for:
- Username: `__token__`
- Password: Your TestPyPI API token (starts with `pypi-`)

### 2. Test Installation

```bash
pip install --index-url https://test.pypi.org/simple/ ocel-ocpn-lakehouse
```

### 3. Verify Installation

```python
import lakehouse
print(lakehouse.__version__)
```

## Publishing to PyPI

### 1. Final Checks

- [ ] Version number updated
- [ ] README.md is complete and accurate
- [ ] All dependencies are correct
- [ ] Package builds without errors
- [ ] Tests pass (if applicable)
- [ ] Tested on TestPyPI

### 2. Upload to PyPI

```bash
python -m twine upload dist/*
```

You'll be prompted for:
- Username: `__token__`
- Password: Your PyPI API token (starts with `pypi-`)

### 3. Verify Publication

Visit: https://pypi.org/project/ocel-ocpn-lakehouse/

## Post-Publication

### 1. Create Git Tag

```bash
git tag -a v1.0.0 -m "Release version 1.0.0"
git push origin v1.0.0
```

### 2. Update Documentation

- Update installation instructions
- Create release notes
- Update changelog

## Troubleshooting

### Common Issues

1. **"File already exists"**: Version already published, increment version
2. **"Invalid distribution"**: Check MANIFEST.in includes all necessary files
3. **"Missing dependencies"**: Verify all dependencies are listed in pyproject.toml
4. **"Import errors"**: Test installation in clean virtual environment

### Checking Package Contents

```bash
# Extract and inspect
tar -xzf dist/ocel-ocpn-lakehouse-*.tar.gz
cd ocel-ocpn-lakehouse-*/
ls -la
```

## Best Practices

1. **Always test on TestPyPI first**
2. **Use API tokens, not passwords**
3. **Keep version numbers consistent with git tags**
4. **Include comprehensive README and documentation**
5. **Test installation in clean environment before publishing**
6. **Follow semantic versioning**

## Automation (Optional)

You can automate publishing using GitHub Actions. Create `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
  release:
    types: [created]

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - run: pip install build twine
      - run: python -m build
      - run: twine upload dist/*
        env:
          TWINE_USERNAME: __token__
          TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
```

