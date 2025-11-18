"""
Data ingestion and bootstrap utilities.

This module provides functions for bootstrapping Iceberg tables,
loading OCEL/OCPN data, and managing catalog configurations.
"""

from .production_bootstrap import (
    bootstrap_lakehouse,
    load_catalog_from_yaml,
    create_ocel_schemas,
    create_ocpn_schemas
)

# Check if classes exist before importing
try:
    from .complete_ocel_loader import CompleteOCELLoader
except ImportError:
    CompleteOCELLoader = None

try:
    from .complete_ocpn_generator import CompleteOCPNGenerator
except ImportError:
    CompleteOCPNGenerator = None

from .pm4py_iceberg_loader import PM4PyIcebergLoader

__all__ = [
    "bootstrap_lakehouse",
    "load_catalog_from_yaml",
    "create_ocel_schemas",
    "create_ocpn_schemas",
    "PM4PyIcebergLoader",
]

# Conditionally add classes if they exist
if CompleteOCELLoader is not None:
    __all__.append("CompleteOCELLoader")
if CompleteOCPNGenerator is not None:
    __all__.append("CompleteOCPNGenerator")

