#!/usr/bin/env python3
"""
Create missing OCPN tables that failed in bootstrap
"""

from pathlib import Path
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import *
from pyiceberg.schema import NestedField


def load_catalog_from_yaml(catalog_name: str, config_path: Path):
    """Load catalog from YAML config."""
    cfg = {}
    with config_path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if ':' in line:
                k, v = line.split(':', 1)
                cfg[k.strip()] = v.strip()
    return load_catalog(catalog_name, **cfg)


def create_missing_ocpn_tables():
    """Create missing OCPN tables."""
    print("Creating missing OCPN tables...")
    
    # Load catalog
    catalog_cfg = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_cfg)
    
    # Create ocpn.markings table
    try:
        markings_schema = Schema(
            NestedField(1, 'model_id', StringType(), required=True, doc='Foreign key to models'),
            NestedField(2, 'place_id', StringType(), required=True, doc='Foreign key to places'),
            NestedField(3, 'tokens', LongType(), required=True, doc='Number of tokens in the place'),
            NestedField(4, 'marking_type', StringType(), required=True, doc='Marking type (initial, final, etc.)')
        )
        
        cat.create_table(
            'ocpn.markings',
            schema=markings_schema,
            properties={
                'format-version': '2',
                'write.target-file-size-bytes': '134217728'  # 128MB
            }
        )
        print("[OK] Created ocpn.markings table")
    except Exception as e:
        print(f"[WARNING] ocpn.markings may already exist: {e}")
    
    # Create ocpn.layout table
    try:
        layout_schema = Schema(
            NestedField(1, 'model_id', StringType(), required=True, doc='Foreign key to models'),
            NestedField(2, 'element_type', StringType(), required=True, doc='Element type (place, transition)'),
            NestedField(3, 'element_id', StringType(), required=True, doc='Element ID within the model'),
            NestedField(4, 'x', DoubleType(), required=True, doc='X coordinate for visualization'),
            NestedField(5, 'y', DoubleType(), required=True, doc='Y coordinate for visualization')
        )
        
        cat.create_table(
            'ocpn.layout',
            schema=layout_schema,
            properties={
                'format-version': '2',
                'write.target-file-size-bytes': '134217728'  # 128MB
            }
        )
        print("[OK] Created ocpn.layout table")
    except Exception as e:
        print(f"[WARNING] ocpn.layout may already exist: {e}")
    
    print("[SUCCESS] Missing OCPN tables created!")


if __name__ == '__main__':
    create_missing_ocpn_tables()
