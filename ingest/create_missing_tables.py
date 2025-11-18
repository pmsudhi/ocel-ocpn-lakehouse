#!/usr/bin/env python3
"""
Create missing dimension tables that failed in bootstrap
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


def create_missing_tables():
    """Create missing dimension tables."""
    print("Creating missing dimension tables...")
    
    # Load catalog
    catalog_cfg = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_cfg)
    
    # Create event_types table
    try:
        event_types_schema = Schema(
            NestedField(1, 'name', StringType(), required=True, doc='Primary key - event type name'),
            NestedField(2, 'description', StringType(), required=False, doc='Human-readable description')
        )
        
        cat.create_table(
            'ocel.event_types',
            schema=event_types_schema,
            properties={
                'format-version': '2',
                'write.target-file-size-bytes': '134217728'  # 128MB for small dimension
            }
        )
        print("[OK] Created ocel.event_types table")
    except Exception as e:
        print(f"[WARNING] ocel.event_types may already exist: {e}")
    
    # Create object_types table
    try:
        object_types_schema = Schema(
            NestedField(1, 'name', StringType(), required=True, doc='Primary key - object type name'),
            NestedField(2, 'description', StringType(), required=False, doc='Human-readable description')
        )
        
        cat.create_table(
            'ocel.object_types',
            schema=object_types_schema,
            properties={
                'format-version': '2',
                'write.target-file-size-bytes': '134217728'  # 128MB for small dimension
            }
        )
        print("[OK] Created ocel.object_types table")
    except Exception as e:
        print(f"[WARNING] ocel.object_types may already exist: {e}")
    
    # Create log_metadata table
    try:
        log_metadata_schema = Schema(
            NestedField(1, 'key', StringType(), required=True, doc='Metadata key'),
            NestedField(2, 'value', StringType(), required=True, doc='Metadata value'),
            NestedField(3, 'updated_at', TimestampType(), required=True, doc='Last update timestamp')
        )
        
        cat.create_table(
            'ocel.log_metadata',
            schema=log_metadata_schema,
            properties={
                'format-version': '2',
                'write.target-file-size-bytes': '134217728'  # 128MB for small metadata
            }
        )
        print("[OK] Created ocel.log_metadata table")
    except Exception as e:
        print(f"[WARNING] ocel.log_metadata may already exist: {e}")
    
    print("[SUCCESS] Missing tables created!")


if __name__ == '__main__':
    create_missing_tables()
