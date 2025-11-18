#!/usr/bin/env python3
"""
Test queries to validate the OCEL/OCPN data in Iceberg tables using Daft
"""

import daft
from pathlib import Path
from pyiceberg.catalog import load_catalog


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


def test_ocel_queries():
    """Test OCEL queries with Daft."""
    print("Testing OCEL queries with Daft...")
    
    # Load catalog
    catalog_cfg = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_cfg)
    
    # Test events table
    try:
        events_table = cat.load_table('ocel.events')
        print(f"Events table location: {events_table.location()}")
        
        # Get data files using correct PyIceberg API
        scan = events_table.scan()
        # Use the correct method to get file tasks
        file_tasks = scan.plan_files()
        files = []
        for task in file_tasks:
            for file in task.files:
                files.append(file.file_path)
        print(f"Found {len(files)} data files")
        
        if files:
            # Read with Daft
            df = daft.read_parquet(files)
            print(f"Events DataFrame shape: {df.shape()}")
            print("Sample events:")
            df.show(5)
            
            # Test aggregation
            print("\nEvent types count:")
            event_counts = df.groupby("type").agg({"id": "count"})
            event_counts.show()
            
    except Exception as e:
        print(f"Error querying events: {e}")
    
    # Test event_objects table
    try:
        rels_table = cat.load_table('ocel.event_objects')
        scan = rels_table.scan()
        file_tasks = scan.plan_files()
        files = []
        for task in file_tasks:
            for file in task.files:
                files.append(file.file_path)
        if files:
            df = daft.read_parquet(files)
            print(f"\nEvent-Object relationships: {len(df)} rows")
            df.show(5)
    except Exception as e:
        print(f"Error querying relationships: {e}")


def test_ocpn_queries():
    """Test OCPN queries with Daft."""
    print("\nTesting OCPN queries with Daft...")
    
    # Load catalog
    catalog_cfg = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_cfg)
    
    try:
        models_table = cat.load_table('ocpn.models')
        scan = models_table.scan()
        file_tasks = scan.plan_files()
        files = []
        for task in file_tasks:
            for file in task.files:
                files.append(file.file_path)
        if files:
            df = daft.read_parquet(files)
            print(f"OCPN Models: {len(df)} rows")
            df.show()
            
        places_table = cat.load_table('ocpn.places')
        scan = places_table.scan()
        file_tasks = scan.plan_files()
        files = []
        for task in file_tasks:
            for file in task.files:
                files.append(file.file_path)
        if files:
            df = daft.read_parquet(files)
            print(f"OCPN Places: {len(df)} rows")
            df.show()
            
        transitions_table = cat.load_table('ocpn.transitions')
        scan = transitions_table.scan()
        file_tasks = scan.plan_files()
        files = []
        for task in file_tasks:
            for file in task.files:
                files.append(file.file_path)
        if files:
            df = daft.read_parquet(files)
            print(f"OCPN Transitions: {len(df)} rows")
            df.show()
            
    except Exception as e:
        print(f"Error querying OCPN: {e}")


def main():
    """Run all test queries."""
    print("=" * 60)
    print("DAFT QUERY VALIDATION FOR OCEL/OCPN LAKEHOUSE")
    print("=" * 60)
    
    test_ocel_queries()
    test_ocpn_queries()
    
    print("\n" + "=" * 60)
    print("QUERY VALIDATION COMPLETED!")
    print("=" * 60)


if __name__ == '__main__':
    main()
