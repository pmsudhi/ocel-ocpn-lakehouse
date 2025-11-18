#!/usr/bin/env python3
"""
Proper Iceberg + Daft integration following best practices
"""

import daft
import json
from pathlib import Path
from datetime import datetime
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


def load_ocel_to_iceberg_proper(ocel_file_path: Path, catalog_file: Path):
    """Load OCEL data to Iceberg using proper Daft + Iceberg integration."""
    print(f"Loading OCEL data from {ocel_file_path} into Iceberg using proper integration...")
    
    # Load catalog
    cat = load_catalog_from_yaml('local', catalog_file)
    
    # Load OCEL data
    with open(ocel_file_path, 'r', encoding='utf-8') as f:
        ocel_data = json.load(f)
    
    print(f"Loaded OCEL with {len(ocel_data['events'])} events and {len(ocel_data['objects'])} objects")
    
    # Process events
    print("Processing events...")
    events_data = []
    for event in ocel_data['events']:
        # Parse timestamp properly
        time_str = event['time']
        if time_str.endswith('Z'):
            # Remove timezone info for PyArrow compatibility
            time_str = time_str[:-1]
        
        event_dict = {
            'id': event['id'],
            'type': event['type'],
            'time': time_str,
            'event_date': event['time'][:10],  # Extract date
            'event_month': event['time'][:7],  # Extract YYYY-MM
        }
        
        # Add hot denormalized keys if available
        for attr in event.get('attributes', []):
            if attr['name'] in ['vendor_code', 'request_id']:
                event_dict[attr['name']] = attr['value']
        
        events_data.append(event_dict)
    
    # Create Daft DataFrame for events
    df_events = daft.from_pylist(events_data)
    print(f"Created events DataFrame with {len(events_data)} rows")
    
    # Write to Iceberg using proper integration
    try:
        events_table = cat.load_table('ocel.events')
        print("Writing events to Iceberg table...")
        df_events.write_iceberg(events_table, mode="append")
        print("[OK] Events written to Iceberg successfully!")
    except Exception as e:
        print(f"[ERROR] Error writing events to Iceberg: {e}")
        return False
    
    # Process event-object relationships
    print("Processing event-object relationships...")
    relationships_data = []
    for event in ocel_data['events']:
        for rel in event.get('relationships', []):
            relationships_data.append({
                'event_id': event['id'],
                'object_id': rel['objectId'],
                'qualifier': rel.get('qualifier', '')
            })
    
    if relationships_data:
        df_relationships = daft.from_pylist(relationships_data)
        print(f"Created relationships DataFrame with {len(relationships_data)} rows")
        
        try:
            rels_table = cat.load_table('ocel.event_objects')
            print("Writing relationships to Iceberg table...")
            df_relationships.write_iceberg(rels_table, mode="append")
            print("[OK] Relationships written to Iceberg successfully!")
        except Exception as e:
            print(f"[ERROR] Error writing relationships to Iceberg: {e}")
    
    # Process objects
    print("Processing objects...")
    objects_data = []
    for obj in ocel_data['objects']:
        objects_data.append({
            'id': obj['id'],
            'type': obj['type'],
            'created_at': obj.get('created_at', ''),
            'lifecycle_state': obj.get('lifecycle_state', '')
        })
    
    if objects_data:
        df_objects = daft.from_pylist(objects_data)
        print(f"Created objects DataFrame with {len(objects_data)} rows")
        
        try:
            objects_table = cat.load_table('ocel.objects')
            print("Writing objects to Iceberg table...")
            df_objects.write_iceberg(objects_table, mode="append")
            print("[OK] Objects written to Iceberg successfully!")
        except Exception as e:
            print(f"[ERROR] Error writing objects to Iceberg: {e}")
    
    print("[OK] OCEL data successfully loaded to Iceberg using proper Daft integration!")
    return True


def main():
    """Main function to load OCEL data to Iceberg."""
    ocel_file = Path('data/purchase_to_pay_ocel_v2.json').resolve()
    catalog_file = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    
    if not ocel_file.exists():
        print(f"[ERROR] OCEL file not found: {ocel_file}")
        return
    
    if not catalog_file.exists():
        print(f"[ERROR] Catalog config not found: {catalog_file}")
        return
    
    success = load_ocel_to_iceberg_proper(ocel_file, catalog_file)
    if success:
        print("\n[SUCCESS] OCEL data properly loaded to Iceberg with Daft integration!")
    else:
        print("\n[FAILED] Could not load OCEL data to Iceberg")


if __name__ == '__main__':
    main()
