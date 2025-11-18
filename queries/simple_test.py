#!/usr/bin/env python3
"""
Simple test to validate data is accessible
"""

import daft
from pathlib import Path


def test_direct_parquet():
    """Test reading Parquet files directly with Daft."""
    print("Testing direct Parquet reading with Daft...")
    
    # Test events
    events_file = Path("warehouse/ocel/events/data/ocel_events_1760997257.639759.parquet")
    if events_file.exists():
        print(f"Reading events from: {events_file}")
        df = daft.read_parquet(str(events_file))
        print("Sample events:")
        df.show(5)
        
        # Test aggregation
        print("\nEvent types:")
        event_counts = df.groupby("type").agg({"id": "count"})
        event_counts.show()
    else:
        print("Events file not found")
    
    # Test relationships
    rels_file = Path("warehouse/ocel/event_objects/data/ocel_event_objects_1760997261.507563.parquet")
    if rels_file.exists():
        print(f"\nReading relationships from: {rels_file}")
        df = daft.read_parquet(str(rels_file))
        df.show(5)
    else:
        print("Relationships file not found")
    
    # Test OCPN models
    models_file = Path("warehouse/ocpn/models/data/ocpn_models_1760997322.775043.parquet")
    if models_file.exists():
        print(f"\nReading OCPN models from: {models_file}")
        df = daft.read_parquet(str(models_file))
        df.show()
    else:
        print("Models file not found")


if __name__ == '__main__':
    test_direct_parquet()
