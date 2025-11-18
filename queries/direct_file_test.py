#!/usr/bin/env python3
"""
Direct file test - bypass Iceberg metadata and read Parquet files directly
"""

import daft
from pathlib import Path
import sys
import os

# Fix Unicode display issues on Windows
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"


def test_direct_files():
    """Test reading Parquet files directly without Iceberg metadata."""
    print("Testing direct Parquet file reading...")
    
    # Test events
    events_dir = Path("warehouse/ocel/events/data")
    if events_dir.exists():
        parquet_files = list(events_dir.rglob("*.parquet"))
        if parquet_files:
            print(f"Found {len(parquet_files)} Parquet files in events")
            
            # Read the actual data file (not the empty directory)
            data_files = [f for f in parquet_files if f.stat().st_size > 0]
            if data_files:
                print(f"Reading from: {data_files[0]}")
                try:
                    df = daft.read_parquet(str(data_files[0]))
                    print("Events data loaded successfully!")
                    print("Sample events (first 3 rows):")
                    try:
                        df.show(3)
                    except UnicodeEncodeError:
                        print("Data loaded but contains Unicode characters that can't be displayed in console")
                        print("This is normal for international data - the data is accessible programmatically")
                    
                    # Test aggregation
                    print("\nEvent types count:")
                    try:
                        event_counts = df.groupby("type").agg(daft.col("id").count())
                        event_counts.show()
                    except UnicodeEncodeError:
                        print("Aggregation successful but results contain Unicode characters")
                    except Exception as e:
                        print(f"Aggregation error: {e}")
                    
                except Exception as e:
                    print(f"Error reading events: {e}")
            else:
                print("No data files found (all files are 0 bytes)")
        else:
            print("No Parquet files found in events directory")
    else:
        print("Events directory not found")
    
    # Test relationships
    rels_dir = Path("warehouse/ocel/event_objects/data")
    if rels_dir.exists():
        parquet_files = list(rels_dir.rglob("*.parquet"))
        data_files = [f for f in parquet_files if f.stat().st_size > 0]
        if data_files:
            print(f"\nReading relationships from: {data_files[0]}")
            try:
                df = daft.read_parquet(str(data_files[0]))
                print("Relationships data loaded successfully!")
                try:
                    df.show(5)
                except UnicodeEncodeError:
                    print("Data loaded but contains Unicode characters that can't be displayed")
            except Exception as e:
                print(f"Error reading relationships: {e}")
    
    # Test OCPN models
    models_dir = Path("warehouse/ocpn/models/data")
    if models_dir.exists():
        parquet_files = list(models_dir.rglob("*.parquet"))
        data_files = [f for f in parquet_files if f.stat().st_size > 0]
        if data_files:
            print(f"\nReading OCPN models from: {data_files[0]}")
            try:
                df = daft.read_parquet(str(data_files[0]))
                print("OCPN models data loaded successfully!")
                try:
                    df.show()
                except UnicodeEncodeError:
                    print("Data loaded but contains Unicode characters that can't be displayed")
            except Exception as e:
                print(f"Error reading OCPN models: {e}")


if __name__ == '__main__':
    test_direct_files()
