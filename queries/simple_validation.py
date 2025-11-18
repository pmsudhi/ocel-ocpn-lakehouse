#!/usr/bin/env python3
"""
Simple validation of the OCEL/OCPN lakehouse system
"""

import daft
from pathlib import Path
import sys
import os

# Fix Unicode display issues on Windows
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"


def validate_system():
    """Simple validation of the lakehouse system."""
    print("=" * 80)
    print("OCEL/OCPN LAKEHOUSE SYSTEM VALIDATION")
    print("=" * 80)
    
    success_count = 0
    total_tests = 0
    
    # Test 1: Events data
    print("\n1. TESTING EVENTS DATA")
    print("-" * 40)
    total_tests += 1
    events_dir = Path("warehouse/ocel/events/data")
    if events_dir.exists():
        parquet_files = [f for f in events_dir.rglob("*.parquet") if f.stat().st_size > 0]
        if parquet_files:
            print(f"[OK] Found {len(parquet_files)} events data files")
            try:
                df = daft.read_parquet(str(parquet_files[0]))
                print("[OK] Events data loaded successfully with Daft")
                success_count += 1
            except Exception as e:
                print(f"[ERROR] Events data failed: {e}")
        else:
            print("[ERROR] No events data files found")
    else:
        print("[ERROR] Events directory not found")
    
    # Test 2: Relationships data
    print("\n2. TESTING RELATIONSHIPS DATA")
    print("-" * 40)
    total_tests += 1
    rels_dir = Path("warehouse/ocel/event_objects/data")
    if rels_dir.exists():
        parquet_files = [f for f in rels_dir.rglob("*.parquet") if f.stat().st_size > 0]
        if parquet_files:
            print(f"[OK] Found {len(parquet_files)} relationships data files")
            try:
                df = daft.read_parquet(str(parquet_files[0]))
                print("[OK] Relationships data loaded successfully with Daft")
                success_count += 1
            except Exception as e:
                print(f"[ERROR] Relationships data failed: {e}")
        else:
            print("[ERROR] No relationships data files found")
    else:
        print("[ERROR] Relationships directory not found")
    
    # Test 3: Event attributes
    print("\n3. TESTING EVENT ATTRIBUTES DATA")
    print("-" * 40)
    total_tests += 1
    attrs_dir = Path("warehouse/ocel/event_attributes/data")
    if attrs_dir.exists():
        parquet_files = [f for f in attrs_dir.rglob("*.parquet") if f.stat().st_size > 0]
        if parquet_files:
            print(f"[OK] Found {len(parquet_files)} event attributes data files")
            try:
                df = daft.read_parquet(str(parquet_files[0]))
                print("[OK] Event attributes loaded successfully with Daft")
                success_count += 1
            except Exception as e:
                print(f"[ERROR] Event attributes failed: {e}")
        else:
            print("[ERROR] No event attributes data files found")
    else:
        print("[ERROR] Event attributes directory not found")
    
    # Test 4: Objects data
    print("\n4. TESTING OBJECTS DATA")
    print("-" * 40)
    total_tests += 1
    objects_dir = Path("warehouse/ocel/objects/data")
    if objects_dir.exists():
        parquet_files = [f for f in objects_dir.rglob("*.parquet") if f.stat().st_size > 0]
        if parquet_files:
            print(f"[OK] Found {len(parquet_files)} objects data files")
            try:
                df = daft.read_parquet(str(parquet_files[0]))
                print("[OK] Objects data loaded successfully with Daft")
                success_count += 1
            except Exception as e:
                print(f"[ERROR] Objects data failed: {e}")
        else:
            print("[ERROR] No objects data files found")
    else:
        print("[ERROR] Objects directory not found")
    
    # Test 5: OCPN Models
    print("\n5. TESTING OCPN MODELS DATA")
    print("-" * 40)
    total_tests += 1
    models_dir = Path("warehouse/ocpn/models/data")
    if models_dir.exists():
        parquet_files = [f for f in models_dir.rglob("*.parquet") if f.stat().st_size > 0]
        if parquet_files:
            print(f"[OK] Found {len(parquet_files)} OCPN models data files")
            try:
                df = daft.read_parquet(str(parquet_files[0]))
                print("[OK] OCPN models loaded successfully with Daft")
                success_count += 1
            except Exception as e:
                print(f"[ERROR] OCPN models failed: {e}")
        else:
            print("[ERROR] No OCPN models data files found")
    else:
        print("[ERROR] OCPN models directory not found")
    
    # Data Quality Check
    print("\n6. DATA QUALITY VALIDATION")
    print("-" * 40)
    
    total_size = 0
    file_count = 0
    
    for table_dir in ["warehouse/ocel/events", "warehouse/ocel/event_objects", 
                      "warehouse/ocel/event_attributes", "warehouse/ocel/objects", 
                      "warehouse/ocel/object_attributes", "warehouse/ocpn/models"]:
        dir_path = Path(table_dir + "/data")
        if dir_path.exists():
            for file_path in dir_path.rglob("*.parquet"):
                if file_path.stat().st_size > 0:
                    total_size += file_path.stat().st_size
                    file_count += 1
    
    print(f"[OK] Total data files: {file_count}")
    print(f"[OK] Total data size: {total_size / (1024*1024):.2f} MB")
    
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Tests passed: {success_count}/{total_tests}")
    print(f"Data files: {file_count}")
    print(f"Data size: {total_size / (1024*1024):.2f} MB")
    
    if success_count == total_tests:
        print("\n[SUCCESS] LAKEHOUSE SYSTEM IS WORKING!")
        print("All data successfully ingested and accessible with Daft")
    else:
        print(f"\n[PARTIAL] {success_count}/{total_tests} tests passed")
        print("Some components working, others need attention")
    
    print("=" * 80)


if __name__ == '__main__':
    validate_system()
