#!/usr/bin/env python3
"""
Test PyIceberg Transform API to find correct usage
"""

from pyiceberg.transforms import Transform
from pyiceberg.partitioning import PartitionField

# Test different Transform methods
try:
    print("Testing Transform.year...")
    year_transform = Transform.year()
    print(f"Transform.year(): {year_transform}")
except Exception as e:
    print(f"Transform.year() failed: {e}")

try:
    print("Testing Transform.month...")
    month_transform = Transform.month()
    print(f"Transform.month(): {month_transform}")
except Exception as e:
    print(f"Transform.month() failed: {e}")

try:
    print("Testing Transform.bucket...")
    bucket_transform = Transform.bucket(32)
    print(f"Transform.bucket(32): {bucket_transform}")
except Exception as e:
    print(f"Transform.bucket() failed: {e}")

try:
    print("Testing Transform.identity...")
    identity_transform = Transform.identity()
    print(f"Transform.identity(): {identity_transform}")
except Exception as e:
    print(f"Transform.identity() failed: {e}")

# Test PartitionField creation
try:
    print("Testing PartitionField creation...")
    field = PartitionField(1, 4, Transform.year())
    print(f"PartitionField created: {field}")
except Exception as e:
    print(f"PartitionField creation failed: {e}")

print("Transform API test completed")
