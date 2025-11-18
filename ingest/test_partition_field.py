#!/usr/bin/env python3
"""
Test PartitionField constructor
"""

from pyiceberg.partitioning import PartitionField
from pyiceberg.transforms import YearTransform, MonthTransform, BucketTransform, IdentityTransform

# Test different PartitionField constructors
try:
    print("Testing PartitionField with name...")
    field = PartitionField(name="year_partition", source_id=4, field_id=4, transform=YearTransform())
    print(f"PartitionField with name: {field}")
except Exception as e:
    print(f"PartitionField with name failed: {e}")

try:
    print("Testing PartitionField without name...")
    field = PartitionField(source_id=4, field_id=4, transform=YearTransform())
    print(f"PartitionField without name: {field}")
except Exception as e:
    print(f"PartitionField without name failed: {e}")

try:
    print("Testing PartitionField with positional args...")
    field = PartitionField(4, 4, YearTransform())
    print(f"PartitionField positional: {field}")
except Exception as e:
    print(f"PartitionField positional failed: {e}")

# Check PartitionField constructor signature
import inspect
print(f"\nPartitionField signature: {inspect.signature(PartitionField.__init__)}")

# Check what fields are required
print(f"PartitionField fields: {PartitionField.model_fields}")
