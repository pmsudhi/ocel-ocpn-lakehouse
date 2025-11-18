#!/usr/bin/env python3
"""
Test SortField constructor
"""

from pyiceberg.table.sorting import SortField, SortDirection, NullOrder

# Test SortField constructor
try:
    print("Testing SortField with source_id...")
    field = SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)
    print(f"SortField with source_id: {field}")
except Exception as e:
    print(f"SortField with source_id failed: {e}")

try:
    print("Testing SortField with field_id...")
    field = SortField(field_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)
    print(f"SortField with field_id: {field}")
except Exception as e:
    print(f"SortField with field_id failed: {e}")

try:
    print("Testing SortField positional...")
    field = SortField(2, SortDirection.ASC, NullOrder.NULLS_FIRST)
    print(f"SortField positional: {field}")
except Exception as e:
    print(f"SortField positional failed: {e}")

# Check SortField constructor signature
import inspect
print(f"\nSortField signature: {inspect.signature(SortField.__init__)}")

# Check what fields are required
print(f"SortField fields: {SortField.model_fields}")
