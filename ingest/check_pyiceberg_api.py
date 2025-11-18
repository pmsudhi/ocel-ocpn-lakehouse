#!/usr/bin/env python3
"""
Check PyIceberg API structure
"""

import pyiceberg
from pyiceberg import transforms
from pyiceberg.transforms import Transform

print("PyIceberg version:", pyiceberg.__version__)
print("Available in transforms module:")
print(dir(transforms))

print("\nTransform class methods:")
print([attr for attr in dir(Transform) if not attr.startswith('_')])

# Try different approaches
try:
    from pyiceberg.transforms import YearTransform
    print("YearTransform available")
except ImportError:
    print("YearTransform not available")

try:
    from pyiceberg.transforms import MonthTransform
    print("MonthTransform available")
except ImportError:
    print("MonthTransform not available")

try:
    from pyiceberg.transforms import BucketTransform
    print("BucketTransform available")
except ImportError:
    print("BucketTransform not available")

try:
    from pyiceberg.transforms import IdentityTransform
    print("IdentityTransform available")
except ImportError:
    print("IdentityTransform not available")

# Check if there are specific transform classes
print("\nAll transforms module contents:")
for item in dir(transforms):
    if 'Transform' in item:
        print(f"  {item}")

# Try to create transforms directly
try:
    year = transforms.YearTransform()
    print(f"YearTransform(): {year}")
except Exception as e:
    print(f"YearTransform() failed: {e}")

try:
    month = transforms.MonthTransform()
    print(f"MonthTransform(): {month}")
except Exception as e:
    print(f"MonthTransform() failed: {e}")

try:
    bucket = transforms.BucketTransform(32)
    print(f"BucketTransform(32): {bucket}")
except Exception as e:
    print(f"BucketTransform() failed: {e}")

try:
    identity = transforms.IdentityTransform()
    print(f"IdentityTransform(): {identity}")
except Exception as e:
    print(f"IdentityTransform() failed: {e}")
