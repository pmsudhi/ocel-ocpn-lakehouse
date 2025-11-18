import sys
import json
from pathlib import Path
from pyiceberg.catalog import load_catalog


def _load_yaml(path: Path):
    # Minimal YAML loader to avoid extra deps; supports subset used here
    import re
    data = {}
    section = None
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n')
            if not line or line.strip().startswith('#'):
                continue
            if not line.startswith(' '):
                key = line.split(':', 1)[0].strip()
                if key:
                    section = key
                    data[section] = {}
            else:
                m = re.match(r"\s*-?\s*\{(.+?)\}\s*$", line)
                if m and isinstance(data.get(section), list):
                    # not used in this simple loader variant
                    continue
    return data


def load_catalog_from_yaml(catalog_name: str, config_path: Path):
    # Expect simple YAML with top-level key/values like type/warehouse
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


def create_table_if_missing(cat, identifier: str, schema_fields: list, partition_spec: list = None, sort_order: list = None, properties: dict = None):
    from pyiceberg.schema import Schema
    from pyiceberg.types import (StringType, TimestampType, DateType, BooleanType, DoubleType, LongType, BinaryType, DecimalType)
    from pyiceberg.schema import NestedField

    def to_type(t: str):
        t = t.lower()
        if t == 'string':
            return StringType()
        if t == 'timestamp':
            return TimestampType()
        if t == 'date':
            return DateType()
        if t == 'boolean':
            return BooleanType()
        if t == 'double':
            return DoubleType()
        if t == 'long' or t == 'int64':
            return LongType()
        if t.startswith('decimal'):
            import re
            m = re.search(r"decimal\((\d+),(\d+)\)", t)
            if m:
                return DecimalType(int(m.group(1)), int(m.group(2)))
            return DecimalType(38, 10)
        if t == 'binary':
            return BinaryType()
        return StringType()

    fields = []
    field_id = 1
    for col in schema_fields:
        name = col['name']
        required = bool(col.get('required', False))
        typ = to_type(col['type'])
        fields.append(NestedField(field_id, name, typ, required=required))
        field_id += 1

    schema = Schema(*fields)

    # Create table without partitions/sort first for stability
    try:
        cat.load_table(identifier)
        print(f"Table {identifier} already exists")
        return
    except Exception:
        pass

    # Create minimal table first - we'll evolve partitions later
    cat.create_table(identifier, schema=schema, properties=properties or {})
    print(f"Created table {identifier}")


def main():
    # Defaults to local catalog config
    catalog_path = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_path)

    # Create namespaces first
    try:
        cat.create_namespace('ocel')
        print("Created namespace 'ocel'")
    except Exception as e:
        print(f"Namespace 'ocel' may already exist: {e}")
    
    try:
        cat.create_namespace('ocpn')
        print("Created namespace 'ocpn'")
    except Exception as e:
        print(f"Namespace 'ocpn' may already exist: {e}")

    # Create minimal tables first - we'll evolve partitions later
    from datetime import datetime
    
    # OCEL tables
    create_table_if_missing(
        cat,
        'ocel.events',
        [
            {'name': 'id', 'type': 'string', 'required': True},
            {'name': 'type', 'type': 'string', 'required': True},
            {'name': 'time', 'type': 'timestamp', 'required': True},
            {'name': 'event_date', 'type': 'date', 'required': True},
            {'name': 'event_month', 'type': 'string', 'required': False},
            {'name': 'vendor_code', 'type': 'string', 'required': False},
            {'name': 'request_id', 'type': 'string', 'required': False},
        ],
        properties={'target-file-size-bytes': '268435456'}
    )

    # relationships
    create_table_if_missing(
        cat,
        'ocel.event_objects',
        [
            {'name': 'event_id', 'type': 'string', 'required': True},
            {'name': 'object_id', 'type': 'string', 'required': True},
            {'name': 'qualifier', 'type': 'string', 'required': False},
        ]
    )

    # event attributes
    create_table_if_missing(
        cat,
        'ocel.event_attributes',
        [
            {'name': 'event_id', 'type': 'string', 'required': True},
            {'name': 'name', 'type': 'string', 'required': True},
            {'name': 'val_string', 'type': 'string', 'required': False},
            {'name': 'val_double', 'type': 'double', 'required': False},
            {'name': 'val_boolean', 'type': 'boolean', 'required': False},
            {'name': 'val_timestamp', 'type': 'timestamp', 'required': False},
            {'name': 'val_long', 'type': 'long', 'required': False},
            {'name': 'val_decimal', 'type': 'decimal(38,10)', 'required': False},
            {'name': 'val_bytes', 'type': 'binary', 'required': False},
            {'name': 'val_type', 'type': 'string', 'required': True},
            {'name': 'val_json', 'type': 'string', 'required': False},
        ]
    )

    # objects
    create_table_if_missing(
        cat,
        'ocel.objects',
        [
            {'name': 'id', 'type': 'string', 'required': True},
            {'name': 'type', 'type': 'string', 'required': True},
            {'name': 'created_at', 'type': 'timestamp', 'required': False},
            {'name': 'lifecycle_state', 'type': 'string', 'required': False},
        ]
    )

    # object attributes
    create_table_if_missing(
        cat,
        'ocel.object_attributes',
        [
            {'name': 'object_id', 'type': 'string', 'required': True},
            {'name': 'name', 'type': 'string', 'required': True},
            {'name': 'val_string', 'type': 'string', 'required': False},
            {'name': 'val_double', 'type': 'double', 'required': False},
            {'name': 'val_boolean', 'type': 'boolean', 'required': False},
            {'name': 'val_timestamp', 'type': 'timestamp', 'required': False},
            {'name': 'val_long', 'type': 'long', 'required': False},
            {'name': 'val_decimal', 'type': 'decimal(38,10)', 'required': False},
            {'name': 'val_bytes', 'type': 'binary', 'required': False},
            {'name': 'val_type', 'type': 'string', 'required': True},
            {'name': 'val_json', 'type': 'string', 'required': False},
        ]
    )

    # OCPN tables
    create_table_if_missing(
        cat,
        'ocpn.models',
        [
            {'name': 'model_id', 'type': 'string', 'required': True},
            {'name': 'version', 'type': 'long', 'required': True},
            {'name': 'name', 'type': 'string', 'required': False},
            {'name': 'created_at', 'type': 'timestamp', 'required': True},
            {'name': 'source_format', 'type': 'string', 'required': False},
            {'name': 'raw_pnml', 'type': 'binary', 'required': False},
            {'name': 'notes', 'type': 'string', 'required': False},
        ]
    )

    create_table_if_missing(
        cat,
        'ocpn.places',
        [
            {'name': 'model_id', 'type': 'string', 'required': True},
            {'name': 'place_id', 'type': 'string', 'required': True},
            {'name': 'label', 'type': 'string', 'required': False},
        ]
    )

    create_table_if_missing(
        cat,
        'ocpn.transitions',
        [
            {'name': 'model_id', 'type': 'string', 'required': True},
            {'name': 'transition_id', 'type': 'string', 'required': True},
            {'name': 'label', 'type': 'string', 'required': False},
            {'name': 'invisible', 'type': 'boolean', 'required': False},
        ]
    )

    create_table_if_missing(
        cat,
        'ocpn.arcs',
        [
            {'name': 'model_id', 'type': 'string', 'required': True},
            {'name': 'arc_id', 'type': 'string', 'required': True},
            {'name': 'src_type', 'type': 'string', 'required': True},
            {'name': 'src_id', 'type': 'string', 'required': True},
            {'name': 'dst_type', 'type': 'string', 'required': True},
            {'name': 'dst_id', 'type': 'string', 'required': True},
            {'name': 'weight', 'type': 'long', 'required': False},
        ]
    )

    print("Bootstrap completed for OCEL tables in catalog.")


if __name__ == '__main__':
    main()

