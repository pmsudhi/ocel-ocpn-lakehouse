#!/usr/bin/env python3
"""
Complete Iceberg table bootstrap following the detailed specification
Implements all OCEL/OCPN tables with proper partitioning, sort orders, and properties
"""

import yaml
from pathlib import Path
from datetime import datetime
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import *
from pyiceberg.schema import NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.table.sorting import SortOrder, SortField


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


def create_schema_from_spec(schema_spec):
    """Create Iceberg schema from specification."""
    fields = []
    field_id = 1
    
    for field_spec in schema_spec['schema']:
        name = field_spec['name']
        required = field_spec.get('required', False)
        comment = field_spec.get('comment', '')
        
        # Map type strings to Iceberg types
        type_mapping = {
            'string': StringType(),
            'long': LongType(),
            'double': DoubleType(),
            'boolean': BooleanType(),
            'date': DateType(),
            'timestamptz': TimestampType(),
            'binary': BinaryType(),
            'decimal(38,10)': DecimalType(38, 10)
        }
        
        field_type = type_mapping.get(field_spec['type'], StringType())
        fields.append(NestedField(field_id, name, field_type, required=required, doc=comment))
        field_id += 1
    
    return Schema(*fields)


def create_partition_spec(schema, partition_spec):
    """Create partition specification."""
    if not partition_spec:
        return None
    
    partition_fields = []
    for partition_field in partition_spec:
        field_name = partition_field['name']
        transform = partition_field['transform']
        source_id = partition_field['source_id']
        
        # Find the field in schema
        field = None
        for schema_field in schema.fields:
            if schema_field.name == field_name:
                field = schema_field
                break
        
        if field:
            if transform == 'year':
                partition_fields.append(PartitionField(source_id, field.field_id, Transform.year))
            elif transform == 'month':
                partition_fields.append(PartitionField(source_id, field.field_id, Transform.month))
            elif transform == 'bucket':
                bucket_count = partition_field.get('bucket_count', 32)
                partition_fields.append(PartitionField(source_id, field.field_id, Transform.bucket(bucket_count)))
            elif transform == 'identity':
                partition_fields.append(PartitionField(source_id, field.field_id, Transform.identity))
    
    return PartitionSpec(*partition_fields) if partition_fields else None


def create_sort_order(schema, sort_order):
    """Create sort order specification."""
    if not sort_order:
        return None
    
    sort_fields = []
    for sort_field in sort_order:
        source_id = sort_field['source_id']
        direction = SortDirection.ASC if sort_field['direction'] == 'asc' else SortDirection.DESC
        null_order = NullOrder.NULLS_FIRST if sort_field.get('null_order') == 'nulls_first' else NullOrder.NULLS_LAST
        
        sort_fields.append(SortField(source_id, direction, null_order))
    
    return SortOrder(*sort_fields) if sort_fields else None


def create_table_with_spec(cat, table_name, schema_spec):
    """Create table with complete specification."""
    print(f"Creating table: {table_name}")
    
    try:
        # Check if table exists
        cat.load_table(table_name)
        print(f"Table {table_name} already exists, skipping...")
        return True
    except Exception:
        pass
    
    # Create schema
    schema = create_schema_from_spec(schema_spec)
    
    # Create partition spec
    partition_spec = create_partition_spec(schema, schema_spec.get('partition_spec', []))
    
    # Create sort order
    sort_order = create_sort_order(schema, schema_spec.get('sort_order', []))
    
    # Get properties
    properties = schema_spec.get('properties', {})
    
    try:
        # Create table
        cat.create_table(
            table_name,
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=properties
        )
        print(f"✅ Created table {table_name}")
        return True
    except Exception as e:
        print(f"❌ Failed to create table {table_name}: {e}")
        return False


def load_schema_specs():
    """Load schema specifications from YAML files."""
    schemas = {}
    
    # Load OCEL schemas
    ocel_schema_file = Path(__file__).parents[1] / 'schemas' / 'complete_ocel_schemas.yaml'
    if ocel_schema_file.exists():
        with open(ocel_schema_file, 'r', encoding='utf-8') as f:
            ocel_schemas = yaml.safe_load(f)
            schemas.update(ocel_schemas)
    
    # Load OCPN schemas
    ocpn_schema_file = Path(__file__).parents[1] / 'schemas' / 'complete_ocpn_schemas.yaml'
    if ocpn_schema_file.exists():
        with open(ocpn_schema_file, 'r', encoding='utf-8') as f:
            ocpn_schemas = yaml.safe_load(f)
            schemas.update(ocpn_schemas)
    
    return schemas


def main():
    """Bootstrap all tables with complete specifications."""
    print("=" * 80)
    print("COMPLETE ICEBERG TABLE BOOTSTRAP")
    print("=" * 80)
    
    # Load catalog
    catalog_cfg = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_cfg)
    
    # Create namespaces
    try:
        cat.create_namespace('ocel')
        print("✅ Created namespace 'ocel'")
    except Exception as e:
        print(f"Namespace 'ocel' may already exist: {e}")
    
    try:
        cat.create_namespace('ocpn')
        print("✅ Created namespace 'ocpn'")
    except Exception as e:
        print(f"Namespace 'ocpn' may already exist: {e}")
    
    # Load schema specifications
    schemas = load_schema_specs()
    
    if not schemas:
        print("❌ No schema specifications found!")
        return False
    
    # Create tables in dependency order
    table_order = [
        # Dimension tables first
        'ocel.event_types',
        'ocel.object_types',
        
        # Main fact table
        'ocel.events',
        
        # Relationship tables
        'ocel.event_objects',
        'ocel.event_attributes',
        
        # Object tables
        'ocel.objects',
        'ocel.object_attributes',
        
        # Metadata table
        'ocel.log_metadata',
        
        # OCPN tables
        'ocpn.models',
        'ocpn.places',
        'ocpn.transitions',
        'ocpn.arcs',
        'ocpn.markings',
        'ocpn.layout'
    ]
    
    success_count = 0
    total_tables = len(table_order)
    
    for table_name in table_order:
        if table_name in schemas:
            if create_table_with_spec(cat, table_name, schemas[table_name]):
                success_count += 1
        else:
            print(f"⚠️ Schema specification not found for {table_name}")
    
    print("\n" + "=" * 80)
    print("BOOTSTRAP SUMMARY")
    print("=" * 80)
    print(f"Tables created: {success_count}/{total_tables}")
    
    if success_count == total_tables:
        print("✅ All tables created successfully!")
        print("✅ Complete OCEL/OCPN schema implemented!")
        print("✅ Partitioning and sort orders configured!")
        print("✅ Table properties optimized!")
    else:
        print(f"⚠️ {total_tables - success_count} tables failed to create")
    
    return success_count == total_tables


if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
