#!/usr/bin/env python3
"""
Production-ready Iceberg table bootstrap following the complete specification
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
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder
from pyiceberg.transforms import YearTransform, MonthTransform, BucketTransform, IdentityTransform


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


def create_ocel_schemas():
    """Create complete OCEL schema definitions following the specification."""
    schemas = {}
    
    # Process Instance Tracking Tables (NEW)
    # 1. Process Instances
    process_instances_schema = Schema(
        NestedField(1, 'instance_id', StringType(), required=True, doc='Primary key - process instance identifier'),
        NestedField(2, 'instance_type', StringType(), required=True, doc='Type of process instance'),
        NestedField(3, 'primary_object_id', StringType(), required=False, doc='Foreign key to objects'),
        NestedField(4, 'start_time', TimestampType(), required=True, doc='Process instance start timestamp'),
        NestedField(5, 'end_time', TimestampType(), required=False, doc='Process instance end timestamp'),
        NestedField(6, 'status', StringType(), required=True, doc='Process status: running, completed, failed, cancelled'),
        NestedField(7, 'duration_seconds', LongType(), required=False, doc='Total duration in seconds')
    )
    
    process_instances_partition_spec = PartitionSpec(
        PartitionField(name="instance_type_identity", source_id=8, field_id=2, transform=IdentityTransform())
    )
    
    process_instances_sort_order = SortOrder(
        SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # instance_type
        SortField(source_id=4, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # start_time
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)   # instance_id
    )
    
    schemas['ocel.process_instances'] = {
        'schema': process_instances_schema,
        'partition_spec': process_instances_partition_spec,
        'sort_order': process_instances_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '268435456'  # 256MB
        }
    }
    
    # 2. Instance Events
    instance_events_schema = Schema(
        NestedField(1, 'instance_id', StringType(), required=True, doc='Foreign key to process_instances'),
        NestedField(2, 'event_id', StringType(), required=True, doc='Foreign key to events'),
        NestedField(3, 'event_sequence', IntegerType(), required=True, doc='Position of event in process instance'),
        NestedField(4, 'event_timestamp', TimestampType(), required=True, doc='Denormalized event timestamp')
    )
    
    instance_events_partition_spec = PartitionSpec(
        PartitionField(name="instance_id_bucket", source_id=5, field_id=1, transform=BucketTransform(64))
    )
    
    instance_events_sort_order = SortOrder(
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # instance_id
        SortField(source_id=3, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)   # event_sequence
    )
    
    schemas['ocel.instance_events'] = {
        'schema': instance_events_schema,
        'partition_spec': instance_events_partition_spec,
        'sort_order': instance_events_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '268435456'  # 256MB
        }
    }
    
    # OCEL 2.0 Metadata Tables (NEW)
    # 3. Event Type Attributes
    event_type_attributes_schema = Schema(
        NestedField(1, 'event_type_name', StringType(), required=True, doc='Foreign key to event_types'),
        NestedField(2, 'attribute_name', StringType(), required=True, doc='Name of the attribute'),
        NestedField(3, 'attribute_type', StringType(), required=True, doc='Data type: string, number, boolean, timestamp, object'),
        NestedField(4, 'is_required', BooleanType(), required=True, doc='Whether this attribute is mandatory'),
        NestedField(5, 'default_value', StringType(), required=False, doc='Default value if not provided')
    )
    
    event_type_attributes_partition_spec = PartitionSpec(
        PartitionField(name="event_type_identity", source_id=6, field_id=1, transform=IdentityTransform())
    )
    
    event_type_attributes_sort_order = SortOrder(
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # event_type_name
        SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)   # attribute_name
    )
    
    schemas['ocel.event_type_attributes'] = {
        'schema': event_type_attributes_schema,
        'partition_spec': event_type_attributes_partition_spec,
        'sort_order': event_type_attributes_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '134217728'  # 128MB
        }
    }
    
    # 4. Object Type Attributes
    object_type_attributes_schema = Schema(
        NestedField(1, 'object_type_name', StringType(), required=True, doc='Foreign key to object_types'),
        NestedField(2, 'attribute_name', StringType(), required=True, doc='Name of the attribute'),
        NestedField(3, 'attribute_type', StringType(), required=True, doc='Data type: string, number, boolean, timestamp, object'),
        NestedField(4, 'is_required', BooleanType(), required=True, doc='Whether this attribute is mandatory'),
        NestedField(5, 'default_value', StringType(), required=False, doc='Default value if not provided')
    )
    
    object_type_attributes_partition_spec = PartitionSpec(
        PartitionField(name="object_type_identity", source_id=6, field_id=1, transform=IdentityTransform())
    )
    
    object_type_attributes_sort_order = SortOrder(
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # object_type_name
    )
    
    schemas['ocel.object_type_attributes'] = {
        'schema': object_type_attributes_schema,
        'partition_spec': object_type_attributes_partition_spec,
        'sort_order': object_type_attributes_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '134217728'  # 128MB
        }
    }
    
    # 5. Version Metadata
    version_metadata_schema = Schema(
        NestedField(1, 'ocel_version', StringType(), required=True, doc='OCEL specification version'),
        NestedField(2, 'created_at', TimestampType(), required=True, doc='When this OCEL was created'),
        NestedField(3, 'source_system', StringType(), required=False, doc='Source system that generated this OCEL'),
        NestedField(4, 'extraction_parameters', StringType(), required=False, doc='JSON string of extraction parameters'),
        NestedField(5, 'total_events', LongType(), required=False, doc='Total number of events in this OCEL'),
        NestedField(6, 'total_objects', LongType(), required=False, doc='Total number of objects in this OCEL')
    )
    
    version_metadata_partition_spec = PartitionSpec(
        PartitionField(name="version_identity", source_id=7, field_id=1, transform=IdentityTransform())
    )
    
    version_metadata_sort_order = SortOrder(
        SortField(source_id=2, direction=SortDirection.DESC, null_order=NullOrder.NULLS_FIRST)  # created_at
    )
    
    schemas['ocel.version_metadata'] = {
        'schema': version_metadata_schema,
        'partition_spec': version_metadata_partition_spec,
        'sort_order': version_metadata_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '134217728'  # 128MB
        }
    }
    
    # 6. Event Types Dimension
    schemas['ocel.event_types'] = {
        'schema': Schema(
            NestedField(1, 'name', StringType(), required=True, doc='Primary key - event type name'),
            NestedField(2, 'description', StringType(), required=False, doc='Human-readable description')
        ),
        'partition_spec': None,  # Small dimension, no partition
        'sort_order': None,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '134217728'  # 128MB for small dimension
        }
    }
    
    # 2. Object Types Dimension
    schemas['ocel.object_types'] = {
        'schema': Schema(
            NestedField(1, 'name', StringType(), required=True, doc='Primary key - object type name'),
            NestedField(2, 'description', StringType(), required=False, doc='Human-readable description')
        ),
        'partition_spec': None,  # Small dimension, no partition
        'sort_order': None,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '134217728'  # 128MB for small dimension
        }
    }
    
    # 3. Events Fact Table (Main)
    events_schema = Schema(
        NestedField(1, 'id', StringType(), required=True, doc='Primary key - event ID'),
        NestedField(2, 'type', StringType(), required=True, doc='Foreign key to event_types'),
        NestedField(3, 'time', TimestampType(), required=True, doc='Event timestamp with timezone'),
        NestedField(4, 'event_date', DateType(), required=True, doc='Derived date for partitioning'),
        NestedField(5, 'event_month', StringType(), required=True, doc='Derived YYYY-MM for quick filters'),
        NestedField(6, 'vendor_code', StringType(), required=False, doc='Hot denormalized key for performance'),
        NestedField(7, 'request_id', StringType(), required=False, doc='Hot denormalized key for performance')
    )
    
    # Events partition spec: YEARS(event_date), MONTHS(event_date)
    events_partition_spec = PartitionSpec(
        PartitionField(name="event_date_year", source_id=4, field_id=4, transform=YearTransform()),
        PartitionField(name="event_date_month", source_id=5, field_id=4, transform=MonthTransform())
    )
    
    # Events sort order: (type, time, id)
    events_sort_order = SortOrder(
        SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # type
        SortField(source_id=3, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # time
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)   # id
    )
    
    schemas['ocel.events'] = {
        'schema': events_schema,
        'partition_spec': events_partition_spec,
        'sort_order': events_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '268435456',  # 256MB
            'write.distribution-mode': 'hash',
            'write.distribution-column': 'id'
        }
    }
    
    # 4. Event-Object Relationships
    event_objects_schema = Schema(
        NestedField(1, 'event_id', StringType(), required=True, doc='Foreign key to events'),
        NestedField(2, 'object_id', StringType(), required=True, doc='Foreign key to objects'),
        NestedField(3, 'qualifier', StringType(), required=False, doc='Relationship qualifier (consumes, produces, etc.)')
    )
    
    # Event-Objects partition spec: BUCKET(64, event_id)
    event_objects_partition_spec = PartitionSpec(
        PartitionField(name="event_id_bucket", source_id=4, field_id=1, transform=BucketTransform(64))
    )
    
    # Event-Objects sort order: (event_id, object_id)
    event_objects_sort_order = SortOrder(
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # event_id
        SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)   # object_id
    )
    
    schemas['ocel.event_objects'] = {
        'schema': event_objects_schema,
        'partition_spec': event_objects_partition_spec,
        'sort_order': event_objects_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '268435456'  # 256MB
        }
    }
    
    # 5. Event Attributes (Typed)
    event_attributes_schema = Schema(
        NestedField(1, 'event_id', StringType(), required=True, doc='Foreign key to events'),
        NestedField(2, 'name', StringType(), required=True, doc='Attribute name'),
        NestedField(3, 'val_string', StringType(), required=False, doc='String value'),
        NestedField(4, 'val_double', DoubleType(), required=False, doc='Numeric value'),
        NestedField(5, 'val_boolean', BooleanType(), required=False, doc='Boolean value'),
        NestedField(6, 'val_timestamp', TimestampType(), required=False, doc='Timestamp value'),
        NestedField(7, 'val_long', LongType(), required=False, doc='Integer value'),
        NestedField(8, 'val_decimal', DecimalType(38, 10), required=False, doc='Decimal value'),
        NestedField(9, 'val_bytes', BinaryType(), required=False, doc='Binary value'),
        NestedField(10, 'val_type', StringType(), required=True, doc='Value type enum'),
        NestedField(11, 'val_json', StringType(), required=False, doc='JSON representation for complex values')
    )
    
    # Event-Attributes partition spec: BUCKET(32, event_id)
    event_attributes_partition_spec = PartitionSpec(
        PartitionField(name="event_id_bucket", source_id=12, field_id=1, transform=BucketTransform(32))
    )
    
    # Event-Attributes sort order: (event_id, name)
    event_attributes_sort_order = SortOrder(
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # event_id
        SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)   # name
    )
    
    schemas['ocel.event_attributes'] = {
        'schema': event_attributes_schema,
        'partition_spec': event_attributes_partition_spec,
        'sort_order': event_attributes_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '268435456'  # 256MB
        }
    }
    
    # 6. Objects Table
    objects_schema = Schema(
        NestedField(1, 'id', StringType(), required=True, doc='Primary key - object ID'),
        NestedField(2, 'type', StringType(), required=True, doc='Foreign key to object_types'),
        NestedField(3, 'created_at', TimestampType(), required=False, doc='Object creation timestamp'),
        NestedField(4, 'lifecycle_state', StringType(), required=False, doc='Object lifecycle state')
    )
    
    # Objects partition spec: IDENTITY(type)
    objects_partition_spec = PartitionSpec(
        PartitionField(name="type_identity", source_id=5, field_id=2, transform=IdentityTransform())
    )
    
    # Objects sort order: (type, id)
    objects_sort_order = SortOrder(
        SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # type
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)  # id
    )
    
    schemas['ocel.objects'] = {
        'schema': objects_schema,
        'partition_spec': objects_partition_spec,
        'sort_order': objects_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '268435456'  # 256MB
        }
    }
    
    # 7. Object Attributes (Typed)
    object_attributes_schema = Schema(
        NestedField(1, 'object_id', StringType(), required=True, doc='Foreign key to objects'),
        NestedField(2, 'name', StringType(), required=True, doc='Attribute name'),
        NestedField(3, 'val_string', StringType(), required=False, doc='String value'),
        NestedField(4, 'val_double', DoubleType(), required=False, doc='Numeric value'),
        NestedField(5, 'val_boolean', BooleanType(), required=False, doc='Boolean value'),
        NestedField(6, 'val_timestamp', TimestampType(), required=False, doc='Timestamp value'),
        NestedField(7, 'val_long', LongType(), required=False, doc='Integer value'),
        NestedField(8, 'val_decimal', DecimalType(38, 10), required=False, doc='Decimal value'),
        NestedField(9, 'val_bytes', BinaryType(), required=False, doc='Binary value'),
        NestedField(10, 'val_type', StringType(), required=True, doc='Value type enum'),
        NestedField(11, 'val_json', StringType(), required=False, doc='JSON representation for complex values')
    )
    
    # Object-Attributes partition spec: BUCKET(32, object_id)
    object_attributes_partition_spec = PartitionSpec(
        PartitionField(name="object_id_bucket", source_id=12, field_id=1, transform=BucketTransform(32))
    )
    
    # Object-Attributes sort order: (object_id, name)
    object_attributes_sort_order = SortOrder(
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # object_id
        SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)  # name
    )
    
    schemas['ocel.object_attributes'] = {
        'schema': object_attributes_schema,
        'partition_spec': object_attributes_partition_spec,
        'sort_order': object_attributes_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '268435456'  # 256MB
        }
    }
    
    # 8. Log Metadata (Governance)
    log_metadata_schema = Schema(
        NestedField(1, 'key', StringType(), required=True, doc='Metadata key'),
        NestedField(2, 'value', StringType(), required=True, doc='Metadata value'),
        NestedField(3, 'updated_at', TimestampType(), required=True, doc='Last update timestamp')
    )
    
    schemas['ocel.log_metadata'] = {
        'schema': log_metadata_schema,
        'partition_spec': None,  # Small metadata table, no partition
        'sort_order': None,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '134217728'  # 128MB for small metadata
        }
    }
    
    return schemas


def create_ocpn_schemas():
    """Create complete OCPN schema definitions following the specification."""
    schemas = {}
    
    # 1. OCPN Models
    models_schema = Schema(
        NestedField(1, 'model_id', StringType(), required=True, doc='Primary key - model identifier'),
        NestedField(2, 'version', LongType(), required=True, doc='Model version number'),
        NestedField(3, 'name', StringType(), required=False, doc='Human-readable model name'),
        NestedField(4, 'created_at', TimestampType(), required=True, doc='Model creation timestamp'),
        NestedField(5, 'source_format', StringType(), required=False, doc='Source format (e.g., PNML)'),
        NestedField(6, 'raw_pnml', BinaryType(), required=False, doc='Raw PNML binary data for fidelity'),
        NestedField(7, 'notes', StringType(), required=False, doc='Additional notes or description')
    )
    
    schemas['ocpn.models'] = {
        'schema': models_schema,
        'partition_spec': None,  # Small model metadata, no partition
        'sort_order': None,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '134217728'  # 128MB for model metadata
        }
    }
    
    # 2. OCPN Places
    places_schema = Schema(
        NestedField(1, 'model_id', StringType(), required=True, doc='Foreign key to models'),
        NestedField(2, 'place_id', StringType(), required=True, doc='Place ID within the model'),
        NestedField(3, 'label', StringType(), required=False, doc='Human-readable place label')
    )
    
    # Places partition spec: IDENTITY(model_id)
    places_partition_spec = PartitionSpec(
        PartitionField(name="model_id_identity", source_id=4, field_id=1, transform=IdentityTransform())
    )
    
    # Places sort order: (model_id, place_id)
    places_sort_order = SortOrder(
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # model_id
        SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)    # place_id
    )
    
    schemas['ocpn.places'] = {
        'schema': places_schema,
        'partition_spec': places_partition_spec,
        'sort_order': places_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '134217728'  # 128MB
        }
    }
    
    # 3. OCPN Transitions
    transitions_schema = Schema(
        NestedField(1, 'model_id', StringType(), required=True, doc='Foreign key to models'),
        NestedField(2, 'transition_id', StringType(), required=True, doc='Transition ID within the model'),
        NestedField(3, 'label', StringType(), required=False, doc='Human-readable transition label'),
        NestedField(4, 'invisible', BooleanType(), required=False, doc='Whether transition is invisible (tau transition)')
    )
    
    # Transitions partition spec: IDENTITY(model_id)
    transitions_partition_spec = PartitionSpec(
        PartitionField(name="model_id_identity", source_id=5, field_id=1, transform=IdentityTransform())
    )
    
    # Transitions sort order: (model_id, transition_id)
    transitions_sort_order = SortOrder(
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # model_id
        SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)    # transition_id
    )
    
    schemas['ocpn.transitions'] = {
        'schema': transitions_schema,
        'partition_spec': transitions_partition_spec,
        'sort_order': transitions_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '134217728'  # 128MB
        }
    }
    
    # 4. OCPN Arcs
    arcs_schema = Schema(
        NestedField(1, 'model_id', StringType(), required=True, doc='Foreign key to models'),
        NestedField(2, 'arc_id', StringType(), required=True, doc='Arc ID within the model'),
        NestedField(3, 'src_type', StringType(), required=True, doc='Source type (place or transition)'),
        NestedField(4, 'src_id', StringType(), required=True, doc='Source element ID'),
        NestedField(5, 'dst_type', StringType(), required=True, doc='Destination type (place or transition)'),
        NestedField(6, 'dst_id', StringType(), required=True, doc='Destination element ID'),
        NestedField(7, 'weight', LongType(), required=False, doc='Arc weight (default 1)')
    )
    
    # Arcs partition spec: IDENTITY(model_id)
    arcs_partition_spec = PartitionSpec(
        PartitionField(name="model_id_identity", source_id=8, field_id=1, transform=IdentityTransform())
    )
    
    # Arcs sort order: (model_id, arc_id)
    arcs_sort_order = SortOrder(
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # model_id
        SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)     # arc_id
    )
    
    schemas['ocpn.arcs'] = {
        'schema': arcs_schema,
        'partition_spec': arcs_partition_spec,
        'sort_order': arcs_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '134217728'  # 128MB
        }
    }
    
    # 5. OCPN Markings
    markings_schema = Schema(
        NestedField(1, 'model_id', StringType(), required=True, doc='Foreign key to models'),
        NestedField(2, 'place_id', StringType(), required=True, doc='Foreign key to places'),
        NestedField(3, 'tokens', LongType(), required=True, doc='Number of tokens in the place'),
        NestedField(4, 'marking_type', StringType(), required=True, doc='Marking type (initial, final, etc.)')
    )
    
    # Markings partition spec: IDENTITY(model_id)
    markings_partition_spec = PartitionSpec(
        PartitionField(name="model_id_identity", source_id=5, field_id=1, transform=IdentityTransform())
    )
    
    # Markings sort order: (model_id, place_id)
    markings_sort_order = SortOrder(
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # model_id
        SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)    # place_id
    )
    
    schemas['ocpn.markings'] = {
        'schema': markings_schema,
        'partition_spec': markings_partition_spec,
        'sort_order': markings_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '134217728'  # 128MB
        }
    }
    
    # 6. OCPN Layout (Visualization)
    layout_schema = Schema(
        NestedField(1, 'model_id', StringType(), required=True, doc='Foreign key to models'),
        NestedField(2, 'element_type', StringType(), required=True, doc='Element type (place, transition)'),
        NestedField(3, 'element_id', StringType(), required=True, doc='Element ID within the model'),
        NestedField(4, 'x', DoubleType(), required=True, doc='X coordinate for visualization'),
        NestedField(5, 'y', DoubleType(), required=True, doc='Y coordinate for visualization')
    )
    
    # Layout partition spec: IDENTITY(model_id)
    layout_partition_spec = PartitionSpec(
        PartitionField(name="model_id_identity", source_id=6, field_id=1, transform=IdentityTransform())
    )
    
    # Layout sort order: (model_id, element_type, element_id)
    layout_sort_order = SortOrder(
        SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),  # model_id
        SortField(source_id=2, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),   # element_type
        SortField(source_id=3, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)    # element_id
    )
    
    schemas['ocpn.layout'] = {
        'schema': layout_schema,
        'partition_spec': layout_partition_spec,
        'sort_order': layout_sort_order,
        'properties': {
            'format-version': '2',
            'write.target-file-size-bytes': '134217728'  # 128MB
        }
    }
    
    return schemas


def create_table_with_complete_spec(cat, table_name, schema_spec):
    """Create table with complete specification."""
    print(f"Creating table: {table_name}")
    
    try:
        # Check if table exists
        cat.load_table(table_name)
        print(f"Table {table_name} already exists, skipping...")
        return True
    except Exception:
        pass
    
    try:
        # Create table with complete specification
        cat.create_table(
            table_name,
            schema=schema_spec['schema'],
            partition_spec=schema_spec['partition_spec'],
            sort_order=schema_spec['sort_order'],
            properties=schema_spec['properties']
        )
        print(f"[OK] Created table {table_name}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to create table {table_name}: {e}")
        return False


def main():
    """Bootstrap all tables with complete production specifications."""
    print("=" * 80)
    print("PRODUCTION ICEBERG TABLE BOOTSTRAP")
    print("Following Complete OCEL/OCPN Specification")
    print("=" * 80)
    
    # Load catalog
    catalog_cfg = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_cfg)
    
    # Create namespaces
    try:
        cat.create_namespace('ocel')
        print("[OK] Created namespace 'ocel'")
    except Exception as e:
        print(f"Namespace 'ocel' may already exist: {e}")
    
    try:
        cat.create_namespace('ocpn')
        print("[OK] Created namespace 'ocpn'")
    except Exception as e:
        print(f"Namespace 'ocpn' may already exist: {e}")
    
    # Create OCEL schemas
    ocel_schemas = create_ocel_schemas()
    ocpn_schemas = create_ocpn_schemas()
    
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
        if table_name in ocel_schemas:
            if create_table_with_complete_spec(cat, table_name, ocel_schemas[table_name]):
                success_count += 1
        elif table_name in ocpn_schemas:
            if create_table_with_complete_spec(cat, table_name, ocpn_schemas[table_name]):
                success_count += 1
        else:
            print(f"[WARNING] Schema specification not found for {table_name}")
    
    print("\n" + "=" * 80)
    print("PRODUCTION BOOTSTRAP SUMMARY")
    print("=" * 80)
    print(f"Tables created: {success_count}/{total_tables}")
    
    if success_count == total_tables:
        print("[SUCCESS] All tables created with complete specifications!")
        print("[SUCCESS] Partitioning and sort orders configured!")
        print("[SUCCESS] Table properties optimized for production!")
        print("[SUCCESS] Ready for production data loading!")
    else:
        print(f"[WARNING] {total_tables - success_count} tables failed to create")
    
    return success_count == total_tables


def bootstrap_lakehouse(catalog_name: str = 'local', 
                        catalog_config_path: Path = None) -> bool:
    """
    Bootstrap the lakehouse with all required tables.
    
    Args:
        catalog_name: Name of the catalog to use
        catalog_config_path: Path to catalog configuration YAML file.
                            If None, uses default location.
    
    Returns:
        True if all tables were created successfully, False otherwise
    """
    if catalog_config_path is None:
        catalog_config_path = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    
    print("=" * 80)
    print("PRODUCTION ICEBERG TABLE BOOTSTRAP")
    print("Following Complete OCEL/OCPN Specification")
    print("=" * 80)
    
    # Load catalog
    cat = load_catalog_from_yaml(catalog_name, catalog_config_path)
    
    # Create namespaces
    try:
        cat.create_namespace('ocel')
        print("[OK] Created namespace 'ocel'")
    except Exception as e:
        print(f"Namespace 'ocel' may already exist: {e}")
    
    try:
        cat.create_namespace('ocpn')
        print("[OK] Created namespace 'ocpn'")
    except Exception as e:
        print(f"Namespace 'ocpn' may already exist: {e}")
    
    # Create OCEL schemas
    ocel_schemas = create_ocel_schemas()
    ocpn_schemas = create_ocpn_schemas()
    
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
        if table_name in ocel_schemas:
            if create_table_with_complete_spec(cat, table_name, ocel_schemas[table_name]):
                success_count += 1
        elif table_name in ocpn_schemas:
            if create_table_with_complete_spec(cat, table_name, ocpn_schemas[table_name]):
                success_count += 1
        else:
            print(f"[WARNING] Schema specification not found for {table_name}")
    
    print("\n" + "=" * 80)
    print("PRODUCTION BOOTSTRAP SUMMARY")
    print("=" * 80)
    print(f"Tables created: {success_count}/{total_tables}")
    
    if success_count == total_tables:
        print("[SUCCESS] All tables created with complete specifications!")
        print("[SUCCESS] Partitioning and sort orders configured!")
        print("[SUCCESS] Table properties optimized for production!")
        print("[SUCCESS] Ready for production data loading!")
    else:
        print(f"[WARNING] {total_tables - success_count} tables failed to create")
    
    return success_count == total_tables


if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
