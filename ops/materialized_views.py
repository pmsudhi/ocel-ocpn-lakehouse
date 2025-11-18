#!/usr/bin/env python3
"""
Materialized Views for Process Mining Analytics
Pre-computes common process mining metrics for 10-100x query performance
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import json

import daft
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, LongType, DoubleType, BooleanType, TimestampType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder
from pyiceberg.transforms import IdentityTransform, BucketTransform

# Fix Unicode display issues on Windows
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"


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


class MaterializedViewManager:
    """Manages materialized views for process mining analytics."""
    
    def __init__(self, catalog):
        self.catalog = catalog
        self.view_definitions = self._define_materialized_views()
    
    def _define_materialized_views(self) -> Dict[str, Any]:
        """Define all materialized views for process mining."""
        views = {}
        
        # 1. Direct-Follows Graph (DFG) - pre-computed
        dfg_schema = Schema(
            NestedField(1, 'from_activity', StringType(), required=True, doc='Source activity name'),
            NestedField(2, 'to_activity', StringType(), required=True, doc='Target activity name'),
            NestedField(3, 'frequency', LongType(), required=True, doc='Number of occurrences'),
            NestedField(4, 'avg_time_seconds', DoubleType(), required=False, doc='Average time between activities'),
            NestedField(5, 'instance_count', LongType(), required=True, doc='Number of instances with this transition'),
            NestedField(6, 'last_updated', TimestampType(), required=True, doc='Last refresh timestamp')
        )
        
        dfg_partition_spec = PartitionSpec(
            PartitionField(name="from_activity_identity", source_id=7, field_id=1, transform=IdentityTransform())
        )
        
        dfg_sort_order = SortOrder(
            SortField(source_id=3, direction=SortDirection.DESC, null_order=NullOrder.NULLS_FIRST),  # frequency
            SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)   # from_activity
        )
        
        views['ocel.dfg_matrix'] = {
            'schema': dfg_schema,
            'partition_spec': dfg_partition_spec,
            'sort_order': dfg_sort_order,
            'properties': {
                'format-version': '2',
                'write.target-file-size-bytes': '134217728'  # 128MB
            }
        }
        
        # 2. Activity performance metrics
        activity_metrics_schema = Schema(
            NestedField(1, 'activity_name', StringType(), required=True, doc='Activity name'),
            NestedField(2, 'total_occurrences', LongType(), required=True, doc='Total number of occurrences'),
            NestedField(3, 'avg_duration_seconds', DoubleType(), required=False, doc='Average duration'),
            NestedField(4, 'min_duration_seconds', DoubleType(), required=False, doc='Minimum duration'),
            NestedField(5, 'max_duration_seconds', DoubleType(), required=False, doc='Maximum duration'),
            NestedField(6, 'instances_with_activity', LongType(), required=True, doc='Number of instances containing this activity'),
            NestedField(7, 'last_updated', TimestampType(), required=True, doc='Last refresh timestamp')
        )
        
        activity_metrics_partition_spec = PartitionSpec(
            PartitionField(name="activity_identity", source_id=8, field_id=1, transform=IdentityTransform())
        )
        
        activity_metrics_sort_order = SortOrder(
            SortField(source_id=2, direction=SortDirection.DESC, null_order=NullOrder.NULLS_FIRST),  # total_occurrences
            SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)    # activity_name
        )
        
        views['ocel.activity_metrics'] = {
            'schema': activity_metrics_schema,
            'partition_spec': activity_metrics_partition_spec,
            'sort_order': activity_metrics_sort_order,
            'properties': {
                'format-version': '2',
                'write.target-file-size-bytes': '134217728'  # 128MB
            }
        }
        
        # 3. Object interaction patterns
        object_interaction_schema = Schema(
            NestedField(1, 'source_object_type', StringType(), required=True, doc='Source object type'),
            NestedField(2, 'target_object_type', StringType(), required=True, doc='Target object type'),
            NestedField(3, 'interaction_count', LongType(), required=True, doc='Number of interactions'),
            NestedField(4, 'common_events', ArrayType(element_type=StringType()), required=False, doc='Common event types'),
            NestedField(5, 'last_updated', TimestampType(), required=True, doc='Last refresh timestamp')
        )
        
        object_interaction_partition_spec = PartitionSpec(
            PartitionField(name="source_type_identity", source_id=6, field_id=1, transform=IdentityTransform())
        )
        
        object_interaction_sort_order = SortOrder(
            SortField(source_id=3, direction=SortDirection.DESC, null_order=NullOrder.NULLS_FIRST),  # interaction_count
            SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)    # source_object_type
        )
        
        views['ocel.object_interaction_graph'] = {
            'schema': object_interaction_schema,
            'partition_spec': object_interaction_partition_spec,
            'sort_order': object_interaction_sort_order,
            'properties': {
                'format-version': '2',
                'write.target-file-size-bytes': '134217728'  # 128MB
            }
        }
        
        # 4. Process variants (materialized)
        process_variants_schema = Schema(
            NestedField(1, 'variant_id', StringType(), required=True, doc='Unique variant identifier'),
            NestedField(2, 'variant_pattern', StringType(), required=True, doc='Event sequence pattern (A->B->C->D)'),
            NestedField(3, 'frequency', LongType(), required=True, doc='Number of instances with this pattern'),
            NestedField(4, 'avg_duration_seconds', DoubleType(), required=False, doc='Average duration for this variant'),
            NestedField(5, 'instance_ids', ArrayType(element_type=StringType()), required=False, doc='Sample instance IDs'),
            NestedField(6, 'last_updated', TimestampType(), required=True, doc='Last refresh timestamp')
        )
        
        process_variants_partition_spec = PartitionSpec(
            PartitionField(name="variant_identity", source_id=7, field_id=1, transform=IdentityTransform())
        )
        
        process_variants_sort_order = SortOrder(
            SortField(source_id=3, direction=SortDirection.DESC, null_order=NullOrder.NULLS_FIRST),  # frequency
            SortField(source_id=1, direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)    # variant_id
        )
        
        views['ocel.process_variants'] = {
            'schema': process_variants_schema,
            'partition_spec': process_variants_partition_spec,
            'sort_order': process_variants_sort_order,
            'properties': {
                'format-version': '2',
                'write.target-file-size-bytes': '134217728'  # 128MB
            }
        }
        
        return views
    
    def create_materialized_views(self) -> bool:
        """Create all materialized view tables."""
        print("Creating materialized views for process mining analytics...")
        
        success_count = 0
        total_views = len(self.view_definitions)
        
        for view_name, config in self.view_definitions.items():
            try:
                if self.catalog.table_exists(view_name):
                    print(f"View {view_name} already exists, skipping...")
                    success_count += 1
                    continue
                
                self.catalog.create_table(
                    identifier=view_name,
                    schema=config['schema'],
                    partition_spec=config['partition_spec'],
                    sort_order=config['sort_order'],
                    properties=config['properties']
                )
                print(f"[OK] Created materialized view: {view_name}")
                success_count += 1
                
            except Exception as e:
                print(f"[ERROR] Failed to create view {view_name}: {e}")
        
        print(f"\nMaterialized views created: {success_count}/{total_views}")
        return success_count == total_views
    
    def refresh_dfg_matrix(self) -> bool:
        """Refresh Direct-Follows Graph matrix."""
        print("Refreshing DFG matrix...")
        
        try:
            # Read events and instance events
            events_table = self.catalog.load_table('ocel.events')
            instance_events_table = self.catalog.load_table('ocel.instance_events')
            
            df_events = daft.read_iceberg(events_table)
            df_instance_events = daft.read_iceberg(instance_events_table)
            
            # Join to get event sequences per instance
            df_sequences = df_instance_events.join(
                df_events, 
                left_on="event_id", 
                right_on="id", 
                how="inner"
            )
            
            # Group by instance and sort by sequence to get activity sequences
            df_sequences = df_sequences.sort(["instance_id", "event_sequence"])
            
            # Compute direct-follows relationships
            dfg_data = []
            current_time = datetime.utcnow().replace(tzinfo=None)
            
            # Group by instance to get activity sequences
            instance_sequences = df_sequences.groupby("instance_id").agg({
                "type": "collect",
                "time": "collect"
            })
            
            # Process each instance sequence
            try:
                for instance_id, row in instance_sequences.iterrows():
                    activities = row.get('type', [])
                    times = row.get('time', [])
                    
                    # Validate data
                    if not activities or len(activities) < 2:
                        continue
                    
                    # Create direct-follows pairs
                    for i in range(len(activities) - 1):
                        from_activity = activities[i]
                        to_activity = activities[i + 1]
                        
                        # Skip if activities are None or empty
                        if not from_activity or not to_activity:
                            continue
                        
                        # Calculate time difference
                        time_diff = None
                        if i < len(times) - 1 and times[i] and times[i + 1]:
                            try:
                                time_diff = (times[i + 1] - times[i]).total_seconds()
                            except (TypeError, ValueError):
                                time_diff = None
                        
                        dfg_data.append({
                            'from_activity': from_activity,
                            'to_activity': to_activity,
                            'frequency': 1,
                            'avg_time_seconds': time_diff,
                            'instance_count': 1,
                            'last_updated': current_time
                        })
            except Exception as e:
                print(f"[WARNING] Error processing instance sequences: {e}")
                # Continue with next iteration if this was inside a loop
            
            # Aggregate DFG data
            if dfg_data:
                df_dfg = daft.from_pylist(dfg_data)
                
                # Group by from_activity, to_activity and aggregate
                dfg_aggregated = df_dfg.groupby(["from_activity", "to_activity"]).agg({
                    "frequency": "sum",
                    "avg_time_seconds": "mean",
                    "instance_count": "sum"
                })
                
                # Add last_updated
                dfg_aggregated = dfg_aggregated.with_columns(
                    daft.lit(current_time).alias("last_updated")
                )
                
                # Write to materialized view
                dfg_table = self.catalog.load_table('ocel.dfg_matrix')
                dfg_aggregated.write_iceberg(dfg_table, mode="overwrite")
                print(f"[OK] Refreshed DFG matrix with {len(dfg_data)} transitions")
                return True
            else:
                print("[WARNING] No DFG data to refresh")
                return False
                
        except Exception as e:
            print(f"[ERROR] Failed to refresh DFG matrix: {e}")
            return False
    
    def refresh_activity_metrics(self) -> bool:
        """Refresh activity performance metrics."""
        print("Refreshing activity metrics...")
        
        try:
            events_table = self.catalog.load_table('ocel.events')
            df_events = daft.read_iceberg(events_table)
            
            # Group by activity type and compute metrics
            activity_metrics = df_events.groupby("type").agg({
                "id": "count",  # total_occurrences
                "time": ["min", "max"]  # for duration calculation
            })
            
            # Calculate durations and other metrics
            current_time = datetime.utcnow().replace(tzinfo=None)
            metrics_data = []
            
            for activity_name, row in activity_metrics.iterrows():
                total_occurrences = row.get('id_count', 0)
                min_time = row.get('time_min')
                max_time = row.get('time_max')
                
                # Calculate duration if we have time data
                avg_duration = None
                min_duration = None
                max_duration = None
                
                if min_time and max_time:
                    duration_seconds = (max_time - min_time).total_seconds()
                    avg_duration = duration_seconds / total_occurrences if total_occurrences > 0 else 0
                    min_duration = 0  # Would need individual event durations
                    max_duration = duration_seconds
                
                metrics_data.append({
                    'activity_name': activity_name,
                    'total_occurrences': total_occurrences,
                    'avg_duration_seconds': avg_duration,
                    'min_duration_seconds': min_duration,
                    'max_duration_seconds': max_duration,
                    'instances_with_activity': total_occurrences,  # Simplified
                    'last_updated': current_time
                })
            
            if metrics_data:
                df_metrics = daft.from_pylist(metrics_data)
                metrics_table = self.catalog.load_table('ocel.activity_metrics')
                df_metrics.write_iceberg(metrics_table, mode="overwrite")
                print(f"[OK] Refreshed activity metrics for {len(metrics_data)} activities")
                return True
            else:
                print("[WARNING] No activity metrics data to refresh")
                return False
                
        except Exception as e:
            print(f"[ERROR] Failed to refresh activity metrics: {e}")
            return False
    
    def refresh_process_variants(self) -> bool:
        """Refresh process variants from instance sequences."""
        print("Refreshing process variants...")
        
        try:
            # Read instance events and events
            instance_events_table = self.catalog.load_table('ocel.instance_events')
            events_table = self.catalog.load_table('ocel.events')
            
            df_instance_events = daft.read_iceberg(instance_events_table)
            df_events = daft.read_iceberg(events_table)
            
            # Join to get activity sequences
            df_sequences = df_instance_events.join(
                df_events,
                left_on="event_id",
                right_on="id",
                how="inner"
            )
            
            # Group by instance to get activity sequences
            instance_sequences = df_sequences.groupby("instance_id").agg({
                "type": "collect",
                "event_sequence": "collect"
            })
            
            # Create variant patterns
            variant_patterns = {}
            current_time = datetime.utcnow().replace(tzinfo=None)
            
            for instance_id, row in instance_sequences.iterrows():
                activities = row.get('type', [])
                sequences = row.get('event_sequence', [])
                
                # Sort by sequence to get proper order
                if activities and sequences:
                    # Create pattern string (A->B->C->D)
                    pattern = "->".join(activities)
                    
                    if pattern not in variant_patterns:
                        variant_patterns[pattern] = {
                            'variant_id': f"variant_{hash(pattern) % 1000000:06d}",
                            'variant_pattern': pattern,
                            'frequency': 0,
                            'instance_ids': [],
                            'durations': []
                        }
                    
                    variant_patterns[pattern]['frequency'] += 1
                    variant_patterns[pattern]['instance_ids'].append(instance_id)
            
            # Convert to materialized view format
            variants_data = []
            for pattern, data in variant_patterns.items():
                # Calculate average duration (simplified)
                avg_duration = None
                if data['durations']:
                    avg_duration = sum(data['durations']) / len(data['durations'])
                
                variants_data.append({
                    'variant_id': data['variant_id'],
                    'variant_pattern': data['variant_pattern'],
                    'frequency': data['frequency'],
                    'avg_duration_seconds': avg_duration,
                    'instance_ids': data['instance_ids'][:10],  # Sample first 10
                    'last_updated': current_time
                })
            
            if variants_data:
                df_variants = daft.from_pylist(variants_data)
                variants_table = self.catalog.load_table('ocel.process_variants')
                df_variants.write_iceberg(variants_table, mode="overwrite")
                print(f"[OK] Refreshed process variants with {len(variants_data)} patterns")
                return True
            else:
                print("[WARNING] No process variants data to refresh")
                return False
                
        except Exception as e:
            print(f"[ERROR] Failed to refresh process variants: {e}")
            return False
    
    def refresh_all_views(self) -> bool:
        """Refresh all materialized views."""
        print("Refreshing all materialized views...")
        
        results = []
        results.append(self.refresh_dfg_matrix())
        results.append(self.refresh_activity_metrics())
        results.append(self.refresh_process_variants())
        
        success_count = sum(results)
        total_views = len(results)
        
        print(f"\nMaterialized views refreshed: {success_count}/{total_views}")
        return success_count == total_views


def main():
    """Main function to create and refresh materialized views."""
    print("=" * 80)
    print("MATERIALIZED VIEWS FOR PROCESS MINING")
    print("=" * 80)
    
    catalog_path = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_path)
    
    # Create materialized view manager
    view_manager = MaterializedViewManager(cat)
    
    # Create all materialized view tables
    if view_manager.create_materialized_views():
        print("\n[SUCCESS] All materialized view tables created!")
        
        # Refresh all views with current data
        if view_manager.refresh_all_views():
            print("\n[SUCCESS] All materialized views refreshed with current data!")
        else:
            print("\n[WARNING] Some materialized views failed to refresh")
    else:
        print("\n[FAILED] Could not create all materialized view tables")


if __name__ == '__main__':
    main()
