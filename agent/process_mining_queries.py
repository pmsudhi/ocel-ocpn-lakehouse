#!/usr/bin/env python3
"""
Process Mining Query Engine
Unified interface for all process mining queries with intelligent optimization
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
import json
import time

import daft
from pyiceberg.catalog import load_catalog
from lakehouse.analytics.pm4py_analytics_wrapper import PM4PyAnalyticsWrapper

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


class ProcessMiningQueryEngine:
    """Unified query engine for process mining analytics."""
    
    def __init__(self, catalog, model_id: str = None):
        self.catalog = catalog
        self.model_id = model_id
        self.query_cache = {}
        self.query_stats = {
            'total_queries': 0,
            'cache_hits': 0,
            'avg_response_time': 0.0
        }
        # Initialize pm4py analytics wrapper
        self.pm4py_analytics = PM4PyAnalyticsWrapper(catalog, model_id)
    
    def get_process_variants(self, top_n: int = 10, 
                           min_frequency: int = 1,
                           instance_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Get most common process variants using pm4py analytics.
        Uses materialized view when available for 10-100x performance.
        """
        cache_key = f"variants_{top_n}_{min_frequency}_{instance_type}"
        if cache_key in self.query_cache:
            self.query_stats['cache_hits'] += 1
            return self.query_cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Validate inputs
            if top_n <= 0:
                return {'error': 'top_n must be positive'}
            if min_frequency < 0:
                return {'error': 'min_frequency must be non-negative'}
            
            # Try to use materialized view first
            if self.catalog.table_exists('ocel.process_variants'):
                try:
                    print("Using materialized view for process variants...")
                    variants_table = self.catalog.load_table('ocel.process_variants')
                    df_variants = daft.read_iceberg(variants_table)
                    
                    # Filter by minimum frequency
                    df_filtered = df_variants.where(daft.col("frequency") >= min_frequency)
                    
                    # Sort by frequency and take top N
                    df_top = df_filtered.sort("frequency", desc=True).limit(top_n)
                    
                    variants = []
                    for row in df_top.to_pandas().itertuples():
                        variants.append({
                            'variant_id': row.variant_id,
                            'pattern': row.variant_pattern,
                            'frequency': row.frequency,
                            'avg_duration_seconds': row.avg_duration_seconds,
                            'sample_instances': row.instance_ids[:5] if row.instance_ids else []
                        })
                    
                    result = {
                        'variants': variants,
                        'total_variants': len(variants),
                        'query_time_ms': (time.time() - start_time) * 1000,
                        'source': 'materialized_view'
                    }
                except Exception as e:
                    print(f"[WARNING] Materialized view failed, falling back to pm4py: {e}")
                    # Fallback to pm4py analytics
                    print("Computing process variants using pm4py...")
                    result = self._get_variants_with_pm4py(top_n, min_frequency, instance_type)
                    if 'error' in result:
                        return {'error': f'Both materialized view and pm4py failed. pm4py error: {result["error"]}'}
                    result['source'] = 'pm4py_analytics'
            else:
                # Use pm4py analytics directly
                print("Computing process variants using pm4py...")
                result = self._get_variants_with_pm4py(top_n, min_frequency, instance_type)
                if 'error' in result:
                    return {'error': f'pm4py computation failed: {result["error"]}'}
                result['source'] = 'pm4py_analytics'
            
            # Cache result
            self.query_cache[cache_key] = result
            self.query_stats['total_queries'] += 1
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to get process variants: {e}'}
    
    def get_case_duration_distribution(self, instance_type: Optional[str] = None) -> Dict[str, Any]:
        """Get distribution of case durations with percentiles."""
        cache_key = f"duration_dist_{instance_type}"
        if cache_key in self.query_cache:
            self.query_stats['cache_hits'] += 1
            return self.query_cache[cache_key]
        
        start_time = time.time()
        
        try:
            instances_table = self.catalog.load_table('ocel.process_instances')
            df_instances = daft.read_iceberg(instances_table)
            
            # Filter by instance type if specified
            if instance_type:
                df_instances = df_instances.where(daft.col("instance_type") == instance_type)
            
            # Get duration statistics
            duration_stats = df_instances.select("duration_seconds").to_pandas()['duration_seconds']
            duration_stats = duration_stats.dropna()
            
            if len(duration_stats) == 0:
                return {'error': 'No duration data available'}
            
            # Calculate percentiles
            percentiles = [10, 25, 50, 75, 90, 95, 99]
            percentile_values = {}
            for p in percentiles:
                percentile_values[f'p{p}'] = duration_stats.quantile(p/100)
            
            result = {
                'total_cases': len(duration_stats),
                'mean_duration_seconds': duration_stats.mean(),
                'median_duration_seconds': duration_stats.median(),
                'min_duration_seconds': duration_stats.min(),
                'max_duration_seconds': duration_stats.max(),
                'std_duration_seconds': duration_stats.std(),
                'percentiles': percentile_values,
                'query_time_ms': (time.time() - start_time) * 1000
            }
            
            self.query_cache[cache_key] = result
            self.query_stats['total_queries'] += 1
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to get case duration distribution: {e}'}
    
    def find_similar_cases(self, instance_id: str, 
                          similarity_metric: str = 'levenshtein',
                          max_results: int = 10) -> Dict[str, Any]:
        """Find cases with similar event sequences."""
        cache_key = f"similar_{instance_id}_{similarity_metric}_{max_results}"
        if cache_key in self.query_cache:
            self.query_stats['cache_hits'] += 1
            return self.query_cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Get the target instance's event sequence
            target_sequence = self._get_instance_sequence(instance_id)
            if not target_sequence:
                return {'error': f'Instance {instance_id} not found'}
            
            # Get all other instances and their sequences
            all_sequences = self._get_all_instance_sequences()
            
            # Calculate similarities
            similarities = []
            for other_instance_id, sequence in all_sequences.items():
                if other_instance_id == instance_id:
                    continue
                
                similarity_score = self._calculate_sequence_similarity(
                    target_sequence, sequence, similarity_metric
                )
                
                similarities.append({
                    'instance_id': other_instance_id,
                    'similarity_score': similarity_score,
                    'sequence_length': len(sequence)
                })
            
            # Sort by similarity and take top results
            similarities.sort(key=lambda x: x['similarity_score'], reverse=True)
            top_similarities = similarities[:max_results]
            
            result = {
                'target_instance': instance_id,
                'target_sequence': target_sequence,
                'similar_cases': top_similarities,
                'total_compared': len(similarities),
                'query_time_ms': (time.time() - start_time) * 1000
            }
            
            self.query_cache[cache_key] = result
            self.query_stats['total_queries'] += 1
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to find similar cases: {e}'}
    
    def get_activity_frequencies(self, start_date: Optional[str] = None, 
                               end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get activity frequencies, using materialized view when available."""
        cache_key = f"activity_freq_{start_date}_{end_date}"
        if cache_key in self.query_cache:
            self.query_stats['cache_hits'] += 1
            return self.query_cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Try to use materialized view first
            if self.catalog.table_exists('ocel.activity_metrics'):
                print("Using materialized view for activity frequencies...")
                metrics_table = self.catalog.load_table('ocel.activity_metrics')
                df_metrics = daft.read_iceberg(metrics_table)
                
                # Apply date filters if specified
                if start_date or end_date:
                    # Would need to join with events table for date filtering
                    # For now, return all metrics
                    pass
                
                activities = []
                for row in df_metrics.to_pandas().itertuples():
                    activities.append({
                        'activity_name': row.activity_name,
                        'total_occurrences': row.total_occurrences,
                        'avg_duration_seconds': row.avg_duration_seconds,
                        'instances_with_activity': row.instances_with_activity
                    })
                
                result = {
                    'activities': activities,
                    'total_activities': len(activities),
                    'query_time_ms': (time.time() - start_time) * 1000,
                    'source': 'materialized_view'
                }
            else:
                # Fallback to raw computation
                print("Computing activity frequencies from raw data...")
                result = self._compute_activity_frequencies_from_raw(start_date, end_date)
                result['source'] = 'raw_computation'
            
            self.query_cache[cache_key] = result
            self.query_stats['total_queries'] += 1
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to get activity frequencies: {e}'}
    
    def identify_bottlenecks(self, threshold_percentile: int = 90) -> Dict[str, Any]:
        """Identify activities with long waiting times (bottlenecks)."""
        cache_key = f"bottlenecks_{threshold_percentile}"
        if cache_key in self.query_cache:
            self.query_stats['cache_hits'] += 1
            return self.query_cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Get activity metrics
            activity_freq = self.get_activity_frequencies()
            if 'error' in activity_freq:
                return activity_freq
            
            # Calculate threshold
            durations = [act.get('avg_duration_seconds', 0) for act in activity_freq['activities']]
            durations = [d for d in durations if d is not None and d > 0]
            
            if not durations:
                return {'error': 'No duration data available'}
            
            threshold = sorted(durations)[int(len(durations) * threshold_percentile / 100)]
            
            # Find bottlenecks
            bottlenecks = []
            for activity in activity_freq['activities']:
                duration = activity.get('avg_duration_seconds', 0)
                if duration and duration >= threshold:
                    bottlenecks.append({
                        'activity_name': activity['activity_name'],
                        'avg_duration_seconds': duration,
                        'total_occurrences': activity['total_occurrences'],
                        'severity': 'high' if duration >= threshold * 1.5 else 'medium'
                    })
            
            # Sort by duration
            bottlenecks.sort(key=lambda x: x['avg_duration_seconds'], reverse=True)
            
            result = {
                'bottlenecks': bottlenecks,
                'threshold_seconds': threshold,
                'threshold_percentile': threshold_percentile,
                'total_bottlenecks': len(bottlenecks),
                'query_time_ms': (time.time() - start_time) * 1000
            }
            
            self.query_cache[cache_key] = result
            self.query_stats['total_queries'] += 1
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to identify bottlenecks: {e}'}
    
    def get_resource_utilization(self, resource_attribute: str = 'user') -> Dict[str, Any]:
        """Analyze resource workload from event attributes."""
        cache_key = f"resource_util_{resource_attribute}"
        if cache_key in self.query_cache:
            self.query_stats['cache_hits'] += 1
            return self.query_cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Get events with resource attributes
            events_table = self.catalog.load_table('ocel.events')
            event_attrs_table = self.catalog.load_table('ocel.event_attributes')
            
            df_events = daft.read_iceberg(events_table)
            df_attrs = daft.read_iceberg(event_attrs_table)
            
            # Join to get resource information
            df_with_resources = df_events.join(
                df_attrs.where(daft.col("name") == resource_attribute),
                left_on="id",
                right_on="event_id",
                how="inner"
            )
            
            # Group by resource and calculate utilization
            resource_stats = df_with_resources.groupby("val_string").agg({
                "id": "count",
                "time": ["min", "max"]
            })
            
            resources = []
            for row in resource_stats.to_pandas().itertuples():
                resource_name = row.Index
                event_count = row.id_count
                min_time = row.time_min
                max_time = row.time_max
                
                # Calculate utilization metrics
                total_time = (max_time - min_time).total_seconds() if min_time and max_time else 0
                avg_events_per_hour = (event_count * 3600) / total_time if total_time > 0 else 0
                
                resources.append({
                    'resource_name': resource_name,
                    'total_events': event_count,
                    'time_span_seconds': total_time,
                    'avg_events_per_hour': avg_events_per_hour,
                    'utilization_score': min(avg_events_per_hour / 10, 1.0)  # Normalized score
                })
            
            # Sort by utilization
            resources.sort(key=lambda x: x['utilization_score'], reverse=True)
            
            result = {
                'resources': resources,
                'total_resources': len(resources),
                'resource_attribute': resource_attribute,
                'query_time_ms': (time.time() - start_time) * 1000
            }
            
            self.query_cache[cache_key] = result
            self.query_stats['total_queries'] += 1
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to get resource utilization: {e}'}
    
    def get_object_lifecycle(self, object_id: str) -> Dict[str, Any]:
        """Get complete lifecycle of an object with all events."""
        cache_key = f"lifecycle_{object_id}"
        if cache_key in self.query_cache:
            self.query_stats['cache_hits'] += 1
            return self.query_cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Get object information
            objects_table = self.catalog.load_table('ocel.objects')
            df_objects = daft.read_iceberg(objects_table)
            object_info = df_objects.where(daft.col("id") == object_id).to_pandas()
            
            if object_info.empty:
                return {'error': f'Object {object_id} not found'}
            
            object_row = object_info.iloc[0]
            
            # Get events involving this object
            event_objects_table = self.catalog.load_table('ocel.event_objects')
            events_table = self.catalog.load_table('ocel.events')
            
            df_event_objects = daft.read_iceberg(event_objects_table)
            df_events = daft.read_iceberg(events_table)
            
            # Join to get events
            object_events = df_event_objects.where(
                daft.col("object_id") == object_id
            ).join(
                df_events,
                left_on="event_id",
                right_on="id",
                how="inner"
            ).sort("time")
            
            # Build lifecycle
            lifecycle_events = []
            for row in object_events.to_pandas().itertuples():
                lifecycle_events.append({
                    'event_id': row.event_id,
                    'event_type': row.type,
                    'timestamp': row.time,
                    'qualifier': row.qualifier
                })
            
            result = {
                'object_id': object_id,
                'object_type': object_row['type'],
                'created_at': object_row.get('created_at'),
                'lifecycle_state': object_row.get('lifecycle_state'),
                'lifecycle_events': lifecycle_events,
                'total_events': len(lifecycle_events),
                'query_time_ms': (time.time() - start_time) * 1000
            }
            
            self.query_cache[cache_key] = result
            self.query_stats['total_queries'] += 1
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to get object lifecycle: {e}'}
    
    def get_object_interaction_graph(self) -> Dict[str, Any]:
        """Get object interaction patterns."""
        cache_key = "object_interactions"
        if cache_key in self.query_cache:
            self.query_stats['cache_hits'] += 1
            return self.query_cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Try to use materialized view first
            if self.catalog.table_exists('ocel.object_interaction_graph'):
                print("Using materialized view for object interactions...")
                interactions_table = self.catalog.load_table('ocel.object_interaction_graph')
                df_interactions = daft.read_iceberg(interactions_table)
                
                interactions = []
                for row in df_interactions.to_pandas().itertuples():
                    interactions.append({
                        'source_type': row.source_object_type,
                        'target_type': row.target_object_type,
                        'interaction_count': row.interaction_count,
                        'common_events': row.common_events if hasattr(row, 'common_events') else []
                    })
                
                result = {
                    'interactions': interactions,
                    'total_interactions': len(interactions),
                    'query_time_ms': (time.time() - start_time) * 1000,
                    'source': 'materialized_view'
                }
            else:
                # Fallback to raw computation
                print("Computing object interactions from raw data...")
                result = self._compute_object_interactions_from_raw()
                result['source'] = 'raw_computation'
            
            self.query_cache[cache_key] = result
            self.query_stats['total_queries'] += 1
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to get object interaction graph: {e}'}
    
    def check_conformance(self, instance_id: str, model_id: str) -> Dict[str, Any]:
        """Check if instance conforms to discovered model."""
        cache_key = f"conformance_{instance_id}_{model_id}"
        if cache_key in self.query_cache:
            self.query_stats['cache_hits'] += 1
            return self.query_cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Import conformance checker
            from conformance_checking import ConformanceChecker
            checker = ConformanceChecker(self.catalog)
            
            # Check conformance for this specific instance
            metrics = checker.compute_conformance_metrics(model_id, [instance_id])
            
            if 'error' in metrics:
                return metrics
            
            # Extract result for this instance
            instance_results = metrics.get('instance_results', [])
            if not instance_results:
                return {'error': f'No conformance data for instance {instance_id}'}
            
            result = instance_results[0]  # Should be the only result
            result['query_time_ms'] = (time.time() - start_time) * 1000
            
            self.query_cache[cache_key] = result
            self.query_stats['total_queries'] += 1
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to check conformance: {e}'}
    
    def predict_next_activity(self, instance_id: str, 
                            ml_model: Optional[str] = None) -> Dict[str, Any]:
        """Predict next likely activity using transition probabilities."""
        cache_key = f"predict_{instance_id}_{ml_model}"
        if cache_key in self.query_cache:
            self.query_stats['cache_hits'] += 1
            return self.query_cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Get current instance sequence
            current_sequence = self._get_instance_sequence(instance_id)
            if not current_sequence:
                return {'error': f'Instance {instance_id} not found'}
            
            # Get DFG matrix for transition probabilities
            if self.catalog.table_exists('ocel.dfg_matrix'):
                dfg_table = self.catalog.load_table('ocel.dfg_matrix')
                df_dfg = daft.read_iceberg(dfg_table)
                
                # Find transitions from current activity
                current_activity = current_sequence[-1] if current_sequence else None
                if not current_activity:
                    return {'error': 'No current activity found'}
                
                # Get possible next activities
                next_activities = df_dfg.where(
                    daft.col("from_activity") == current_activity
                ).to_pandas()
                
                if next_activities.empty:
                    return {
                        'instance_id': instance_id,
                        'current_activity': current_activity,
                        'predictions': [],
                        'message': 'No transitions found from current activity'
                    }
                
                # Calculate probabilities
                predictions = []
                total_frequency = next_activities['frequency'].sum()
                
                for _, row in next_activities.iterrows():
                    probability = row['frequency'] / total_frequency
                    predictions.append({
                        'next_activity': row['to_activity'],
                        'probability': probability,
                        'frequency': row['frequency'],
                        'avg_time_seconds': row.get('avg_time_seconds')
                    })
                
                # Sort by probability
                predictions.sort(key=lambda x: x['probability'], reverse=True)
                
                result = {
                    'instance_id': instance_id,
                    'current_activity': current_activity,
                    'current_sequence': current_sequence,
                    'predictions': predictions[:5],  # Top 5 predictions
                    'query_time_ms': (time.time() - start_time) * 1000
                }
            else:
                return {'error': 'DFG matrix not available for predictions'}
            
            self.query_cache[cache_key] = result
            self.query_stats['total_queries'] += 1
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to predict next activity: {e}'}
    
    def predict_case_outcome(self, instance_id: str) -> Dict[str, Any]:
        """Predict if case will complete successfully."""
        cache_key = f"outcome_{instance_id}"
        if cache_key in self.query_cache:
            self.query_stats['cache_hits'] += 1
            return self.query_cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Get instance information
            instances_table = self.catalog.load_table('ocel.process_instances')
            df_instances = daft.read_iceberg(instances_table)
            instance_info = df_instances.where(daft.col("instance_id") == instance_id).to_pandas()
            
            if instance_info.empty:
                return {'error': f'Instance {instance_id} not found'}
            
            instance_row = instance_info.iloc[0]
            
            # Get current status and progress
            current_status = instance_row['status']
            duration_seconds = instance_row.get('duration_seconds', 0)
            instance_type = instance_row['instance_type']
            
            # Get similar completed cases for comparison
            similar_cases = self.find_similar_cases(instance_id, max_results=10)
            
            # Calculate completion probability based on:
            # 1. Current status
            # 2. Duration compared to similar cases
            # 3. Instance type patterns
            
            completion_probability = 0.5  # Default
            
            if current_status == 'completed':
                completion_probability = 1.0
            elif current_status == 'failed' or current_status == 'cancelled':
                completion_probability = 0.0
            elif current_status == 'running':
                # Analyze duration and similar cases
                if 'similar_cases' in similar_cases and similar_cases['similar_cases']:
                    # Use similar cases to estimate completion
                    avg_similar_duration = sum(
                        case.get('sequence_length', 0) for case in similar_cases['similar_cases']
                    ) / len(similar_cases['similar_cases'])
                    
                    if duration_seconds > 0:
                        progress_ratio = min(duration_seconds / avg_similar_duration, 1.0)
                        completion_probability = 0.3 + (progress_ratio * 0.7)  # 30% base + progress
                    else:
                        completion_probability = 0.3  # Default for running cases
                else:
                    completion_probability = 0.3  # Default for running cases
            
            # Determine risk factors
            risk_factors = []
            if duration_seconds > 86400:  # More than 1 day
                risk_factors.append('long_duration')
            if instance_type == 'invoice_process':
                risk_factors.append('complex_process')
            
            result = {
                'instance_id': instance_id,
                'current_status': current_status,
                'duration_seconds': duration_seconds,
                'completion_probability': completion_probability,
                'risk_factors': risk_factors,
                'confidence': 'high' if len(risk_factors) == 0 else 'medium',
                'query_time_ms': (time.time() - start_time) * 1000
            }
            
            self.query_cache[cache_key] = result
            self.query_stats['total_queries'] += 1
            
            return result
            
        except Exception as e:
            return {'error': f'Failed to predict case outcome: {e}'}
    
    def _get_instance_sequence(self, instance_id: str) -> List[str]:
        """Get event sequence for an instance."""
        try:
            instance_events_table = self.catalog.load_table('ocel.instance_events')
            events_table = self.catalog.load_table('ocel.events')
            
            df_instance_events = daft.read_iceberg(instance_events_table)
            df_events = daft.read_iceberg(events_table)
            
            sequence = df_instance_events.where(
                daft.col("instance_id") == instance_id
            ).join(
                df_events,
                left_on="event_id",
                right_on="id",
                how="inner"
            ).sort("event_sequence").select("type").to_pandas()['type'].tolist()
            
            return sequence
            
        except Exception:
            return []
    
    def _get_all_instance_sequences(self) -> Dict[str, List[str]]:
        """Get all instance sequences for similarity comparison."""
        try:
            instance_events_table = self.catalog.load_table('ocel.instance_events')
            events_table = self.catalog.load_table('ocel.events')
            
            df_instance_events = daft.read_iceberg(instance_events_table)
            df_events = daft.read_iceberg(events_table)
            
            # Join and group by instance
            df_sequences = df_instance_events.join(
                df_events,
                left_on="event_id",
                right_on="id",
                how="inner"
            ).sort(["instance_id", "event_sequence"])
            
            sequences = {}
            for row in df_sequences.to_pandas().itertuples():
                instance_id = row.instance_id
                if instance_id not in sequences:
                    sequences[instance_id] = []
                sequences[instance_id].append(row.type)
            
            return sequences
            
        except Exception:
            return {}
    
    def _calculate_sequence_similarity(self, seq1: List[str], seq2: List[str], 
                                     metric: str) -> float:
        """Calculate similarity between two sequences."""
        if metric == 'levenshtein':
            return self._levenshtein_similarity(seq1, seq2)
        elif metric == 'jaccard':
            return self._jaccard_similarity(seq1, seq2)
        else:
            return self._jaccard_similarity(seq1, seq2)  # Default
    
    def _levenshtein_similarity(self, seq1: List[str], seq2: List[str]) -> float:
        """Calculate Levenshtein similarity between sequences."""
        # Simplified implementation
        if not seq1 or not seq2:
            return 0.0
        
        # Convert to strings for comparison
        str1 = '->'.join(seq1)
        str2 = '->'.join(seq2)
        
        # Simple similarity based on common subsequences
        common_elements = set(seq1).intersection(set(seq2))
        return len(common_elements) / max(len(set(seq1)), len(set(seq2)))
    
    def _jaccard_similarity(self, seq1: List[str], seq2: List[str]) -> float:
        """Calculate Jaccard similarity between sequences."""
        if not seq1 or not seq2:
            return 0.0
        
        set1 = set(seq1)
        set2 = set(seq2)
        
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0.0
    
    def _get_variants_with_pm4py(self, top_n: int, min_frequency: int, 
                                instance_type: Optional[str]) -> Dict[str, Any]:
        """
        Get process variants using pm4py analytics
        
        Args:
            top_n: Number of top variants to return
            min_frequency: Minimum frequency threshold
            instance_type: Optional instance type filter
            
        Returns:
            Dictionary with variants data
        """
        try:
            # Use pm4py analytics wrapper
            variants = self.pm4py_analytics.get_process_variants(top_n)
            
            # Filter by minimum frequency
            filtered_variants = [
                (variant, freq) for variant, freq in variants 
                if freq >= min_frequency
            ]
            
            # Convert to expected format
            result_variants = []
            for i, (variant, frequency) in enumerate(filtered_variants[:top_n]):
                result_variants.append({
                    'variant_id': f"variant_{i+1:06d}",
                    'pattern': variant,
                    'frequency': frequency,
                    'avg_duration_seconds': 0.0,  # Would need additional calculation
                    'sample_instances': []  # Would need additional calculation
                })
            
            return {
                'variants': result_variants,
                'total_variants': len(filtered_variants),
                'query_time_ms': 0,  # Would measure actual time
                'source': 'pm4py_analytics'
            }
            
        except Exception as e:
            return {'error': f'pm4py variants computation failed: {e}'}
    
    def _compute_variants_from_raw_data(self, top_n: int, min_frequency: int, 
                                      instance_type: Optional[str]) -> Dict[str, Any]:
        """Fallback method to compute variants from raw data."""
        try:
            # Get process instances
            instances_table = self.catalog.load_table('ocel.process_instances')
            df_instances = daft.read_iceberg(instances_table)
            
            # Filter by instance type if specified
            if instance_type:
                df_instances = df_instances.where(daft.col("instance_type") == instance_type)
            
            # Get instance events and events
            instance_events_table = self.catalog.load_table('ocel.instance_events')
            events_table = self.catalog.load_table('ocel.events')
            
            df_instance_events = daft.read_iceberg(instance_events_table)
            df_events = daft.read_iceberg(events_table)
            
            # Join to get event sequences
            df_sequences = df_instance_events.join(
                df_events,
                left_on="event_id",
                right_on="id",
                how="inner"
            ).sort(["instance_id", "event_sequence"])
            
            # Group by instance to get activity sequences
            instance_sequences = df_sequences.groupby("instance_id").agg({
                "type": "collect"
            })
            
            # Build variant patterns
            variant_patterns = {}
            for row in instance_sequences.to_pandas().itertuples():
                instance_id = row.Index
                activities = row.type
                
                if len(activities) > 1:
                    # Create pattern string (A->B->C->D)
                    pattern = "->".join(activities)
                    
                    if pattern not in variant_patterns:
                        variant_patterns[pattern] = {
                            'variant_id': f"variant_{hash(pattern) % 1000000:06d}",
                            'variant_pattern': pattern,
                            'frequency': 0,
                            'instance_ids': []
                        }
                    
                    variant_patterns[pattern]['frequency'] += 1
                    variant_patterns[pattern]['instance_ids'].append(instance_id)
            
            # Filter by minimum frequency and sort
            filtered_variants = [
                v for v in variant_patterns.values() 
                if v['frequency'] >= min_frequency
            ]
            filtered_variants.sort(key=lambda x: x['frequency'], reverse=True)
            
            # Take top N
            top_variants = filtered_variants[:top_n]
            
            return {
                'variants': top_variants,
                'total_variants': len(filtered_variants),
                'query_time_ms': 0,  # Would measure actual time
                'source': 'raw_computation'
            }
            
        except Exception as e:
            return {'error': f'Raw variants computation failed: {e}'}
    
    def _compute_activity_frequencies_from_raw(self, start_date: Optional[str], 
                                             end_date: Optional[str]) -> Dict[str, Any]:
        """Fallback method to compute activity frequencies from raw data."""
        try:
            # Get events table
            events_table = self.catalog.load_table('ocel.events')
            df_events = daft.read_iceberg(events_table)
            
            # Apply date filters if provided
            if start_date:
                df_events = df_events.where(daft.col("time") >= start_date)
            if end_date:
                df_events = df_events.where(daft.col("time") <= end_date)
            
            # Group by event type and count
            activity_counts = df_events.groupby("type").agg({
                "id": "count"
            }).with_column("activity_name", daft.col("type"))
            
            # Convert to list for return
            activities = []
            for row in activity_counts.to_pandas().itertuples():
                activities.append({
                    'activity_name': row.activity_name,
                    'total_occurrences': row.id,
                    'avg_duration_seconds': 0.0,  # Would need more complex calculation
                    'min_duration_seconds': 0.0,
                    'max_duration_seconds': 0.0,
                    'resource_count': 0  # Would need resource analysis
                })
            
            # Sort by frequency
            activities.sort(key=lambda x: x['total_occurrences'], reverse=True)
            
            return {
                'activities': activities,
                'total_activities': len(activities),
                'query_time_ms': 0,
                'source': 'raw_computation'
            }
            
        except Exception as e:
            return {'error': f'Raw activity frequencies computation failed: {e}'}
    
    def _compute_object_interactions_from_raw(self) -> Dict[str, Any]:
        """Fallback method to compute object interactions from raw data."""
        try:
            # Get event-object relationships and objects
            event_objects_table = self.catalog.load_table('ocel.event_objects')
            objects_table = self.catalog.load_table('ocel.objects')
            
            df_event_objects = daft.read_iceberg(event_objects_table)
            df_objects = daft.read_iceberg(objects_table)
            
            # Join to get object types
            df_enriched = df_event_objects.join(
                df_objects.select(daft.col("id").alias("object_id"), daft.col("type").alias("object_type")),
                on="object_id",
                how="inner"
            )
            
            # Group by event to find objects that appear together
            event_object_groups = df_enriched.groupby("event_id").agg({
                "object_type": "collect"
            })
            
            # Build interaction patterns
            interactions = {}
            for row in event_object_groups.to_pandas().itertuples():
                event_id = row.Index
                object_types = row.object_type
                
                # Create pairs of object types that interact
                for i in range(len(object_types)):
                    for j in range(i + 1, len(object_types)):
                        type1, type2 = object_types[i], object_types[j]
                        # Ensure consistent ordering
                        if type1 > type2:
                            type1, type2 = type2, type1
                        
                        interaction_key = f"{type1}->{type2}"
                        
                        if interaction_key not in interactions:
                            interactions[interaction_key] = {
                                'source_object_type': type1,
                                'target_object_type': type2,
                                'interaction_count': 0,
                                'common_events': []
                            }
                        
                        interactions[interaction_key]['interaction_count'] += 1
                        interactions[interaction_key]['common_events'].append(event_id)
            
            # Convert to list and sort by interaction count
            interaction_list = list(interactions.values())
            interaction_list.sort(key=lambda x: x['interaction_count'], reverse=True)
            
            return {
                'interactions': interaction_list,
                'total_interactions': len(interaction_list),
                'query_time_ms': 0,
                'source': 'raw_computation'
            }
            
        except Exception as e:
            return {'error': f'Raw object interactions computation failed: {e}'}
    
    def get_query_stats(self) -> Dict[str, Any]:
        """Get query engine statistics."""
        cache_hit_rate = (self.query_stats['cache_hits'] / 
                         max(self.query_stats['total_queries'], 1)) * 100
        
        return {
            'total_queries': self.query_stats['total_queries'],
            'cache_hits': self.query_stats['cache_hits'],
            'cache_hit_rate': cache_hit_rate,
            'avg_response_time': self.query_stats['avg_response_time'],
            'cache_size': len(self.query_cache)
        }


def main():
    """Main function for testing the query engine."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process Mining Query Engine')
    parser.add_argument('--variants', action='store_true', help='Get process variants')
    parser.add_argument('--durations', action='store_true', help='Get case duration distribution')
    parser.add_argument('--similar', help='Find similar cases for instance ID')
    parser.add_argument('--activities', action='store_true', help='Get activity frequencies')
    parser.add_argument('--bottlenecks', action='store_true', help='Identify bottlenecks')
    parser.add_argument('--resources', action='store_true', help='Get resource utilization')
    parser.add_argument('--lifecycle', help='Get object lifecycle for object ID')
    parser.add_argument('--interactions', action='store_true', help='Get object interactions')
    parser.add_argument('--conformance', nargs=2, metavar=('INSTANCE_ID', 'MODEL_ID'), 
                       help='Check conformance')
    parser.add_argument('--predict', help='Predict next activity for instance ID')
    parser.add_argument('--outcome', help='Predict case outcome for instance ID')
    parser.add_argument('--stats', action='store_true', help='Show query statistics')
    
    args = parser.parse_args()
    
    catalog_path = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_path)
    
    engine = ProcessMiningQueryEngine(cat)
    
    if args.variants:
        result = engine.get_process_variants()
        print(f"Process Variants: {json.dumps(result, indent=2)}")
    
    elif args.durations:
        result = engine.get_case_duration_distribution()
        print(f"Case Duration Distribution: {json.dumps(result, indent=2)}")
    
    elif args.similar:
        result = engine.find_similar_cases(args.similar)
        print(f"Similar Cases: {json.dumps(result, indent=2)}")
    
    elif args.activities:
        result = engine.get_activity_frequencies()
        print(f"Activity Frequencies: {json.dumps(result, indent=2)}")
    
    elif args.bottlenecks:
        result = engine.identify_bottlenecks()
        print(f"Bottlenecks: {json.dumps(result, indent=2)}")
    
    elif args.resources:
        result = engine.get_resource_utilization()
        print(f"Resource Utilization: {json.dumps(result, indent=2)}")
    
    elif args.lifecycle:
        result = engine.get_object_lifecycle(args.lifecycle)
        print(f"Object Lifecycle: {json.dumps(result, indent=2)}")
    
    elif args.interactions:
        result = engine.get_object_interaction_graph()
        print(f"Object Interactions: {json.dumps(result, indent=2)}")
    
    elif args.conformance:
        instance_id, model_id = args.conformance
        result = engine.check_conformance(instance_id, model_id)
        print(f"Conformance: {json.dumps(result, indent=2)}")
    
    elif args.predict:
        result = engine.predict_next_activity(args.predict)
        print(f"Next Activity Prediction: {json.dumps(result, indent=2)}")
    
    elif args.outcome:
        result = engine.predict_case_outcome(args.outcome)
        print(f"Case Outcome Prediction: {json.dumps(result, indent=2)}")
    
    elif args.stats:
        stats = engine.get_query_stats()
        print(f"Query Engine Statistics: {json.dumps(stats, indent=2)}")
    
    else:
        print("No query specified. Use --help for available options.")


if __name__ == '__main__':
    main()
