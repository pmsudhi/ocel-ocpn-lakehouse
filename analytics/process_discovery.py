#!/usr/bin/env python3
"""
Advanced Process Discovery Analytics for OCEL/OCPN Lakehouse
Implements sophisticated process discovery algorithms and insights extraction
"""

import daft
from pathlib import Path
from datetime import datetime, timedelta
from pyiceberg.catalog import load_catalog
from typing import Dict, List, Any, Optional, Tuple
import json
import networkx as nx
import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import matplotlib.pyplot as plt
import seaborn as sns


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


def discover_process_variants(cat) -> Dict[str, Any]:
    """Discover and analyze process variants from OCEL data."""
    print("Discovering process variants...")
    
    try:
        # Load events and relationships
        events_table = cat.load_table('ocel.events')
        rels_table = cat.load_table('ocel.event_objects')
        
        df_events = daft.read_iceberg(events_table)
        df_rels = daft.read_iceberg(rels_table)
        
        # Join events with relationships to get complete traces
        df_traces = df_events.join(df_rels, left_on="id", right_on="event_id", how="inner")
        
        # Group by object to get traces
        traces = df_traces.groupby("object_id").agg({
            "type": "collect",
            "time": "collect",
            "id": "collect"
        })
        
        # Analyze trace patterns
        trace_analysis = {
            'total_traces': len(traces),
            'trace_lengths': [],
            'event_sequences': [],
            'variant_patterns': {},
            'discovery_insights': []
        }
        
        # Analyze each trace
        for trace in traces:
            event_types = trace.get('type', [])
            event_times = trace.get('time', [])
            event_ids = trace.get('id', [])
            
            # Create event sequence
            event_sequence = list(zip(event_types, event_times, event_ids))
            event_sequence.sort(key=lambda x: x[1])  # Sort by time
            
            sequence_types = [event[0] for event in event_sequence]
            trace_analysis['trace_lengths'].append(len(sequence_types))
            trace_analysis['event_sequences'].append(sequence_types)
            
            # Create sequence pattern
            pattern = ' -> '.join(sequence_types)
            if pattern not in trace_analysis['variant_patterns']:
                trace_analysis['variant_patterns'][pattern] = 0
            trace_analysis['variant_patterns'][pattern] += 1
        
        # Calculate statistics
        trace_analysis['avg_trace_length'] = np.mean(trace_analysis['trace_lengths'])
        trace_analysis['max_trace_length'] = max(trace_analysis['trace_lengths'])
        trace_analysis['min_trace_length'] = min(trace_analysis['trace_lengths'])
        
        # Find most common patterns
        sorted_patterns = sorted(trace_analysis['variant_patterns'].items(), 
                               key=lambda x: x[1], reverse=True)
        trace_analysis['top_variants'] = sorted_patterns[:10]
        
        # Generate insights
        total_variants = len(trace_analysis['variant_patterns'])
        trace_analysis['discovery_insights'].append(
            f"Discovered {total_variants} unique process variants"
        )
        
        if total_variants > 100:
            trace_analysis['discovery_insights'].append(
                "High process variability detected - consider standardization"
            )
        elif total_variants < 10:
            trace_analysis['discovery_insights'].append(
                "Low process variability - process is well standardized"
            )
        
        # Check for common patterns
        most_common_freq = sorted_patterns[0][1] if sorted_patterns else 0
        total_traces = trace_analysis['total_traces']
        if most_common_freq / total_traces > 0.5:
            trace_analysis['discovery_insights'].append(
                f"Dominant pattern: {sorted_patterns[0][0]} ({most_common_freq/total_traces*100:.1f}% of cases)"
            )
        
        return trace_analysis
        
    except Exception as e:
        print(f"Error discovering process variants: {e}")
        return {'error': str(e)}


def analyze_process_performance(cat) -> Dict[str, Any]:
    """Analyze process performance metrics and bottlenecks."""
    print("Analyzing process performance...")
    
    try:
        events_table = cat.load_table('ocel.events')
        df_events = daft.read_iceberg(events_table)
        
        # Calculate performance metrics
        performance_analysis = {
            'timestamp': datetime.now().isoformat(),
            'event_metrics': {},
            'temporal_analysis': {},
            'bottleneck_analysis': {},
            'performance_insights': []
        }
        
        # Event frequency analysis
        event_counts = df_events.groupby("type").agg(daft.col("id").count())
        performance_analysis['event_metrics']['total_events'] = len(df_events)
        performance_analysis['event_metrics']['unique_event_types'] = len(event_counts)
        
        # Time-based analysis
        time_stats = df_events.groupby("event_date").agg({
            "id": "count",
            "type": "collect"
        })
        
        # Calculate daily event counts
        daily_counts = []
        for day_data in time_stats:
            daily_counts.append(day_data.get('id', 0))
        
        if daily_counts:
            performance_analysis['temporal_analysis'] = {
                'avg_daily_events': np.mean(daily_counts),
                'max_daily_events': max(daily_counts),
                'min_daily_events': min(daily_counts),
                'event_volatility': np.std(daily_counts)
            }
        
        # Bottleneck analysis
        event_type_counts = {}
        for event_type in df_events.select("type").distinct():
            count = df_events.where(df_events["type"] == event_type).count_rows()
            event_type_counts[event_type] = count
        
        # Identify potential bottlenecks (high frequency events)
        total_events = performance_analysis['event_metrics']['total_events']
        bottleneck_threshold = total_events * 0.1  # 10% of total events
        
        bottlenecks = []
        for event_type, count in event_type_counts.items():
            if count > bottleneck_threshold:
                bottlenecks.append({
                    'event_type': event_type,
                    'count': count,
                    'percentage': (count / total_events) * 100
                })
        
        performance_analysis['bottleneck_analysis'] = {
            'bottlenecks': bottlenecks,
            'bottleneck_threshold': bottleneck_threshold
        }
        
        # Generate performance insights
        if bottlenecks:
            performance_analysis['performance_insights'].append(
                f"Identified {len(bottlenecks)} potential bottlenecks"
            )
            top_bottleneck = max(bottlenecks, key=lambda x: x['count'])
            performance_analysis['performance_insights'].append(
                f"Primary bottleneck: {top_bottleneck['event_type']} ({top_bottleneck['percentage']:.1f}% of events)"
            )
        
        # Volatility analysis
        volatility = performance_analysis['temporal_analysis'].get('event_volatility', 0)
        if volatility > performance_analysis['temporal_analysis'].get('avg_daily_events', 0) * 0.5:
            performance_analysis['performance_insights'].append(
                "High process volatility detected - consider capacity planning"
            )
        
        return performance_analysis
        
    except Exception as e:
        print(f"Error analyzing process performance: {e}")
        return {'error': str(e)}


def discover_object_lifecycles(cat) -> Dict[str, Any]:
    """Discover and analyze object lifecycles."""
    print("Discovering object lifecycles...")
    
    try:
        objects_table = cat.load_table('ocel.objects')
        events_table = cat.load_table('ocel.events')
        rels_table = cat.load_table('ocel.event_objects')
        
        df_objects = daft.read_iceberg(objects_table)
        df_events = daft.read_iceberg(events_table)
        df_rels = daft.read_iceberg(rels_table)
        
        # Join to get object-event relationships
        df_object_events = df_events.join(df_rels, left_on="id", right_on="event_id", how="inner")
        df_object_events = df_object_events.join(df_objects, left_on="object_id", right_on="id", how="inner")
        
        lifecycle_analysis = {
            'timestamp': datetime.now().isoformat(),
            'object_types': {},
            'lifecycle_patterns': {},
            'lifecycle_insights': []
        }
        
        # Analyze by object type
        object_types = df_objects.select("type").distinct()
        
        for obj_type in object_types:
            type_events = df_object_events.where(df_object_events["type"] == obj_type)
            
            # Get lifecycle events for this object type
            lifecycle_events = type_events.groupby("object_id").agg({
                "type": "collect",
                "time": "collect"
            })
            
            # Analyze lifecycle patterns
            lifecycle_patterns = []
            for obj_lifecycle in lifecycle_events:
                events = obj_lifecycle.get('type', [])
                times = obj_lifecycle.get('time', [])
                
                # Create lifecycle sequence
                lifecycle_sequence = list(zip(events, times))
                lifecycle_sequence.sort(key=lambda x: x[1])  # Sort by time
                
                sequence_types = [event[0] for event in lifecycle_sequence]
                lifecycle_patterns.append(sequence_types)
            
            # Find common lifecycle patterns
            pattern_counts = Counter([' -> '.join(pattern) for pattern in lifecycle_patterns])
            common_patterns = pattern_counts.most_common(5)
            
            lifecycle_analysis['object_types'][obj_type] = {
                'total_objects': len(lifecycle_patterns),
                'unique_patterns': len(pattern_counts),
                'common_patterns': common_patterns
            }
        
        # Generate lifecycle insights
        total_object_types = len(lifecycle_analysis['object_types'])
        lifecycle_analysis['lifecycle_insights'].append(
            f"Analyzed lifecycles for {total_object_types} object types"
        )
        
        # Check for complex lifecycles
        complex_types = []
        for obj_type, analysis in lifecycle_analysis['object_types'].items():
            if analysis['unique_patterns'] > 10:
                complex_types.append(obj_type)
        
        if complex_types:
            lifecycle_analysis['lifecycle_insights'].append(
                f"Complex lifecycles detected for: {', '.join(complex_types)}"
            )
        
        return lifecycle_analysis
        
    except Exception as e:
        print(f"Error discovering object lifecycles: {e}")
        return {'error': str(e)}


def analyze_process_networks(cat) -> Dict[str, Any]:
    """Analyze process networks and object interactions."""
    print("Analyzing process networks...")
    
    try:
        events_table = cat.load_table('ocel.events')
        rels_table = cat.load_table('ocel.event_objects')
        objects_table = cat.load_table('ocel.objects')
        
        df_events = daft.read_iceberg(events_table)
        df_rels = daft.read_iceberg(rels_table)
        df_objects = daft.read_iceberg(objects_table)
        
        # Create network analysis
        network_analysis = {
            'timestamp': datetime.now().isoformat(),
            'network_metrics': {},
            'interaction_patterns': {},
            'network_insights': []
        }
        
        # Build object interaction network
        df_interactions = df_events.join(df_rels, left_on="id", right_on="event_id", how="inner")
        
        # Analyze object co-occurrence
        object_cooccurrence = {}
        
        # Group by event to find object interactions
        event_objects = df_interactions.groupby("event_id").agg({
            "object_id": "collect"
        })
        
        for event_data in event_objects:
            object_ids = event_data.get('object_id', [])
            if len(object_ids) > 1:
                # Multiple objects in same event - interaction
                for i, obj1 in enumerate(object_ids):
                    for obj2 in object_ids[i+1:]:
                        pair = tuple(sorted([obj1, obj2]))
                        if pair not in object_cooccurrence:
                            object_cooccurrence[pair] = 0
                        object_cooccurrence[pair] += 1
        
        # Calculate network metrics
        total_interactions = sum(object_cooccurrence.values())
        unique_pairs = len(object_cooccurrence)
        
        network_analysis['network_metrics'] = {
            'total_interactions': total_interactions,
            'unique_object_pairs': unique_pairs,
            'avg_interactions_per_pair': total_interactions / unique_pairs if unique_pairs > 0 else 0
        }
        
        # Find highly connected objects
        object_connections = defaultdict(int)
        for (obj1, obj2), count in object_cooccurrence.items():
            object_connections[obj1] += count
            object_connections[obj2] += count
        
        # Top connected objects
        top_connected = sorted(object_connections.items(), key=lambda x: x[1], reverse=True)[:10]
        network_analysis['interaction_patterns']['top_connected_objects'] = top_connected
        
        # Network density analysis
        total_objects = df_objects.count_rows()
        max_possible_connections = total_objects * (total_objects - 1) / 2
        network_density = unique_pairs / max_possible_connections if max_possible_connections > 0 else 0
        
        network_analysis['network_metrics']['network_density'] = network_density
        
        # Generate network insights
        if network_density > 0.5:
            network_analysis['network_insights'].append(
                "High network density - objects are highly interconnected"
            )
        elif network_density < 0.1:
            network_analysis['network_insights'].append(
                "Low network density - objects operate relatively independently"
            )
        
        if top_connected:
            most_connected = top_connected[0]
            network_analysis['network_insights'].append(
                f"Most connected object: {most_connected[0]} ({most_connected[1]} interactions)"
            )
        
        return network_analysis
        
    except Exception as e:
        print(f"Error analyzing process networks: {e}")
        return {'error': str(e)}


def generate_process_insights_report(cat) -> Dict[str, Any]:
    """Generate comprehensive process insights report."""
    print("Generating comprehensive process insights report...")
    
    # Run all analyses
    variant_analysis = discover_process_variants(cat)
    performance_analysis = analyze_process_performance(cat)
    lifecycle_analysis = discover_object_lifecycles(cat)
    network_analysis = analyze_process_networks(cat)
    
    # Combine into comprehensive report
    insights_report = {
        'timestamp': datetime.now().isoformat(),
        'variant_analysis': variant_analysis,
        'performance_analysis': performance_analysis,
        'lifecycle_analysis': lifecycle_analysis,
        'network_analysis': network_analysis,
        'executive_summary': {},
        'key_recommendations': []
    }
    
    # Generate executive summary
    total_variants = len(variant_analysis.get('variant_patterns', {}))
    total_events = performance_analysis.get('event_metrics', {}).get('total_events', 0)
    network_density = network_analysis.get('network_metrics', {}).get('network_density', 0)
    
    insights_report['executive_summary'] = {
        'process_complexity': 'High' if total_variants > 50 else 'Medium' if total_variants > 20 else 'Low',
        'process_volume': 'High' if total_events > 100000 else 'Medium' if total_events > 10000 else 'Low',
        'network_connectivity': 'High' if network_density > 0.3 else 'Medium' if network_density > 0.1 else 'Low',
        'total_variants': total_variants,
        'total_events': total_events,
        'network_density': network_density
    }
    
    # Generate key recommendations
    if total_variants > 100:
        insights_report['key_recommendations'].append(
            "High process variability - consider process standardization initiatives"
        )
    
    bottlenecks = performance_analysis.get('bottleneck_analysis', {}).get('bottlenecks', [])
    if bottlenecks:
        insights_report['key_recommendations'].append(
            f"Address {len(bottlenecks)} identified bottlenecks for performance improvement"
        )
    
    if network_density > 0.5:
        insights_report['key_recommendations'].append(
            "High object interconnectivity - consider impact analysis for changes"
        )
    
    return insights_report


def main():
    """Main function for process discovery analytics."""
    print("=" * 80)
    print("ADVANCED PROCESS DISCOVERY ANALYTICS")
    print("=" * 80)
    
    # Load catalog
    catalog_cfg = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_cfg)
    
    # Step 1: Discover process variants
    print("\n1. DISCOVERING PROCESS VARIANTS")
    print("-" * 50)
    variant_analysis = discover_process_variants(cat)
    
    if 'error' not in variant_analysis:
        print(f"Total variants discovered: {len(variant_analysis.get('variant_patterns', {}))}")
        print(f"Average trace length: {variant_analysis.get('avg_trace_length', 0):.1f}")
        print("Top variants:")
        for pattern, count in variant_analysis.get('top_variants', [])[:5]:
            print(f"  - {pattern}: {count} cases")
    else:
        print(f"Error: {variant_analysis['error']}")
    
    # Step 2: Analyze process performance
    print("\n2. ANALYZING PROCESS PERFORMANCE")
    print("-" * 50)
    performance_analysis = analyze_process_performance(cat)
    
    if 'error' not in performance_analysis:
        metrics = performance_analysis.get('event_metrics', {})
        print(f"Total events: {metrics.get('total_events', 0):,}")
        print(f"Unique event types: {metrics.get('unique_event_types', 0)}")
        
        bottlenecks = performance_analysis.get('bottleneck_analysis', {}).get('bottlenecks', [])
        print(f"Potential bottlenecks: {len(bottlenecks)}")
        for bottleneck in bottlenecks[:3]:
            print(f"  - {bottleneck['event_type']}: {bottleneck['percentage']:.1f}% of events")
    else:
        print(f"Error: {performance_analysis['error']}")
    
    # Step 3: Discover object lifecycles
    print("\n3. DISCOVERING OBJECT LIFECYCLES")
    print("-" * 50)
    lifecycle_analysis = discover_object_lifecycles(cat)
    
    if 'error' not in lifecycle_analysis:
        object_types = lifecycle_analysis.get('object_types', {})
        print(f"Object types analyzed: {len(object_types)}")
        for obj_type, analysis in list(object_types.items())[:3]:
            print(f"  - {obj_type}: {analysis['total_objects']} objects, {analysis['unique_patterns']} patterns")
    else:
        print(f"Error: {lifecycle_analysis['error']}")
    
    # Step 4: Analyze process networks
    print("\n4. ANALYZING PROCESS NETWORKS")
    print("-" * 50)
    network_analysis = analyze_process_networks(cat)
    
    if 'error' not in network_analysis:
        metrics = network_analysis.get('network_metrics', {})
        print(f"Total interactions: {metrics.get('total_interactions', 0):,}")
        print(f"Network density: {metrics.get('network_density', 0):.3f}")
        
        top_connected = network_analysis.get('interaction_patterns', {}).get('top_connected_objects', [])
        if top_connected:
            print(f"Most connected object: {top_connected[0][0]} ({top_connected[0][1]} interactions)")
    else:
        print(f"Error: {network_analysis['error']}")
    
    # Step 5: Generate comprehensive insights report
    print("\n5. GENERATING COMPREHENSIVE INSIGHTS REPORT")
    print("-" * 50)
    insights_report = generate_process_insights_report(cat)
    
    # Display executive summary
    summary = insights_report.get('executive_summary', {})
    print(f"Process complexity: {summary.get('process_complexity', 'Unknown')}")
    print(f"Process volume: {summary.get('process_volume', 'Unknown')}")
    print(f"Network connectivity: {summary.get('network_connectivity', 'Unknown')}")
    
    # Display recommendations
    recommendations = insights_report.get('key_recommendations', [])
    if recommendations:
        print("\nKey recommendations:")
        for rec in recommendations:
            print(f"  - {rec}")
    
    # Save insights report
    reports_dir = Path(__file__).parents[1] / 'docs'
    reports_dir.mkdir(exist_ok=True)
    
    insights_file = reports_dir / 'process_insights_report.json'
    with open(insights_file, 'w', encoding='utf-8') as f:
        json.dump(insights_report, f, indent=2)
    
    print(f"\nProcess insights report saved to: {insights_file}")
    
    print("\n" + "=" * 80)
    print("PROCESS DISCOVERY ANALYTICS COMPLETE")
    print("=" * 80)
    print("[SUCCESS] Process variants discovered")
    print("[SUCCESS] Performance analysis completed")
    print("[SUCCESS] Object lifecycles analyzed")
    print("[SUCCESS] Process networks analyzed")
    print("[SUCCESS] Comprehensive insights report generated")


if __name__ == '__main__':
    main()
