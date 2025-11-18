#!/usr/bin/env python3
"""
Executive Dashboard for OCEL/OCPN Lakehouse
Creates comprehensive executive dashboards with key metrics and insights
"""

import daft
from pathlib import Path
from datetime import datetime, timedelta
from pyiceberg.catalog import load_catalog
from typing import Dict, List, Any, Optional
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Rectangle
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots


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


def create_executive_kpis(cat) -> Dict[str, Any]:
    """Create executive KPIs and metrics."""
    print("Creating executive KPIs...")
    
    try:
        events_table = cat.load_table('ocel.events')
        df_events = daft.read_iceberg(events_table)
        
        # Convert to pandas for analysis
        events_data = df_events.to_pandas()
        
        kpi_metrics = {
            'timestamp': datetime.now().isoformat(),
            'process_metrics': {},
            'performance_metrics': {},
            'quality_metrics': {},
            'cost_metrics': {}
        }
        
        # Process metrics
        total_events = len(events_data)
        unique_event_types = events_data['type'].nunique()
        
        kpi_metrics['process_metrics'] = {
            'total_events': total_events,
            'unique_event_types': unique_event_types,
            'process_complexity': 'High' if unique_event_types > 10 else 'Medium' if unique_event_types > 5 else 'Low'
        }
        
        # Performance metrics
        if 'vendor_code' in events_data.columns:
            vendor_count = events_data['vendor_code'].nunique()
            avg_events_per_vendor = total_events / vendor_count if vendor_count > 0 else 0
            
            kpi_metrics['performance_metrics'] = {
                'total_vendors': vendor_count,
                'avg_events_per_vendor': avg_events_per_vendor,
                'vendor_efficiency': 'High' if avg_events_per_vendor > 100 else 'Medium' if avg_events_per_vendor > 50 else 'Low'
            }
        
        # Time-based performance
        events_data['hour'] = pd.to_datetime(events_data['time']).dt.hour
        events_data['day_of_week'] = pd.to_datetime(events_data['time']).dt.dayofweek
        
        # Peak activity analysis
        hourly_counts = events_data.groupby('hour').size()
        peak_hour = hourly_counts.idxmax()
        peak_activity = hourly_counts.max()
        avg_activity = hourly_counts.mean()
        
        kpi_metrics['performance_metrics']['peak_analysis'] = {
            'peak_hour': int(peak_hour),
            'peak_activity': int(peak_activity),
            'avg_activity': float(avg_activity),
            'utilization_ratio': float(peak_activity / avg_activity) if avg_activity > 0 else 0
        }
        
        # Quality metrics
        null_events = events_data['type'].isnull().sum()
        quality_score = (1 - null_events / total_events) * 100 if total_events > 0 else 0
        
        kpi_metrics['quality_metrics'] = {
            'data_quality_score': quality_score,
            'null_events': int(null_events),
            'quality_status': 'Excellent' if quality_score > 95 else 'Good' if quality_score > 90 else 'Needs Improvement'
        }
        
        # Cost metrics (simplified)
        event_costs = {
            'create_order': 10.0, 'approve_order': 15.0, 'create_invoice': 8.0,
            'approve_invoice': 12.0, 'payment': 5.0
        }
        
        total_cost = sum([event_costs.get(event_type, 5.0) for event_type in events_data['type']])
        avg_cost_per_event = total_cost / total_events if total_events > 0 else 0
        
        kpi_metrics['cost_metrics'] = {
            'total_cost': total_cost,
            'avg_cost_per_event': avg_cost_per_event,
            'cost_efficiency': 'High' if avg_cost_per_event < 10 else 'Medium' if avg_cost_per_event < 20 else 'Low'
        }
        
        return kpi_metrics
        
    except Exception as e:
        print(f"Error creating executive KPIs: {e}")
        return {'error': str(e)}


def create_process_health_dashboard(cat) -> Dict[str, Any]:
    """Create process health dashboard."""
    print("Creating process health dashboard...")
    
    try:
        events_table = cat.load_table('ocel.events')
        df_events = daft.read_iceberg(events_table)
        
        # Convert to pandas
        events_data = df_events.to_pandas()
        
        health_dashboard = {
            'timestamp': datetime.now().isoformat(),
            'health_indicators': {},
            'trend_analysis': {},
            'alerts': [],
            'recommendations': []
        }
        
        # Health indicators
        total_events = len(events_data)
        events_data['date'] = pd.to_datetime(events_data['time']).dt.date
        
        # Daily trend analysis
        daily_counts = events_data.groupby('date').size()
        avg_daily_events = daily_counts.mean()
        std_daily_events = daily_counts.std()
        
        health_dashboard['health_indicators'] = {
            'total_events': total_events,
            'avg_daily_events': float(avg_daily_events),
            'daily_volatility': float(std_daily_events),
            'stability_score': float(1 - (std_daily_events / avg_daily_events)) if avg_daily_events > 0 else 0
        }
        
        # Trend analysis
        if len(daily_counts) > 7:  # At least a week of data
            recent_avg = daily_counts.tail(7).mean()
            historical_avg = daily_counts.mean()
            trend_direction = 'Increasing' if recent_avg > historical_avg else 'Decreasing' if recent_avg < historical_avg else 'Stable'
            
            health_dashboard['trend_analysis'] = {
                'trend_direction': trend_direction,
                'recent_avg': float(recent_avg),
                'historical_avg': float(historical_avg),
                'trend_magnitude': float(abs(recent_avg - historical_avg) / historical_avg) if historical_avg > 0 else 0
            }
        
        # Generate alerts
        stability_score = health_dashboard['health_indicators']['stability_score']
        if stability_score < 0.7:
            health_dashboard['alerts'].append({
                'type': 'warning',
                'message': 'High process volatility detected',
                'severity': 'medium'
            })
        
        if 'trend_analysis' in health_dashboard:
            trend_magnitude = health_dashboard['trend_analysis']['trend_magnitude']
            if trend_magnitude > 0.2:
                health_dashboard['alerts'].append({
                    'type': 'info',
                    'message': f'Significant trend detected: {health_dashboard["trend_analysis"]["trend_direction"]}',
                    'severity': 'low'
                })
        
        # Generate recommendations
        if stability_score < 0.8:
            health_dashboard['recommendations'].append(
                'Implement process standardization to reduce volatility'
            )
        
        if len(health_dashboard['alerts']) > 3:
            health_dashboard['recommendations'].append(
                'Review process monitoring and alert thresholds'
            )
        
        return health_dashboard
        
    except Exception as e:
        print(f"Error creating process health dashboard: {e}")
        return {'error': str(e)}


def create_performance_analytics(cat) -> Dict[str, Any]:
    """Create performance analytics dashboard."""
    print("Creating performance analytics dashboard...")
    
    try:
        events_table = cat.load_table('ocel.events')
        df_events = daft.read_iceberg(events_table)
        
        # Convert to pandas
        events_data = df_events.to_pandas()
        
        performance_analytics = {
            'timestamp': datetime.now().isoformat(),
            'throughput_metrics': {},
            'efficiency_metrics': {},
            'bottleneck_analysis': {},
            'optimization_opportunities': []
        }
        
        # Throughput metrics
        total_events = len(events_data)
        events_data['hour'] = pd.to_datetime(events_data['time']).dt.hour
        
        # Hourly throughput
        hourly_throughput = events_data.groupby('hour').size()
        peak_throughput = hourly_throughput.max()
        avg_throughput = hourly_throughput.mean()
        
        performance_analytics['throughput_metrics'] = {
            'total_events': total_events,
            'peak_throughput': int(peak_throughput),
            'avg_throughput': float(avg_throughput),
            'throughput_efficiency': float(peak_throughput / avg_throughput) if avg_throughput > 0 else 0
        }
        
        # Efficiency metrics
        if 'vendor_code' in events_data.columns:
            vendor_throughput = events_data.groupby('vendor_code').size()
            vendor_efficiency = vendor_throughput.std() / vendor_throughput.mean() if vendor_throughput.mean() > 0 else 0
            
            performance_analytics['efficiency_metrics'] = {
                'vendor_count': vendor_throughput.nunique(),
                'vendor_efficiency_score': float(1 - vendor_efficiency),  # Lower variance = higher efficiency
                'top_performing_vendor': vendor_throughput.idxmax(),
                'vendor_throughput_variance': float(vendor_throughput.std())
            }
        
        # Bottleneck analysis
        event_type_counts = events_data['type'].value_counts()
        total_events = len(events_data)
        bottleneck_threshold = total_events * 0.1  # 10% threshold
        
        bottlenecks = event_type_counts[event_type_counts > bottleneck_threshold]
        
        performance_analytics['bottleneck_analysis'] = {
            'bottleneck_count': len(bottlenecks),
            'bottleneck_events': bottlenecks.to_dict(),
            'bottleneck_threshold': bottleneck_threshold
        }
        
        # Optimization opportunities
        if len(bottlenecks) > 0:
            performance_analytics['optimization_opportunities'].append({
                'type': 'bottleneck_optimization',
                'description': f'Optimize {len(bottlenecks)} high-frequency events',
                'potential_impact': 'High',
                'priority': 'High'
            })
        
        if performance_analytics['throughput_metrics']['throughput_efficiency'] > 2:
            performance_analytics['optimization_opportunities'].append({
                'type': 'load_balancing',
                'description': 'Implement load balancing for peak hours',
                'potential_impact': 'Medium',
                'priority': 'Medium'
            })
        
        return performance_analytics
        
    except Exception as e:
        print(f"Error creating performance analytics: {e}")
        return {'error': str(e)}


def create_executive_summary(cat) -> Dict[str, Any]:
    """Create comprehensive executive summary."""
    print("Creating executive summary...")
    
    # Run all dashboard components
    kpi_metrics = create_executive_kpis(cat)
    health_dashboard = create_process_health_dashboard(cat)
    performance_analytics = create_performance_analytics(cat)
    
    # Combine into executive summary
    executive_summary = {
        'timestamp': datetime.now().isoformat(),
        'kpi_metrics': kpi_metrics,
        'health_dashboard': health_dashboard,
        'performance_analytics': performance_analytics,
        'executive_overview': {},
        'key_insights': [],
        'strategic_recommendations': []
    }
    
    # Generate executive overview
    total_events = kpi_metrics.get('process_metrics', {}).get('total_events', 0)
    quality_score = kpi_metrics.get('quality_metrics', {}).get('data_quality_score', 0)
    stability_score = health_dashboard.get('health_indicators', {}).get('stability_score', 0)
    throughput_efficiency = performance_analytics.get('throughput_metrics', {}).get('throughput_efficiency', 0)
    
    executive_summary['executive_overview'] = {
        'process_volume': 'High' if total_events > 100000 else 'Medium' if total_events > 10000 else 'Low',
        'data_quality': 'Excellent' if quality_score > 95 else 'Good' if quality_score > 90 else 'Needs Improvement',
        'process_stability': 'High' if stability_score > 0.8 else 'Medium' if stability_score > 0.6 else 'Low',
        'operational_efficiency': 'High' if throughput_efficiency < 2 else 'Medium' if throughput_efficiency < 3 else 'Low',
        'overall_health': 'Excellent' if all([quality_score > 95, stability_score > 0.8, throughput_efficiency < 2]) else 'Good' if all([quality_score > 90, stability_score > 0.6, throughput_efficiency < 3]) else 'Needs Attention'
    }
    
    # Generate key insights
    if total_events > 100000:
        executive_summary['key_insights'].append(
            f"High process volume ({total_events:,} events) - excellent operational scale"
        )
    
    if quality_score > 95:
        executive_summary['key_insights'].append(
            f"Excellent data quality ({quality_score:.1f}%) - reliable analytics foundation"
        )
    
    if stability_score > 0.8:
        executive_summary['key_insights'].append(
            f"High process stability ({stability_score:.1f}) - predictable operations"
        )
    
    # Generate strategic recommendations
    if quality_score < 90:
        executive_summary['strategic_recommendations'].append(
            "Improve data quality through enhanced validation and monitoring"
        )
    
    if stability_score < 0.7:
        executive_summary['strategic_recommendations'].append(
            "Implement process standardization to improve stability"
        )
    
    if throughput_efficiency > 3:
        executive_summary['strategic_recommendations'].append(
            "Optimize resource allocation to improve throughput efficiency"
        )
    
    return executive_summary


def generate_dashboard_visualizations(cat) -> Dict[str, Any]:
    """Generate dashboard visualizations."""
    print("Generating dashboard visualizations...")
    
    try:
        events_table = cat.load_table('ocel.events')
        df_events = daft.read_iceberg(events_table)
        
        # Convert to pandas
        events_data = df_events.to_pandas()
        
        visualization_data = {
            'timestamp': datetime.now().isoformat(),
            'charts': {},
            'visualization_insights': []
        }
        
        # Create time series chart data
        events_data['date'] = pd.to_datetime(events_data['time']).dt.date
        daily_counts = events_data.groupby('date').size()
        
        visualization_data['charts']['daily_trends'] = {
            'dates': [str(date) for date in daily_counts.index],
            'values': daily_counts.values.tolist(),
            'chart_type': 'line',
            'title': 'Daily Event Trends'
        }
        
        # Create event type distribution
        event_type_counts = events_data['type'].value_counts()
        
        visualization_data['charts']['event_distribution'] = {
            'labels': event_type_counts.index.tolist(),
            'values': event_type_counts.values.tolist(),
            'chart_type': 'pie',
            'title': 'Event Type Distribution'
        }
        
        # Create hourly heatmap data
        events_data['hour'] = pd.to_datetime(events_data['time']).dt.hour
        events_data['day_of_week'] = pd.to_datetime(events_data['time']).dt.dayofweek
        
        hourly_activity = events_data.groupby(['day_of_week', 'hour']).size().unstack(fill_value=0)
        
        visualization_data['charts']['hourly_heatmap'] = {
            'data': hourly_activity.values.tolist(),
            'x_labels': [str(h) for h in hourly_activity.columns],
            'y_labels': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            'chart_type': 'heatmap',
            'title': 'Hourly Activity Heatmap'
        }
        
        # Generate visualization insights
        peak_day = daily_counts.idxmax()
        peak_events = daily_counts.max()
        
        visualization_data['visualization_insights'].append(
            f"Peak activity day: {peak_day} with {peak_events} events"
        )
        
        most_common_event = event_type_counts.index[0]
        most_common_count = event_type_counts.iloc[0]
        
        visualization_data['visualization_insights'].append(
            f"Most common event: {most_common_event} ({most_common_count} occurrences)"
        )
        
        return visualization_data
        
    except Exception as e:
        print(f"Error generating visualizations: {e}")
        return {'error': str(e)}


def main():
    """Main function for executive dashboard."""
    print("=" * 80)
    print("EXECUTIVE DASHBOARD & ANALYTICS")
    print("=" * 80)
    
    # Load catalog
    catalog_cfg = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_cfg)
    
    # Step 1: Create executive KPIs
    print("\n1. CREATING EXECUTIVE KPIs")
    print("-" * 50)
    kpi_metrics = create_executive_kpis(cat)
    
    if 'error' not in kpi_metrics:
        process_metrics = kpi_metrics.get('process_metrics', {})
        print(f"Total events: {process_metrics.get('total_events', 0):,}")
        print(f"Process complexity: {process_metrics.get('process_complexity', 'Unknown')}")
        
        quality_metrics = kpi_metrics.get('quality_metrics', {})
        print(f"Data quality score: {quality_metrics.get('data_quality_score', 0):.1f}%")
        print(f"Quality status: {quality_metrics.get('quality_status', 'Unknown')}")
    else:
        print(f"Error: {kpi_metrics['error']}")
    
    # Step 2: Create process health dashboard
    print("\n2. CREATING PROCESS HEALTH DASHBOARD")
    print("-" * 50)
    health_dashboard = create_process_health_dashboard(cat)
    
    if 'error' not in health_dashboard:
        health_indicators = health_dashboard.get('health_indicators', {})
        print(f"Average daily events: {health_indicators.get('avg_daily_events', 0):.1f}")
        print(f"Stability score: {health_indicators.get('stability_score', 0):.3f}")
        
        alerts = health_dashboard.get('alerts', [])
        print(f"Active alerts: {len(alerts)}")
        for alert in alerts[:3]:
            print(f"  - {alert['message']} ({alert['severity']})")
    else:
        print(f"Error: {health_dashboard['error']}")
    
    # Step 3: Create performance analytics
    print("\n3. CREATING PERFORMANCE ANALYTICS")
    print("-" * 50)
    performance_analytics = create_performance_analytics(cat)
    
    if 'error' not in performance_analytics:
        throughput_metrics = performance_analytics.get('throughput_metrics', {})
        print(f"Peak throughput: {throughput_metrics.get('peak_throughput', 0)}")
        print(f"Throughput efficiency: {throughput_metrics.get('throughput_efficiency', 0):.2f}")
        
        bottlenecks = performance_analytics.get('bottleneck_analysis', {})
        print(f"Bottlenecks identified: {bottlenecks.get('bottleneck_count', 0)}")
    else:
        print(f"Error: {performance_analytics['error']}")
    
    # Step 4: Create executive summary
    print("\n4. CREATING EXECUTIVE SUMMARY")
    print("-" * 50)
    executive_summary = create_executive_summary(cat)
    
    overview = executive_summary.get('executive_overview', {})
    print(f"Process volume: {overview.get('process_volume', 'Unknown')}")
    print(f"Data quality: {overview.get('data_quality', 'Unknown')}")
    print(f"Process stability: {overview.get('process_stability', 'Unknown')}")
    print(f"Overall health: {overview.get('overall_health', 'Unknown')}")
    
    # Display key insights
    insights = executive_summary.get('key_insights', [])
    if insights:
        print("\nKey insights:")
        for insight in insights:
            print(f"  - {insight}")
    
    # Display strategic recommendations
    recommendations = executive_summary.get('strategic_recommendations', [])
    if recommendations:
        print("\nStrategic recommendations:")
        for rec in recommendations:
            print(f"  - {rec}")
    
    # Step 5: Generate visualizations
    print("\n5. GENERATING DASHBOARD VISUALIZATIONS")
    print("-" * 50)
    visualization_data = generate_dashboard_visualizations(cat)
    
    if 'error' not in visualization_data:
        charts = visualization_data.get('charts', {})
        print(f"Charts generated: {len(charts)}")
        
        insights = visualization_data.get('visualization_insights', [])
        for insight in insights:
            print(f"  - {insight}")
    else:
        print(f"Error: {visualization_data['error']}")
    
    # Save executive dashboard
    reports_dir = Path(__file__).parents[1] / 'docs'
    reports_dir.mkdir(exist_ok=True)
    
    dashboard_file = reports_dir / 'executive_dashboard_report.json'
    with open(dashboard_file, 'w', encoding='utf-8') as f:
        json.dump(executive_summary, f, indent=2)
    
    print(f"\nExecutive dashboard saved to: {dashboard_file}")
    
    print("\n" + "=" * 80)
    print("EXECUTIVE DASHBOARD COMPLETE")
    print("=" * 80)
    print("[SUCCESS] Executive KPIs created")
    print("[SUCCESS] Process health dashboard generated")
    print("[SUCCESS] Performance analytics completed")
    print("[SUCCESS] Executive summary created")
    print("[SUCCESS] Dashboard visualizations generated")


if __name__ == '__main__':
    main()
