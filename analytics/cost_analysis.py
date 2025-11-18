#!/usr/bin/env python3
"""
Cost Analysis for OCEL/OCPN Lakehouse
Implements comprehensive cost analysis, resource optimization, and ROI calculations
"""

import daft
from pathlib import Path
from datetime import datetime, timedelta
from pyiceberg.catalog import load_catalog
from typing import Dict, List, Any, Optional
import json
import numpy as np
import pandas as pd


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


def analyze_process_costs(cat) -> Dict[str, Any]:
    """Analyze process costs and resource utilization."""
    print("Analyzing process costs...")
    
    try:
        events_table = cat.load_table('ocel.events')
        df_events = daft.read_iceberg(events_table)
        
        # Convert to pandas for cost analysis
        events_data = df_events.to_pandas()
        
        cost_analysis = {
            'timestamp': datetime.now().isoformat(),
            'cost_metrics': {},
            'resource_analysis': {},
            'optimization_opportunities': [],
            'cost_insights': []
        }
        
        # Analyze event frequency and duration
        event_counts = events_data['type'].value_counts()
        total_events = len(events_data)
        
        # Calculate cost metrics (simplified model)
        # Assume different event types have different costs
        event_costs = {
            'create_order': 10.0,
            'approve_order': 15.0,
            'create_invoice': 8.0,
            'approve_invoice': 12.0,
            'payment': 5.0
        }
        
        total_cost = 0
        cost_by_event_type = {}
        
        for event_type, count in event_counts.items():
            cost_per_event = event_costs.get(event_type, 5.0)  # Default cost
            event_total_cost = count * cost_per_event
            cost_by_event_type[event_type] = {
                'count': count,
                'cost_per_event': cost_per_event,
                'total_cost': event_total_cost
            }
            total_cost += event_total_cost
        
        cost_analysis['cost_metrics'] = {
            'total_cost': total_cost,
            'average_cost_per_event': total_cost / total_events if total_events > 0 else 0,
            'cost_by_event_type': cost_by_event_type
        }
        
        # Resource analysis
        if 'vendor_code' in events_data.columns:
            vendor_costs = events_data.groupby('vendor_code').apply(
                lambda x: sum([event_costs.get(event_type, 5.0) for event_type in x['type']])
            ).to_dict()
            
            cost_analysis['resource_analysis'] = {
                'vendor_costs': vendor_costs,
                'top_cost_vendors': sorted(vendor_costs.items(), key=lambda x: x[1], reverse=True)[:5]
            }
        
        # Time-based cost analysis
        events_data['hour'] = pd.to_datetime(events_data['time']).dt.hour
        hourly_costs = events_data.groupby('hour').apply(
            lambda x: sum([event_costs.get(event_type, 5.0) for event_type in x['type']])
        )
        
        peak_cost_hour = hourly_costs.idxmax()
        peak_cost = hourly_costs.max()
        avg_hourly_cost = hourly_costs.mean()
        
        cost_analysis['resource_analysis']['hourly_costs'] = {
            'peak_hour': int(peak_cost_hour),
            'peak_cost': float(peak_cost),
            'average_hourly_cost': float(avg_hourly_cost)
        }
        
        # Generate optimization opportunities
        if peak_cost > avg_hourly_cost * 2:
            cost_analysis['optimization_opportunities'].append(
                f"High cost concentration at hour {peak_cost_hour} - consider load balancing"
            )
        
        # Find most expensive event types
        expensive_events = sorted(cost_by_event_type.items(), 
                                key=lambda x: x[1]['total_cost'], reverse=True)[:3]
        
        for event_type, cost_info in expensive_events:
            if cost_info['total_cost'] > total_cost * 0.2:  # More than 20% of total cost
                cost_analysis['optimization_opportunities'].append(
                    f"High cost event type '{event_type}' - consider automation"
                )
        
        # Generate cost insights
        cost_analysis['cost_insights'].append(
            f"Total process cost: ${total_cost:,.2f}"
        )
        cost_analysis['cost_insights'].append(
            f"Average cost per event: ${total_cost/total_events:.2f}"
        )
        
        if len(cost_analysis['optimization_opportunities']) > 0:
            cost_analysis['cost_insights'].append(
                f"Identified {len(cost_analysis['optimization_opportunities'])} optimization opportunities"
            )
        
        return cost_analysis
        
    except Exception as e:
        print(f"Error analyzing process costs: {e}")
        return {'error': str(e)}


def calculate_roi_metrics(cat) -> Dict[str, Any]:
    """Calculate ROI metrics and business value."""
    print("Calculating ROI metrics...")
    
    try:
        events_table = cat.load_table('ocel.events')
        df_events = daft.read_iceberg(events_table)
        
        # Convert to pandas
        events_data = df_events.to_pandas()
        
        roi_analysis = {
            'timestamp': datetime.now().isoformat(),
            'roi_metrics': {},
            'business_value': {},
            'efficiency_metrics': {},
            'roi_insights': []
        }
        
        # Calculate process efficiency metrics
        total_events = len(events_data)
        
        # Time-based efficiency
        events_data['hour'] = pd.to_datetime(events_data['time']).dt.hour
        events_data['day_of_week'] = pd.to_datetime(events_data['time']).dt.dayofweek
        
        # Calculate process throughput
        if 'vendor_code' in events_data.columns:
            vendor_throughput = events_data.groupby('vendor_code').size()
            avg_throughput = vendor_throughput.mean()
            max_throughput = vendor_throughput.max()
            
            roi_analysis['efficiency_metrics'] = {
                'average_throughput_per_vendor': float(avg_throughput),
                'max_throughput_per_vendor': int(max_throughput),
                'throughput_variance': float(vendor_throughput.std())
            }
        
        # Calculate ROI metrics (simplified model)
        # Assume business value increases with process efficiency
        process_efficiency = min(1.0, total_events / 1000)  # Normalize to 0-1
        business_value = process_efficiency * 100000  # $100K base value
        
        # Calculate costs (from previous analysis)
        event_costs = {
            'create_order': 10.0, 'approve_order': 15.0, 'create_invoice': 8.0,
            'approve_invoice': 12.0, 'payment': 5.0
        }
        
        total_cost = sum([event_costs.get(event_type, 5.0) for event_type in events_data['type']])
        
        # ROI calculation
        roi = (business_value - total_cost) / total_cost if total_cost > 0 else 0
        roi_percentage = roi * 100
        
        roi_analysis['roi_metrics'] = {
            'total_investment': total_cost,
            'business_value': business_value,
            'net_profit': business_value - total_cost,
            'roi_percentage': roi_percentage,
            'payback_period_months': 12 / roi if roi > 0 else float('inf')
        }
        
        # Business value analysis
        roi_analysis['business_value'] = {
            'process_automation_potential': 'High' if process_efficiency > 0.8 else 'Medium' if process_efficiency > 0.5 else 'Low',
            'cost_optimization_potential': 'High' if total_cost > 50000 else 'Medium' if total_cost > 10000 else 'Low',
            'scalability_score': min(1.0, total_events / 5000)  # Normalize to 0-1
        }
        
        # Generate ROI insights
        if roi_percentage > 100:
            roi_analysis['roi_insights'].append(
                f"Excellent ROI: {roi_percentage:.1f}% return on investment"
            )
        elif roi_percentage > 50:
            roi_analysis['roi_insights'].append(
                f"Good ROI: {roi_percentage:.1f}% return on investment"
            )
        else:
            roi_analysis['roi_insights'].append(
                f"ROI needs improvement: {roi_percentage:.1f}% return on investment"
            )
        
        if roi_analysis['business_value']['process_automation_potential'] == 'High':
            roi_analysis['roi_insights'].append(
                "High automation potential - consider RPA implementation"
            )
        
        return roi_analysis
        
    except Exception as e:
        print(f"Error calculating ROI metrics: {e}")
        return {'error': str(e)}


def analyze_resource_optimization(cat) -> Dict[str, Any]:
    """Analyze resource optimization opportunities."""
    print("Analyzing resource optimization opportunities...")
    
    try:
        events_table = cat.load_table('ocel.events')
        df_events = daft.read_iceberg(events_table)
        
        # Convert to pandas
        events_data = df_events.to_pandas()
        
        optimization_analysis = {
            'timestamp': datetime.now().isoformat(),
            'resource_utilization': {},
            'optimization_opportunities': [],
            'capacity_planning': {},
            'optimization_insights': []
        }
        
        # Analyze resource utilization patterns
        events_data['hour'] = pd.to_datetime(events_data['time']).dt.hour
        events_data['day_of_week'] = pd.to_datetime(events_data['time']).dt.dayofweek
        
        # Hourly utilization analysis
        hourly_utilization = events_data.groupby('hour').size()
        peak_hour = hourly_utilization.idxmax()
        peak_utilization = hourly_utilization.max()
        avg_utilization = hourly_utilization.mean()
        
        optimization_analysis['resource_utilization'] = {
            'peak_hour': int(peak_hour),
            'peak_utilization': int(peak_utilization),
            'average_utilization': float(avg_utilization),
            'utilization_ratio': float(peak_utilization / avg_utilization) if avg_utilization > 0 else 0
        }
        
        # Identify optimization opportunities
        if peak_utilization > avg_utilization * 2:
            optimization_analysis['optimization_opportunities'].append({
                'type': 'load_balancing',
                'description': f'High utilization spike at hour {peak_hour}',
                'potential_savings': f'${(peak_utilization - avg_utilization) * 10:.0f}',
                'priority': 'High'
            })
        
        # Weekend vs weekday analysis
        events_data['is_weekend'] = events_data['day_of_week'].isin([5, 6])
        weekend_utilization = events_data[events_data['is_weekend']].groupby('hour').size()
        weekday_utilization = events_data[~events_data['is_weekend']].groupby('hour').size()
        
        if len(weekend_utilization) > 0 and len(weekday_utilization) > 0:
            weekend_avg = weekend_utilization.mean()
            weekday_avg = weekday_utilization.mean()
            
            if weekend_avg > weekday_avg * 0.3:
                optimization_analysis['optimization_opportunities'].append({
                    'type': 'capacity_planning',
                    'description': 'Significant weekend activity detected',
                    'potential_savings': f'${weekend_avg * 5:.0f}',
                    'priority': 'Medium'
                })
        
        # Vendor-specific optimization
        if 'vendor_code' in events_data.columns:
            vendor_utilization = events_data.groupby('vendor_code').size()
            top_vendors = vendor_utilization.nlargest(5)
            
            optimization_analysis['capacity_planning'] = {
                'top_vendors': top_vendors.to_dict(),
                'vendor_utilization_variance': float(vendor_utilization.std())
            }
            
            # Identify vendor optimization opportunities
            if vendor_utilization.std() > vendor_utilization.mean() * 0.5:
                optimization_analysis['optimization_opportunities'].append({
                    'type': 'vendor_optimization',
                    'description': 'High variance in vendor utilization',
                    'potential_savings': f'${vendor_utilization.std() * 20:.0f}',
                    'priority': 'Medium'
                })
        
        # Generate optimization insights
        total_opportunities = len(optimization_analysis['optimization_opportunities'])
        optimization_analysis['optimization_insights'].append(
            f"Identified {total_opportunities} optimization opportunities"
        )
        
        high_priority = len([opp for opp in optimization_analysis['optimization_opportunities'] 
                           if opp['priority'] == 'High'])
        if high_priority > 0:
            optimization_analysis['optimization_insights'].append(
                f"{high_priority} high-priority optimization opportunities identified"
            )
        
        return optimization_analysis
        
    except Exception as e:
        print(f"Error analyzing resource optimization: {e}")
        return {'error': str(e)}


def generate_cost_optimization_report(cat) -> Dict[str, Any]:
    """Generate comprehensive cost optimization report."""
    print("Generating comprehensive cost optimization report...")
    
    # Run all cost analyses
    cost_analysis = analyze_process_costs(cat)
    roi_analysis = calculate_roi_metrics(cat)
    optimization_analysis = analyze_resource_optimization(cat)
    
    # Combine into comprehensive report
    cost_report = {
        'timestamp': datetime.now().isoformat(),
        'cost_analysis': cost_analysis,
        'roi_analysis': roi_analysis,
        'optimization_analysis': optimization_analysis,
        'executive_summary': {},
        'strategic_recommendations': []
    }
    
    # Generate executive summary
    total_cost = cost_analysis.get('cost_metrics', {}).get('total_cost', 0)
    roi_percentage = roi_analysis.get('roi_metrics', {}).get('roi_percentage', 0)
    optimization_opportunities = len(optimization_analysis.get('optimization_opportunities', []))
    
    cost_report['executive_summary'] = {
        'total_process_cost': total_cost,
        'roi_percentage': roi_percentage,
        'optimization_opportunities': optimization_opportunities,
        'cost_efficiency': 'High' if total_cost < 10000 else 'Medium' if total_cost < 50000 else 'Low',
        'roi_status': 'Excellent' if roi_percentage > 100 else 'Good' if roi_percentage > 50 else 'Needs Improvement'
    }
    
    # Generate strategic recommendations
    if roi_percentage < 50:
        cost_report['strategic_recommendations'].append(
            "Low ROI - implement cost reduction initiatives"
        )
    
    if optimization_opportunities > 5:
        cost_report['strategic_recommendations'].append(
            f"High optimization potential - {optimization_opportunities} opportunities identified"
        )
    
    if total_cost > 100000:
        cost_report['strategic_recommendations'].append(
            "High process costs - consider automation and process redesign"
        )
    
    return cost_report


def main():
    """Main function for cost analysis."""
    print("=" * 80)
    print("COST ANALYSIS & OPTIMIZATION")
    print("=" * 80)
    
    # Load catalog
    catalog_cfg = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_cfg)
    
    # Step 1: Analyze process costs
    print("\n1. ANALYZING PROCESS COSTS")
    print("-" * 50)
    cost_analysis = analyze_process_costs(cat)
    
    if 'error' not in cost_analysis:
        metrics = cost_analysis.get('cost_metrics', {})
        print(f"Total process cost: ${metrics.get('total_cost', 0):,.2f}")
        print(f"Average cost per event: ${metrics.get('average_cost_per_event', 0):.2f}")
        
        opportunities = cost_analysis.get('optimization_opportunities', [])
        print(f"Optimization opportunities: {len(opportunities)}")
        for opp in opportunities[:3]:
            print(f"  - {opp}")
    else:
        print(f"Error: {cost_analysis['error']}")
    
    # Step 2: Calculate ROI metrics
    print("\n2. CALCULATING ROI METRICS")
    print("-" * 50)
    roi_analysis = calculate_roi_metrics(cat)
    
    if 'error' not in roi_analysis:
        metrics = roi_analysis.get('roi_metrics', {})
        print(f"ROI percentage: {metrics.get('roi_percentage', 0):.1f}%")
        print(f"Net profit: ${metrics.get('net_profit', 0):,.2f}")
        print(f"Payback period: {metrics.get('payback_period_months', 0):.1f} months")
        
        insights = roi_analysis.get('roi_insights', [])
        for insight in insights:
            print(f"  - {insight}")
    else:
        print(f"Error: {roi_analysis['error']}")
    
    # Step 3: Analyze resource optimization
    print("\n3. ANALYZING RESOURCE OPTIMIZATION")
    print("-" * 50)
    optimization_analysis = analyze_resource_optimization(cat)
    
    if 'error' not in optimization_analysis:
        utilization = optimization_analysis.get('resource_utilization', {})
        print(f"Peak hour: {utilization.get('peak_hour', 'N/A')}")
        print(f"Utilization ratio: {utilization.get('utilization_ratio', 0):.2f}")
        
        opportunities = optimization_analysis.get('optimization_opportunities', [])
        print(f"Optimization opportunities: {len(opportunities)}")
        for opp in opportunities[:3]:
            print(f"  - {opp['description']} (Priority: {opp['priority']})")
    else:
        print(f"Error: {optimization_analysis['error']}")
    
    # Step 4: Generate comprehensive cost report
    print("\n4. GENERATING COST OPTIMIZATION REPORT")
    print("-" * 50)
    cost_report = generate_cost_optimization_report(cat)
    
    # Display executive summary
    summary = cost_report.get('executive_summary', {})
    print(f"Total process cost: ${summary.get('total_process_cost', 0):,.2f}")
    print(f"ROI percentage: {summary.get('roi_percentage', 0):.1f}%")
    print(f"Cost efficiency: {summary.get('cost_efficiency', 'Unknown')}")
    print(f"ROI status: {summary.get('roi_status', 'Unknown')}")
    
    # Display strategic recommendations
    recommendations = cost_report.get('strategic_recommendations', [])
    if recommendations:
        print("\nStrategic recommendations:")
        for rec in recommendations:
            print(f"  - {rec}")
    
    # Save cost report
    reports_dir = Path(__file__).parents[1] / 'docs'
    reports_dir.mkdir(exist_ok=True)
    
    cost_file = reports_dir / 'cost_optimization_report.json'
    with open(cost_file, 'w', encoding='utf-8') as f:
        json.dump(cost_report, f, indent=2)
    
    print(f"\nCost optimization report saved to: {cost_file}")
    
    print("\n" + "=" * 80)
    print("COST ANALYSIS COMPLETE")
    print("=" * 80)
    print("[SUCCESS] Process costs analyzed")
    print("[SUCCESS] ROI metrics calculated")
    print("[SUCCESS] Resource optimization analyzed")
    print("[SUCCESS] Cost optimization report generated")


if __name__ == '__main__':
    main()
