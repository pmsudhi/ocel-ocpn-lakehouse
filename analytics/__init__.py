"""
Analytics modules for process mining.

This module provides process discovery, conformance checking, cost analysis,
and pm4py integration for OCEL/OCPN data stored in Iceberg.
"""

from .pm4py_analytics_wrapper import PM4PyAnalyticsWrapper
from .conformance_checking import ConformanceChecker

# Import functions from process_discovery and cost_analysis
from .process_discovery import (
    discover_process_variants,
    analyze_process_performance,
    discover_object_lifecycles,
    analyze_process_networks,
    generate_process_insights_report
)

from .cost_analysis import (
    analyze_process_costs,
    calculate_roi_metrics,
    analyze_resource_optimization,
    generate_cost_optimization_report
)

__all__ = [
    "PM4PyAnalyticsWrapper",
    "ConformanceChecker",
    "discover_process_variants",
    "analyze_process_performance",
    "discover_object_lifecycles",
    "analyze_process_networks",
    "generate_process_insights_report",
    "analyze_process_costs",
    "calculate_roi_metrics",
    "analyze_resource_optimization",
    "generate_cost_optimization_report",
]

