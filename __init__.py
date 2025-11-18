"""
OCEL/OCPN Lakehouse - A production-ready data lakehouse system for process mining.

This package provides comprehensive support for Object-Centric Event Logs (OCEL) 
and Object-Centric Petri Nets (OCPN) with advanced analytics capabilities built 
on Apache Iceberg.

Main Components:
    - agent: Process mining query engine and natural language interface
    - analytics: Process discovery, conformance checking, and analytics
    - ingest: Data loading and table bootstrap utilities
    - ops: Operations and maintenance tools
    - queries: Query templates and validation utilities
"""

__version__ = "1.0.0"
__author__ = "Process Mining Team"
__license__ = "MIT"

# Core imports
from .agent import (
    ProcessMiningQueryEngine,
    ProcessMiningAgent,
    QueryOptimizer
)

from .analytics import (
    PM4PyAnalyticsWrapper,
    ConformanceChecker,
    discover_process_variants,
    analyze_process_performance,
    discover_object_lifecycles,
    analyze_process_networks,
    generate_process_insights_report,
    analyze_process_costs,
    calculate_roi_metrics,
    analyze_resource_optimization,
    generate_cost_optimization_report
)

from .ingest.production_bootstrap import (
    bootstrap_lakehouse,
    load_catalog_from_yaml
)

__all__ = [
    # Version
    "__version__",
    
    # Agent
    "ProcessMiningQueryEngine",
    "ProcessMiningAgent",
    "QueryOptimizer",
    
    # Analytics
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
    
    # Ingest
    "bootstrap_lakehouse",
    "load_catalog_from_yaml",
]

