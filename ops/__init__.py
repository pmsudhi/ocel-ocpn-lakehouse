"""
Operations and maintenance utilities.

This module provides tools for system maintenance, performance optimization,
schema evolution, and materialized view management.
"""

from .maintenance_system import (
    check_system_health,
    run_maintenance,
    optimize_tables
)

from .performance_optimization import (
    optimize_query_performance,
    benchmark_queries
)

from .schema_evolution import (
    evolve_schema,
    migrate_table
)

from .materialized_views import (
    create_materialized_view,
    refresh_materialized_views
)

__all__ = [
    "check_system_health",
    "run_maintenance",
    "optimize_tables",
    "optimize_query_performance",
    "benchmark_queries",
    "evolve_schema",
    "migrate_table",
    "create_materialized_view",
    "refresh_materialized_views",
]

