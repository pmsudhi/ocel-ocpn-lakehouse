#!/usr/bin/env python3
"""
Query Optimizer for Process Mining
Intelligent query routing, caching, and performance optimization
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import json
import time
import hashlib

from pyiceberg.catalog import load_catalog

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


class QueryOptimizer:
    """Intelligent query routing and caching for process mining queries."""
    
    def __init__(self, catalog, config: Optional[Dict[str, Any]] = None):
        self.catalog = catalog
        self.config = config or self._get_default_config()
        self.query_cache = {}
        self.query_stats = {
            'total_queries': 0,
            'cache_hits': 0,
            'materialized_view_hits': 0,
            'slow_queries': 0,
            'avg_response_time': 0.0,
            'query_patterns': {}
        }
        self.performance_metrics = []
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default optimization configuration."""
        return {
            'cache_enabled': True,
            'cache_ttl_seconds': 3600,
            'max_cache_size_mb': 1024,
            'materialized_views_enabled': True,
            'partition_pruning_enabled': True,
            'slow_query_threshold_seconds': 10,
            'auto_optimize': True
        }
    
    def optimize_query_plan(self, query_type: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize query execution plan.
        
        Args:
            query_type: Type of query (e.g., 'process_variants', 'bottlenecks')
            parameters: Query parameters
            
        Returns:
            Optimized query plan with execution strategy
        """
        start_time = time.time()
        
        try:
            # Generate cache key
            cache_key = self._generate_cache_key(query_type, parameters)
            
            # Check cache first
            if self.config['cache_enabled'] and cache_key in self.query_cache:
                cache_entry = self.query_cache[cache_key]
                if not self._is_cache_expired(cache_entry):
                    self.query_stats['cache_hits'] += 1
                    return {
                        'execution_strategy': 'cache_hit',
                        'cache_key': cache_key,
                        'estimated_time_ms': 1,
                        'optimization_notes': ['Using cached result']
                    }
            
            # Determine execution strategy
            strategy = self._determine_execution_strategy(query_type, parameters)
            
            # Check if materialized views are available
            if self.config['materialized_views_enabled']:
                materialized_available = self._check_materialized_views(query_type)
                if materialized_available:
                    strategy['use_materialized_views'] = True
                    strategy['estimated_time_ms'] = strategy.get('estimated_time_ms', 1000) * 0.1  # 10x faster
                    self.query_stats['materialized_view_hits'] += 1
            
            # Apply partition pruning if applicable
            if self.config['partition_pruning_enabled']:
                partition_strategy = self._get_partition_strategy(query_type, parameters)
                if partition_strategy:
                    strategy['partition_pruning'] = partition_strategy
                    strategy['estimated_time_ms'] = strategy.get('estimated_time_ms', 1000) * 0.5  # 2x faster
            
            # Estimate query cost
            estimated_time = strategy.get('estimated_time_ms', 1000)
            if estimated_time > self.config['slow_query_threshold_seconds'] * 1000:
                self.query_stats['slow_queries'] += 1
                strategy['optimization_notes'].append('Slow query detected - consider optimization')
            
            # Add optimization recommendations
            recommendations = self._get_optimization_recommendations(query_type, parameters, strategy)
            strategy['recommendations'] = recommendations
            
            # Record query pattern
            self._record_query_pattern(query_type, parameters, estimated_time)
            
            optimization_time = (time.time() - start_time) * 1000
            strategy['optimization_time_ms'] = optimization_time
            
            return strategy
            
        except Exception as e:
            return {
                'execution_strategy': 'error',
                'error': f'Query optimization failed: {e}',
                'estimated_time_ms': 0
            }
    
    def _generate_cache_key(self, query_type: str, parameters: Dict[str, Any]) -> str:
        """Generate cache key for query."""
        # Create deterministic key from query type and parameters
        key_data = {
            'query_type': query_type,
            'parameters': sorted(parameters.items()) if parameters else []
        }
        key_string = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _is_cache_expired(self, cache_entry: Dict[str, Any]) -> bool:
        """Check if cache entry is expired."""
        if 'timestamp' not in cache_entry:
            return True
        
        cache_time = datetime.fromisoformat(cache_entry['timestamp'])
        ttl_seconds = self.config.get('cache_ttl_seconds', 3600)
        
        return datetime.utcnow() - cache_time > timedelta(seconds=ttl_seconds)
    
    def _determine_execution_strategy(self, query_type: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Determine the best execution strategy for a query."""
        strategy = {
            'execution_strategy': 'direct',
            'estimated_time_ms': 1000,
            'optimization_notes': []
        }
        
        # Query-specific optimizations
        if query_type == 'process_variants':
            strategy['estimated_time_ms'] = 2000
            strategy['optimization_notes'].append('Complex aggregation query')
            
        elif query_type == 'similar_cases':
            strategy['estimated_time_ms'] = 5000
            strategy['optimization_notes'].append('Sequence comparison query')
            
        elif query_type == 'bottlenecks':
            strategy['estimated_time_ms'] = 1500
            strategy['optimization_notes'].append('Performance analysis query')
            
        elif query_type == 'conformance':
            strategy['estimated_time_ms'] = 3000
            strategy['optimization_notes'].append('Model comparison query')
            
        elif query_type == 'predictions':
            strategy['estimated_time_ms'] = 2500
            strategy['optimization_notes'].append('ML prediction query')
        
        # Parameter-based optimizations
        if 'top_n' in parameters and parameters['top_n'] <= 10:
            strategy['estimated_time_ms'] *= 0.5
            strategy['optimization_notes'].append('Limited result set')
        
        if 'instance_id' in parameters:
            strategy['estimated_time_ms'] *= 0.3
            strategy['optimization_notes'].append('Single instance query')
        
        return strategy
    
    def _check_materialized_views(self, query_type: str) -> bool:
        """Check if materialized views are available for query type."""
        materialized_views = {
            'process_variants': 'ocel.process_variants',
            'activity_frequencies': 'ocel.activity_metrics',
            'bottlenecks': 'ocel.activity_metrics',
            'object_interactions': 'ocel.object_interaction_graph',
            'dfg_queries': 'ocel.dfg_matrix'
        }
        
        view_name = materialized_views.get(query_type)
        if not view_name:
            return False
        
        try:
            return self.catalog.table_exists(view_name)
        except Exception:
            return False
    
    def _get_partition_strategy(self, query_type: str, parameters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get partition pruning strategy for query."""
        partition_strategy = {
            'enabled': False,
            'partitions_to_scan': [],
            'estimated_reduction': 0.0
        }
        
        # Date-based partition pruning
        if 'start_date' in parameters or 'end_date' in parameters:
            partition_strategy['enabled'] = True
            partition_strategy['partitions_to_scan'] = self._get_date_partitions(
                parameters.get('start_date'),
                parameters.get('end_date')
            )
            partition_strategy['estimated_reduction'] = 0.7  # 70% reduction
        
        # Instance type partition pruning
        elif 'instance_type' in parameters:
            partition_strategy['enabled'] = True
            partition_strategy['partitions_to_scan'] = [parameters['instance_type']]
            partition_strategy['estimated_reduction'] = 0.5  # 50% reduction
        
        return partition_strategy if partition_strategy['enabled'] else None
    
    def _get_date_partitions(self, start_date: Optional[str], end_date: Optional[str]) -> List[str]:
        """Get date partitions to scan based on date range."""
        # Simplified implementation - in production would calculate actual partitions
        if start_date and end_date:
            return [f"date_range_{start_date}_{end_date}"]
        elif start_date:
            return [f"date_from_{start_date}"]
        elif end_date:
            return [f"date_to_{end_date}"]
        else:
            return []
    
    def _get_optimization_recommendations(self, query_type: str, parameters: Dict[str, Any], 
                                        strategy: Dict[str, Any]) -> List[str]:
        """Get optimization recommendations for query."""
        recommendations = []
        
        # Check if materialized views would help
        if not strategy.get('use_materialized_views', False):
            if query_type in ['process_variants', 'activity_frequencies', 'bottlenecks']:
                recommendations.append("Consider creating materialized views for better performance")
        
        # Check for large result sets
        if 'top_n' not in parameters or parameters.get('top_n', 100) > 100:
            recommendations.append("Consider limiting result set size with top_n parameter")
        
        # Check for complex queries
        if strategy.get('estimated_time_ms', 0) > 5000:
            recommendations.append("Consider breaking down complex query into smaller parts")
        
        # Check for missing filters
        if query_type == 'process_variants' and 'instance_type' not in parameters:
            recommendations.append("Consider filtering by instance_type for better performance")
        
        return recommendations
    
    def _record_query_pattern(self, query_type: str, parameters: Dict[str, Any], estimated_time: float):
        """Record query pattern for analysis."""
        pattern_key = f"{query_type}_{len(parameters)}_params"
        
        if pattern_key not in self.query_stats['query_patterns']:
            self.query_stats['query_patterns'][pattern_key] = {
                'count': 0,
                'total_time': 0.0,
                'avg_time': 0.0,
                'last_seen': None
            }
        
        pattern = self.query_stats['query_patterns'][pattern_key]
        pattern['count'] += 1
        pattern['total_time'] += estimated_time
        pattern['avg_time'] = pattern['total_time'] / pattern['count']
        pattern['last_seen'] = datetime.utcnow().isoformat()
    
    def execute_optimized_query(self, query_type: str, parameters: Dict[str, Any], 
                              query_function) -> Dict[str, Any]:
        """Execute query with optimization."""
        start_time = time.time()
        
        # Get optimization plan
        plan = self.optimize_query_plan(query_type, parameters)
        
        if plan.get('execution_strategy') == 'error':
            return plan
        
        # Execute query
        try:
            result = query_function(**parameters)
            
            # Record performance metrics
            execution_time = (time.time() - start_time) * 1000
            self._record_performance_metrics(query_type, execution_time, plan)
            
            # Cache result if enabled
            if self.config['cache_enabled'] and 'error' not in result:
                cache_key = self._generate_cache_key(query_type, parameters)
                self.query_cache[cache_key] = {
                    'result': result,
                    'timestamp': datetime.utcnow().isoformat(),
                    'query_type': query_type,
                    'parameters': parameters
                }
            
            # Update query stats
            self.query_stats['total_queries'] += 1
            self._update_avg_response_time(execution_time)
            
            return {
                'result': result,
                'execution_time_ms': execution_time,
                'optimization_plan': plan,
                'cache_hit': plan.get('execution_strategy') == 'cache_hit'
            }
            
        except Exception as e:
            return {
                'error': f'Query execution failed: {e}',
                'execution_time_ms': (time.time() - start_time) * 1000,
                'optimization_plan': plan
            }
    
    def _record_performance_metrics(self, query_type: str, execution_time: float, plan: Dict[str, Any]):
        """Record performance metrics for analysis."""
        metrics = {
            'query_type': query_type,
            'execution_time_ms': execution_time,
            'timestamp': datetime.utcnow().isoformat(),
            'used_materialized_views': plan.get('use_materialized_views', False),
            'used_partition_pruning': bool(plan.get('partition_pruning')),
            'optimization_notes': plan.get('optimization_notes', [])
        }
        
        self.performance_metrics.append(metrics)
        
        # Keep only recent metrics (last 1000)
        if len(self.performance_metrics) > 1000:
            self.performance_metrics = self.performance_metrics[-1000:]
    
    def _update_avg_response_time(self, execution_time: float):
        """Update average response time."""
        total_queries = self.query_stats['total_queries']
        current_avg = self.query_stats['avg_response_time']
        
        # Calculate new average
        new_avg = ((current_avg * (total_queries - 1)) + execution_time) / total_queries
        self.query_stats['avg_response_time'] = new_avg
    
    def get_query_execution_stats(self) -> Dict[str, Any]:
        """Get query execution statistics."""
        total_queries = self.query_stats['total_queries']
        cache_hit_rate = (self.query_stats['cache_hits'] / max(total_queries, 1)) * 100
        materialized_view_rate = (self.query_stats['materialized_view_hits'] / max(total_queries, 1)) * 100
        slow_query_rate = (self.query_stats['slow_queries'] / max(total_queries, 1)) * 100
        
        return {
            'total_queries': total_queries,
            'cache_hit_rate': cache_hit_rate,
            'materialized_view_hit_rate': materialized_view_rate,
            'slow_query_rate': slow_query_rate,
            'avg_response_time_ms': self.query_stats['avg_response_time'],
            'cache_size': len(self.query_cache),
            'query_patterns': len(self.query_stats['query_patterns']),
            'performance_metrics_count': len(self.performance_metrics)
        }
    
    def recommend_new_materialized_views(self) -> List[Dict[str, Any]]:
        """Recommend new materialized views based on query patterns."""
        recommendations = []
        
        # Analyze slow queries
        slow_queries = [m for m in self.performance_metrics 
                       if m['execution_time_ms'] > self.config['slow_query_threshold_seconds'] * 1000]
        
        if slow_queries:
            # Group by query type
            slow_by_type = {}
            for query in slow_queries:
                query_type = query['query_type']
                if query_type not in slow_by_type:
                    slow_by_type[query_type] = []
                slow_by_type[query_type].append(query)
            
            # Recommend views for slow query types
            for query_type, queries in slow_by_type.items():
                if len(queries) > 5:  # Only recommend if frequently slow
                    recommendations.append({
                        'query_type': query_type,
                        'frequency': len(queries),
                        'avg_time_ms': sum(q['execution_time_ms'] for q in queries) / len(queries),
                        'recommended_view': f"ocel.{query_type}_optimized",
                        'priority': 'high' if len(queries) > 20 else 'medium'
                    })
        
        return recommendations
    
    def get_performance_analysis(self) -> Dict[str, Any]:
        """Get detailed performance analysis."""
        if not self.performance_metrics:
            return {'error': 'No performance metrics available'}
        
        # Analyze performance trends
        recent_metrics = self.performance_metrics[-100:]  # Last 100 queries
        avg_recent_time = sum(m['execution_time_ms'] for m in recent_metrics) / len(recent_metrics)
        
        # Find slowest queries
        slowest_queries = sorted(self.performance_metrics, 
                               key=lambda x: x['execution_time_ms'], reverse=True)[:10]
        
        # Analyze by query type
        by_type = {}
        for metric in self.performance_metrics:
            query_type = metric['query_type']
            if query_type not in by_type:
                by_type[query_type] = []
            by_type[query_type].append(metric)
        
        type_analysis = {}
        for query_type, metrics in by_type.items():
            type_analysis[query_type] = {
                'count': len(metrics),
                'avg_time_ms': sum(m['execution_time_ms'] for m in metrics) / len(metrics),
                'max_time_ms': max(m['execution_time_ms'] for m in metrics),
                'materialized_view_usage': sum(1 for m in metrics if m.get('used_materialized_views', False)) / len(metrics)
            }
        
        return {
            'total_metrics': len(self.performance_metrics),
            'avg_recent_time_ms': avg_recent_time,
            'slowest_queries': slowest_queries,
            'type_analysis': type_analysis,
            'optimization_opportunities': self.recommend_new_materialized_views()
        }
    
    def clear_cache(self):
        """Clear query cache."""
        self.query_cache.clear()
        print("Query cache cleared.")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_size = sum(len(str(entry)) for entry in self.query_cache.values())
        
        return {
            'cache_entries': len(self.query_cache),
            'estimated_size_bytes': total_size,
            'estimated_size_mb': total_size / (1024 * 1024),
            'max_size_mb': self.config.get('max_cache_size_mb', 1024),
            'utilization_percent': (total_size / (1024 * 1024)) / self.config.get('max_cache_size_mb', 1024) * 100
        }


def main():
    """Main function for testing the query optimizer."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process Mining Query Optimizer')
    parser.add_argument('--stats', action='store_true', help='Show query statistics')
    parser.add_argument('--performance', action='store_true', help='Show performance analysis')
    parser.add_argument('--recommendations', action='store_true', help='Show optimization recommendations')
    parser.add_argument('--cache-stats', action='store_true', help='Show cache statistics')
    parser.add_argument('--clear-cache', action='store_true', help='Clear query cache')
    
    args = parser.parse_args()
    
    catalog_path = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_path)
    
    optimizer = QueryOptimizer(cat)
    
    if args.stats:
        stats = optimizer.get_query_execution_stats()
        print(f"Query Execution Statistics: {json.dumps(stats, indent=2)}")
    
    elif args.performance:
        analysis = optimizer.get_performance_analysis()
        print(f"Performance Analysis: {json.dumps(analysis, indent=2)}")
    
    elif args.recommendations:
        recommendations = optimizer.recommend_new_materialized_views()
        print(f"Optimization Recommendations: {json.dumps(recommendations, indent=2)}")
    
    elif args.cache_stats:
        cache_stats = optimizer.get_cache_stats()
        print(f"Cache Statistics: {json.dumps(cache_stats, indent=2)}")
    
    elif args.clear_cache:
        optimizer.clear_cache()
    
    else:
        print("No action specified. Use --help for available options.")


if __name__ == '__main__':
    main()
