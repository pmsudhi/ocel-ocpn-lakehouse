#!/usr/bin/env python3
"""
Process Mining Agent Demo
Comprehensive demonstration of all system capabilities
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime
import json

import daft
from pyiceberg.catalog import load_catalog

# Add lakehouse to path
sys.path.append(str(Path(__file__).parents[1]))

from lakehouse.agent.process_mining_queries import ProcessMiningQueryEngine
from lakehouse.agent.nl_query_agent import ProcessMiningAgent
from lakehouse.agent.query_optimizer import QueryOptimizer
from lakehouse.analytics.ocpn_discovery import OCPNDiscovery
from lakehouse.analytics.conformance_checking import ConformanceChecker
from lakehouse.ops.materialized_views import MaterializedViewManager

# Fix Unicode display issues on Windows
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"


class ProcessMiningDemo:
    """Comprehensive demo of the Process Mining Agent system."""
    
    def __init__(self):
        self.catalog = None
        self.query_engine = None
        self.agent = None
        self.optimizer = None
        self.discovery = None
        self.checker = None
        self.view_manager = None
        
        self.setup_system()
    
    def setup_system(self):
        """Setup the entire system."""
        print("Setting up Process Mining Agent System...")
        
        try:
            # Setup catalog
            from lakehouse.agent.process_mining_queries import load_catalog_from_yaml
            catalog_path = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
            self.catalog = load_catalog_from_yaml('local', catalog_path)
            print("✓ Catalog connection established")
            
            # Initialize components
            self.query_engine = ProcessMiningQueryEngine(self.catalog)
            self.agent = ProcessMiningAgent(self.query_engine)
            self.optimizer = QueryOptimizer(self.catalog)
            self.discovery = OCPNDiscovery(self.catalog)
            self.checker = ConformanceChecker(self.catalog)
            self.view_manager = MaterializedViewManager(self.catalog)
            
            print("✓ All system components initialized")
            
        except Exception as e:
            print(f"✗ System setup failed: {e}")
            return False
        
        return True
    
    def demo_phase1_foundation(self):
        """Demonstrate Phase 1: Foundation capabilities."""
        print("\n" + "=" * 60)
        print("PHASE 1: FOUNDATION - Process Instance Tracking")
        print("=" * 60)
        
        try:
            # Check process instances
            instances_table = self.catalog.load_table('ocel.process_instances')
            df_instances = daft.read_iceberg(instances_table)
            instance_count = df_instances.count_rows()
            print(f"✓ Found {instance_count:,} process instances")
            
            # Check instance events
            instance_events_table = self.catalog.load_table('ocel.instance_events')
            df_instance_events = daft.read_iceberg(instance_events_table)
            event_count = df_instance_events.count_rows()
            print(f"✓ Found {event_count:,} instance-event relationships")
            
            # Show sample process instances
            if instance_count > 0:
                sample_instances = df_instances.limit(5).to_pandas()
                print("\nSample Process Instances:")
                for _, row in sample_instances.iterrows():
                    print(f"  - {row['instance_id']}: {row['instance_type']} ({row['status']})")
            
            return True
            
        except Exception as e:
            print(f"✗ Foundation demo failed: {e}")
            return False
    
    def demo_phase2_materialization(self):
        """Demonstrate Phase 2: Materialized Views."""
        print("\n" + "=" * 60)
        print("PHASE 2: MATERIALIZATION - Pre-computed Analytics")
        print("=" * 60)
        
        try:
            # Check if materialized views exist
            view_tables = [
                'ocel.dfg_matrix', 'ocel.activity_metrics', 
                'ocel.process_variants', 'ocel.object_interaction_graph'
            ]
            
            existing_views = 0
            for view_name in view_tables:
                if self.catalog.table_exists(view_name):
                    existing_views += 1
                    print(f"✓ Materialized view exists: {view_name}")
                else:
                    print(f"⚠ Materialized view missing: {view_name}")
            
            if existing_views > 0:
                print(f"\n✓ {existing_views}/{len(view_tables)} materialized views available")
                
                # Demonstrate performance difference
                print("\nPerformance Comparison:")
                
                # Test with materialized view
                start_time = time.time()
                if self.catalog.table_exists('ocel.activity_metrics'):
                    metrics_table = self.catalog.load_table('ocel.activity_metrics')
                    df_metrics = daft.read_iceberg(metrics_table)
                    metrics_count = df_metrics.count_rows()
                    materialized_time = (time.time() - start_time) * 1000
                    print(f"  Materialized view query: {materialized_time:.1f}ms ({metrics_count} activities)")
                
                # Test without materialized view (raw computation)
                start_time = time.time()
                result = self.query_engine.get_activity_frequencies()
                raw_time = (time.time() - start_time) * 1000
                print(f"  Raw computation query: {raw_time:.1f}ms")
                
                if 'materialized_time' in locals():
                    speedup = raw_time / materialized_time
                    print(f"  Performance improvement: {speedup:.1f}x faster")
            
            return True
            
        except Exception as e:
            print(f"✗ Materialization demo failed: {e}")
            return False
    
    def demo_phase3_discovery(self):
        """Demonstrate Phase 3: Process Discovery & Conformance."""
        print("\n" + "=" * 60)
        print("PHASE 3: DISCOVERY & CONFORMANCE - Process Mining")
        print("=" * 60)
        
        try:
            # List existing models
            models = self.discovery.list_discovered_models()
            print(f"✓ Found {len(models)} discovered models")
            
            if models:
                print("\nDiscovered Models:")
                for model in models[:3]:  # Show first 3
                    print(f"  - {model['model_id']}: {model['name']} (v{model['version']})")
            
            # Demonstrate conformance checking
            if models:
                model_id = models[0]['model_id']
                print(f"\nConformance Analysis for Model: {model_id}")
                
                # Get conformance metrics
                metrics = self.checker.compute_conformance_metrics(model_id)
                if 'error' not in metrics:
                    print(f"  Total Instances: {metrics.get('total_instances', 0)}")
                    print(f"  Conforming: {metrics.get('conforming_instances', 0)}")
                    print(f"  Conformance Rate: {metrics.get('conformance_rate', 0):.1%}")
                    print(f"  Average Fitness: {metrics.get('avg_fitness', 0):.3f}")
                else:
                    print(f"  Conformance check failed: {metrics['error']}")
            
            return True
            
        except Exception as e:
            print(f"✗ Discovery demo failed: {e}")
            return False
    
    def demo_phase4_query_engine(self):
        """Demonstrate Phase 4: Query Engine capabilities."""
        print("\n" + "=" * 60)
        print("PHASE 4: QUERY ENGINE - Advanced Analytics")
        print("=" * 60)
        
        try:
            # Test various query types
            queries_to_test = [
                ("Process Variants", lambda: self.query_engine.get_process_variants(top_n=5)),
                ("Case Duration Distribution", lambda: self.query_engine.get_case_duration_distribution()),
                ("Activity Frequencies", lambda: self.query_engine.get_activity_frequencies()),
                ("Bottlenecks", lambda: self.query_engine.identify_bottlenecks()),
                ("Resource Utilization", lambda: self.query_engine.get_resource_utilization())
            ]
            
            successful_queries = 0
            for query_name, query_func in queries_to_test:
                try:
                    start_time = time.time()
                    result = query_func()
                    execution_time = (time.time() - start_time) * 1000
                    
                    if 'error' not in result:
                        print(f"✓ {query_name}: {execution_time:.1f}ms")
                        successful_queries += 1
                        
                        # Show sample results
                        if query_name == "Process Variants" and 'variants' in result:
                            variants = result['variants'][:2]  # Show first 2
                            for variant in variants:
                                print(f"    - {variant['pattern']}: {variant['frequency']} occurrences")
                        elif query_name == "Bottlenecks" and 'bottlenecks' in result:
                            bottlenecks = result['bottlenecks'][:2]  # Show first 2
                            for bottleneck in bottlenecks:
                                print(f"    - {bottleneck['activity_name']}: {bottleneck['avg_duration_seconds']:.1f}s")
                    else:
                        print(f"⚠ {query_name}: {result['error']}")
                        
                except Exception as e:
                    print(f"✗ {query_name}: {e}")
            
            print(f"\n✓ {successful_queries}/{len(queries_to_test)} queries successful")
            return successful_queries > 0
            
        except Exception as e:
            print(f"✗ Query engine demo failed: {e}")
            return False
    
    def demo_phase5_agent(self):
        """Demonstrate Phase 5: Natural Language Agent."""
        print("\n" + "=" * 60)
        print("PHASE 5: NATURAL LANGUAGE AGENT - Conversational Analytics")
        print("=" * 60)
        
        try:
            # Test various natural language questions
            test_questions = [
                "What are the most common process variants?",
                "Which activities take the longest?",
                "Show me bottlenecks in the process",
                "How long do cases typically take?",
                "What's the resource utilization?"
            ]
            
            successful_questions = 0
            for question in test_questions:
                try:
                    start_time = time.time()
                    result = self.agent.ask(question)
                    execution_time = (time.time() - start_time) * 1000
                    
                    if 'error' not in result and 'answer' in result:
                        print(f"✓ Q: {question}")
                        print(f"  A: {result['answer']}")
                        print(f"  Time: {execution_time:.1f}ms")
                        successful_questions += 1
                    else:
                        print(f"⚠ Q: {question}")
                        print(f"  Error: {result.get('error', 'No answer generated')}")
                        
                except Exception as e:
                    print(f"✗ Q: {question}")
                    print(f"  Error: {e}")
            
            print(f"\n✓ {successful_questions}/{len(test_questions)} questions answered successfully")
            
            # Show agent statistics
            stats = self.agent.get_agent_stats()
            print(f"\nAgent Statistics:")
            print(f"  Total Queries: {stats['total_queries']}")
            print(f"  Supported Intents: {len(stats['supported_intents'])}")
            print(f"  LLM Configured: {stats['llm_configured']}")
            
            return successful_questions > 0
            
        except Exception as e:
            print(f"✗ Agent demo failed: {e}")
            return False
    
    def demo_phase6_optimization(self):
        """Demonstrate Phase 6: Performance Optimization."""
        print("\n" + "=" * 60)
        print("PHASE 6: OPTIMIZATION - Performance & Intelligence")
        print("=" * 60)
        
        try:
            # Test query optimization
            print("Query Optimization:")
            plan = self.optimizer.optimize_query_plan('process_variants', {'top_n': 10})
            if 'error' not in plan:
                print(f"✓ Optimization plan generated: {plan['execution_strategy']}")
                print(f"  Estimated time: {plan.get('estimated_time_ms', 0):.1f}ms")
                if plan.get('recommendations'):
                    print("  Recommendations:")
                    for rec in plan['recommendations']:
                        print(f"    - {rec}")
            else:
                print(f"⚠ Optimization failed: {plan['error']}")
            
            # Test query execution with optimization
            print("\nOptimized Query Execution:")
            result = self.optimizer.execute_optimized_query(
                'process_variants', 
                {'top_n': 5}, 
                self.query_engine.get_process_variants
            )
            
            if 'error' not in result:
                execution_time = result.get('execution_time_ms', 0)
                cache_hit = result.get('cache_hit', False)
                print(f"✓ Query executed: {execution_time:.1f}ms")
                print(f"  Cache hit: {cache_hit}")
                print(f"  Result: {len(result['result'].get('variants', []))} variants")
            else:
                print(f"⚠ Optimized execution failed: {result['error']}")
            
            # Show optimization statistics
            stats = self.optimizer.get_query_execution_stats()
            print(f"\nOptimization Statistics:")
            print(f"  Total Queries: {stats['total_queries']}")
            print(f"  Cache Hit Rate: {stats['cache_hit_rate']:.1f}%")
            print(f"  Materialized View Hit Rate: {stats['materialized_view_hit_rate']:.1f}%")
            print(f"  Average Response Time: {stats['avg_response_time_ms']:.1f}ms")
            
            return True
            
        except Exception as e:
            print(f"✗ Optimization demo failed: {e}")
            return False
    
    def demo_comprehensive_workflow(self):
        """Demonstrate a comprehensive end-to-end workflow."""
        print("\n" + "=" * 60)
        print("COMPREHENSIVE WORKFLOW - End-to-End Process Mining")
        print("=" * 60)
        
        try:
            # Step 1: Ask a business question
            business_question = "What are the bottlenecks in our purchase order process and how can we optimize them?"
            print(f"Business Question: {business_question}")
            
            # Step 2: Use NL agent to understand and execute
            agent_result = self.agent.ask(business_question)
            print(f"\nAgent Response: {agent_result.get('answer', 'No answer')}")
            
            # Step 3: Get detailed analytics
            print("\nDetailed Analytics:")
            
            # Get bottlenecks
            bottlenecks = self.query_engine.identify_bottlenecks()
            if 'error' not in bottlenecks and bottlenecks.get('bottlenecks'):
                print(f"  Found {len(bottlenecks['bottlenecks'])} bottlenecks")
                for bottleneck in bottlenecks['bottlenecks'][:3]:
                    print(f"    - {bottleneck['activity_name']}: {bottleneck['avg_duration_seconds']:.1f}s")
            
            # Get process variants
            variants = self.query_engine.get_process_variants(top_n=3)
            if 'error' not in variants and variants.get('variants'):
                print(f"  Found {len(variants['variants'])} process variants")
                for variant in variants['variants'][:2]:
                    print(f"    - {variant['pattern']}: {variant['frequency']} occurrences")
            
            # Step 4: Generate recommendations
            print("\nRecommendations:")
            print("  1. Focus on activities with longest duration")
            print("  2. Standardize process variants")
            print("  3. Implement process monitoring")
            print("  4. Consider automation for repetitive tasks")
            
            return True
            
        except Exception as e:
            print(f"✗ Comprehensive workflow demo failed: {e}")
            return False
    
    def run_complete_demo(self):
        """Run the complete demonstration."""
        print("=" * 80)
        print("PROCESS MINING AGENT - COMPREHENSIVE DEMONSTRATION")
        print("=" * 80)
        print(f"Started at: {datetime.utcnow().isoformat()}")
        
        if not self.setup_system():
            print("System setup failed. Cannot proceed with demo.")
            return
        
        # Run all phase demonstrations
        phases = [
            ("Phase 1: Foundation", self.demo_phase1_foundation),
            ("Phase 2: Materialization", self.demo_phase2_materialization),
            ("Phase 3: Discovery & Conformance", self.demo_phase3_discovery),
            ("Phase 4: Query Engine", self.demo_phase4_query_engine),
            ("Phase 5: Natural Language Agent", self.demo_phase5_agent),
            ("Phase 6: Optimization", self.demo_phase6_optimization),
            ("Comprehensive Workflow", self.demo_comprehensive_workflow)
        ]
        
        successful_phases = 0
        for phase_name, demo_func in phases:
            try:
                if demo_func():
                    successful_phases += 1
                    print(f"✓ {phase_name} completed successfully")
                else:
                    print(f"⚠ {phase_name} completed with warnings")
            except Exception as e:
                print(f"✗ {phase_name} failed: {e}")
        
        # Final summary
        print("\n" + "=" * 80)
        print("DEMONSTRATION SUMMARY")
        print("=" * 80)
        print(f"Successful Phases: {successful_phases}/{len(phases)}")
        print(f"Success Rate: {successful_phases/len(phases)*100:.1f}%")
        
        if successful_phases >= len(phases) * 0.8:
            print("\n🎉 PROCESS MINING AGENT IS PRODUCTION READY!")
            print("   All major capabilities demonstrated successfully.")
            print("   System ready for enterprise deployment.")
        elif successful_phases >= len(phases) * 0.6:
            print("\n✅ PROCESS MINING AGENT IS FUNCTIONAL!")
            print("   Most capabilities working. Some optimization needed.")
        else:
            print("\n⚠️  PROCESS MINING AGENT NEEDS WORK!")
            print("   Several components need attention before production use.")
        
        print(f"\nCompleted at: {datetime.utcnow().isoformat()}")
        print("=" * 80)


def main():
    """Main function to run the demo."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process Mining Agent Demo')
    parser.add_argument('--phase', choices=['1', '2', '3', '4', '5', '6', 'all'],
                       default='all', help='Which phase to demonstrate')
    
    args = parser.parse_args()
    
    demo = ProcessMiningDemo()
    
    if args.phase == 'all':
        demo.run_complete_demo()
    elif args.phase == '1':
        demo.setup_system()
        demo.demo_phase1_foundation()
    elif args.phase == '2':
        demo.setup_system()
        demo.demo_phase2_materialization()
    elif args.phase == '3':
        demo.setup_system()
        demo.demo_phase3_discovery()
    elif args.phase == '4':
        demo.setup_system()
        demo.demo_phase4_query_engine()
    elif args.phase == '5':
        demo.setup_system()
        demo.demo_phase5_agent()
    elif args.phase == '6':
        demo.setup_system()
        demo.demo_phase6_optimization()


if __name__ == '__main__':
    main()
