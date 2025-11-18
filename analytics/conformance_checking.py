#!/usr/bin/env python3
"""
Conformance Checking for Process Mining
Checks how well process instances conform to discovered models
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import json

import daft
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


class ConformanceChecker:
    """Checks conformance of process instances against discovered models."""
    
    def __init__(self, catalog):
        self.catalog = catalog
    
    def compute_conformance_metrics(self, model_id: str, 
                                  instance_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Compute conformance metrics for instances against a model.
        
        Args:
            model_id: ID of the discovered model
            instance_ids: Specific instance IDs to check (None for all)
            
        Returns:
            Dictionary with conformance metrics
        """
        print(f"Computing conformance metrics for model {model_id}...")
        
        try:
            # Get model structure
            model_structure = self._get_model_structure(model_id)
            if 'error' in model_structure:
                return model_structure
            
            # Get process instances to check
            instances = self._get_process_instances(instance_ids)
            if not instances:
                return {'error': 'No process instances found'}
            
            # Compute conformance for each instance
            conformance_results = []
            total_instances = len(instances)
            conforming_instances = 0
            
            for instance_id in instances:
                instance_conformance = self._check_instance_conformance(
                    instance_id, model_structure
                )
                conformance_results.append({
                    'instance_id': instance_id,
                    'conformance_score': instance_conformance['score'],
                    'fitness': instance_conformance['fitness'],
                    'precision': instance_conformance['precision'],
                    'deviations': instance_conformance['deviations']
                })
                
                if instance_conformance['score'] > 0.8:  # Threshold for "conforming"
                    conforming_instances += 1
            
            # Calculate overall metrics
            avg_fitness = sum(r['fitness'] for r in conformance_results) / total_instances
            avg_precision = sum(r['precision'] for r in conformance_results) / total_instances
            avg_score = sum(r['conformance_score'] for r in conformance_results) / total_instances
            
            conformance_rate = conforming_instances / total_instances
            
            # Find common deviations
            all_deviations = []
            for result in conformance_results:
                all_deviations.extend(result['deviations'])
            
            deviation_counts = {}
            for deviation in all_deviations:
                deviation_key = f"{deviation['type']}: {deviation['description']}"
                deviation_counts[deviation_key] = deviation_counts.get(deviation_key, 0) + 1
            
            common_deviations = sorted(
                deviation_counts.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:10]
            
            metrics = {
                'model_id': model_id,
                'total_instances': total_instances,
                'conforming_instances': conforming_instances,
                'conformance_rate': conformance_rate,
                'avg_fitness': avg_fitness,
                'avg_precision': avg_precision,
                'avg_conformance_score': avg_score,
                'common_deviations': common_deviations,
                'instance_results': conformance_results,
                'computed_at': datetime.utcnow().isoformat()
            }
            
            print(f"[SUCCESS] Conformance metrics computed:")
            print(f"  Total instances: {total_instances}")
            print(f"  Conforming: {conforming_instances} ({conformance_rate:.1%})")
            print(f"  Average fitness: {avg_fitness:.3f}")
            print(f"  Average precision: {avg_precision:.3f}")
            
            return metrics
            
        except Exception as e:
            print(f"[ERROR] Conformance checking failed: {e}")
            return {'error': str(e)}
    
    def _get_model_structure(self, model_id: str) -> Dict[str, Any]:
        """Get the structure of a discovered model."""
        try:
            # Get places
            places_table = self.catalog.load_table('ocpn.places')
            df_places = daft.read_iceberg(places_table)
            places = df_places.where(daft.col("model_id") == model_id).to_pandas()
            
            # Get transitions
            transitions_table = self.catalog.load_table('ocpn.transitions')
            df_transitions = daft.read_iceberg(transitions_table)
            transitions = df_transitions.where(daft.col("model_id") == model_id).to_pandas()
            
            # Get arcs
            arcs_table = self.catalog.load_table('ocpn.arcs')
            df_arcs = daft.read_iceberg(arcs_table)
            arcs = df_arcs.where(daft.col("model_id") == model_id).to_pandas()
            
            if places.empty or transitions.empty:
                return {'error': f'Model {model_id} not found or incomplete'}
            
            # Build model structure
            model_structure = {
                'places': places.to_dict('records'),
                'transitions': transitions.to_dict('records'),
                'arcs': arcs.to_dict('records') if not arcs.empty else []
            }
            
            # Build activity graph for conformance checking
            activity_graph = {}
            for _, arc in arcs.iterrows():
                src_id = arc['src_id']
                dst_id = arc['dst_id']
                
                if src_id not in activity_graph:
                    activity_graph[src_id] = []
                activity_graph[src_id].append(dst_id)
            
            model_structure['activity_graph'] = activity_graph
            
            return model_structure
            
        except Exception as e:
            return {'error': f'Failed to get model structure: {e}'}
    
    def _get_process_instances(self, instance_ids: Optional[List[str]] = None) -> List[str]:
        """Get list of process instance IDs to check."""
        try:
            instances_table = self.catalog.load_table('ocel.process_instances')
            df_instances = daft.read_iceberg(instances_table)
            
            if instance_ids:
                # Filter to specific instances
                df_instances = df_instances.where(daft.col("instance_id").isin(instance_ids))
            
            instances = df_instances.select("instance_id").to_pandas()['instance_id'].tolist()
            return instances
            
        except Exception as e:
            print(f"[WARNING] Could not get process instances: {e}")
            return []
    
    def _check_instance_conformance(self, instance_id: str, 
                                  model_structure: Dict[str, Any]) -> Dict[str, Any]:
        """Check conformance of a single instance against the model."""
        try:
            # Get instance event sequence
            instance_events_table = self.catalog.load_table('ocel.instance_events')
            events_table = self.catalog.load_table('ocel.events')
            
            df_instance_events = daft.read_iceberg(instance_events_table)
            df_events = daft.read_iceberg(events_table)
            
            # Get events for this instance
            instance_events = df_instance_events.where(
                daft.col("instance_id") == instance_id
            ).join(
                df_events,
                left_on="event_id",
                right_on="id",
                how="inner"
            ).sort("event_sequence")
            
            if instance_events.count_rows() == 0:
                return {
                    'score': 0.0,
                    'fitness': 0.0,
                    'precision': 0.0,
                    'deviations': [{'type': 'missing_data', 'description': 'No events found'}]
                }
            
            # Get activity sequence
            activities = instance_events.select("type").to_pandas()['type'].tolist()
            
            # Check conformance using simplified algorithm
            deviations = []
            fitness_score = self._compute_fitness(activities, model_structure, deviations)
            precision_score = self._compute_precision(activities, model_structure)
            
            # Overall conformance score (weighted average)
            conformance_score = (fitness_score * 0.7) + (precision_score * 0.3)
            
            return {
                'score': conformance_score,
                'fitness': fitness_score,
                'precision': precision_score,
                'deviations': deviations
            }
            
        except Exception as e:
            return {
                'score': 0.0,
                'fitness': 0.0,
                'precision': 0.0,
                'deviations': [{'type': 'error', 'description': f'Conformance check failed: {e}'}]
            }
    
    def _compute_fitness(self, activities: List[str], 
                        model_structure: Dict[str, Any], 
                        deviations: List[Dict]) -> float:
        """
        Compute fitness: how much of the trace can be replayed on the model.
        Returns value between 0.0 and 1.0.
        """
        try:
            # Simplified fitness computation
            # In production, this would use proper token replay
            
            activity_graph = model_structure.get('activity_graph', {})
            transitions = model_structure.get('transitions', [])
            
            if not activities or not transitions:
                return 0.0
            
            # Check if each activity transition is allowed by the model
            allowed_transitions = 0
            total_transitions = len(activities) - 1
            
            for i in range(len(activities) - 1):
                from_activity = activities[i]
                to_activity = activities[i + 1]
                
                # Check if this transition is allowed
                if self._is_transition_allowed(from_activity, to_activity, activity_graph):
                    allowed_transitions += 1
                else:
                    deviations.append({
                        'type': 'invalid_transition',
                        'description': f'Invalid transition: {from_activity} -> {to_activity}'
                    })
            
            if total_transitions == 0:
                return 1.0  # Single activity, always fits
            
            fitness = allowed_transitions / total_transitions
            return fitness
            
        except Exception as e:
            deviations.append({'type': 'error', 'description': f'Fitness computation failed: {e}'})
            return 0.0
    
    def _compute_precision(self, activities: List[str], 
                          model_structure: Dict[str, Any]) -> float:
        """
        Compute precision: how much behavior the model allows vs the log.
        Returns value between 0.0 and 1.0.
        """
        try:
            # Simplified precision computation
            # In production, this would compare model behavior with log behavior
            
            transitions = model_structure.get('transitions', [])
            if not transitions:
                return 0.0
            
            # Count unique activities in the trace
            unique_activities = set(activities)
            
            # Count activities that appear in the model
            model_activities = set()
            for transition in transitions:
                if 'label' in transition:
                    # Extract activities from transition labels
                    label = transition['label']
                    if '->' in label:
                        from_act, to_act = label.split('->', 1)
                        model_activities.add(from_act.strip())
                        model_activities.add(to_act.strip())
            
            if not model_activities:
                return 0.0
            
            # Precision = activities in trace that are in model / total activities in trace
            precision = len(unique_activities.intersection(model_activities)) / len(unique_activities)
            return precision
            
        except Exception as e:
            return 0.0
    
    def _is_transition_allowed(self, from_activity: str, to_activity: str, 
                              activity_graph: Dict[str, List[str]]) -> bool:
        """Check if a transition is allowed by the model."""
        try:
            # Find the corresponding nodes in the model
            # This is simplified - in production would use proper graph traversal
            
            for src_id, dst_ids in activity_graph.items():
                # Check if this source can reach the destination
                if to_activity in dst_ids:
                    return True
            
            return False
            
        except Exception:
            return False
    
    def identify_deviations(self, model_id: str, 
                          min_frequency: int = 1) -> List[Dict[str, Any]]:
        """
        Identify common deviations from the model.
        
        Args:
            model_id: ID of the discovered model
            min_frequency: Minimum frequency to report deviation
            
        Returns:
            List of deviation patterns
        """
        print(f"Identifying deviations for model {model_id}...")
        
        try:
            # Get conformance results
            conformance_metrics = self.compute_conformance_metrics(model_id)
            if 'error' in conformance_metrics:
                return [{'error': conformance_metrics['error']}]
            
            # Extract deviations from results
            all_deviations = []
            for result in conformance_metrics.get('instance_results', []):
                all_deviations.extend(result.get('deviations', []))
            
            # Count deviation patterns
            deviation_patterns = {}
            for deviation in all_deviations:
                pattern_key = f"{deviation['type']}: {deviation['description']}"
                if pattern_key not in deviation_patterns:
                    deviation_patterns[pattern_key] = {
                        'type': deviation['type'],
                        'description': deviation['description'],
                        'frequency': 0,
                        'affected_instances': []
                    }
                deviation_patterns[pattern_key]['frequency'] += 1
            
            # Filter by minimum frequency
            filtered_deviations = [
                pattern for pattern in deviation_patterns.values()
                if pattern['frequency'] >= min_frequency
            ]
            
            # Sort by frequency
            filtered_deviations.sort(key=lambda x: x['frequency'], reverse=True)
            
            print(f"[SUCCESS] Found {len(filtered_deviations)} deviation patterns")
            return filtered_deviations
            
        except Exception as e:
            print(f"[ERROR] Deviation identification failed: {e}")
            return [{'error': str(e)}]
    
    def get_conformance_report(self, model_id: str) -> Dict[str, Any]:
        """Generate a comprehensive conformance report."""
        print(f"Generating conformance report for model {model_id}...")
        
        try:
            # Get conformance metrics
            metrics = self.compute_conformance_metrics(model_id)
            if 'error' in metrics:
                return metrics
            
            # Get deviations
            deviations = self.identify_deviations(model_id)
            
            # Generate report
            report = {
                'model_id': model_id,
                'generated_at': datetime.utcnow().isoformat(),
                'summary': {
                    'total_instances': metrics['total_instances'],
                    'conforming_instances': metrics['conforming_instances'],
                    'conformance_rate': metrics['conformance_rate'],
                    'avg_fitness': metrics['avg_fitness'],
                    'avg_precision': metrics['avg_precision'],
                    'avg_conformance_score': metrics['avg_conformance_score']
                },
                'deviations': deviations,
                'recommendations': self._generate_recommendations(metrics, deviations)
            }
            
            return report
            
        except Exception as e:
            return {'error': f'Failed to generate conformance report: {e}'}
    
    def _generate_recommendations(self, metrics: Dict[str, Any], 
                                deviations: List[Dict[str, Any]]) -> List[str]:
        """Generate recommendations based on conformance analysis."""
        recommendations = []
        
        conformance_rate = metrics.get('conformance_rate', 0)
        avg_fitness = metrics.get('avg_fitness', 0)
        avg_precision = metrics.get('avg_precision', 0)
        
        if conformance_rate < 0.5:
            recommendations.append("Low conformance rate detected. Consider model refinement or data quality improvement.")
        
        if avg_fitness < 0.7:
            recommendations.append("Low fitness score indicates many instances don't fit the model. Review model completeness.")
        
        if avg_precision < 0.7:
            recommendations.append("Low precision score indicates model allows too much behavior. Consider model simplification.")
        
        if len(deviations) > 10:
            recommendations.append("High number of deviations detected. Consider process standardization or model updates.")
        
        # Add specific recommendations based on deviation types
        deviation_types = [d.get('type', '') for d in deviations]
        if 'invalid_transition' in deviation_types:
            recommendations.append("Invalid transitions detected. Review process flow and update model accordingly.")
        
        if not recommendations:
            recommendations.append("Process conformance is good. Consider monitoring for new patterns.")
        
        return recommendations


def main():
    """Main function for conformance checking."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process Conformance Checking')
    parser.add_argument('--model-id', required=True, help='Model ID to check against')
    parser.add_argument('--instance-ids', nargs='+', help='Specific instance IDs to check')
    parser.add_argument('--deviations', action='store_true', help='Identify deviations')
    parser.add_argument('--report', action='store_true', help='Generate full conformance report')
    
    args = parser.parse_args()
    
    catalog_path = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_path)
    
    checker = ConformanceChecker(cat)
    
    if args.deviations:
        deviations = checker.identify_deviations(args.model_id)
        print(f"\nDeviations for model {args.model_id}:")
        for i, deviation in enumerate(deviations[:10], 1):  # Show top 10
            if 'error' not in deviation:
                print(f"  {i}. {deviation['type']}: {deviation['description']} (freq: {deviation['frequency']})")
            else:
                print(f"  {i}. ERROR: {deviation['error']}")
    
    elif args.report:
        report = checker.get_conformance_report(args.model_id)
        if 'error' in report:
            print(f"[ERROR] {report['error']}")
        else:
            print(f"\nConformance Report for Model {args.model_id}:")
            print(f"Generated: {report['generated_at']}")
            print(f"Total Instances: {report['summary']['total_instances']}")
            print(f"Conforming: {report['summary']['conforming_instances']} ({report['summary']['conformance_rate']:.1%})")
            print(f"Average Fitness: {report['summary']['avg_fitness']:.3f}")
            print(f"Average Precision: {report['summary']['avg_precision']:.3f}")
            print(f"Deviations: {len(report['deviations'])}")
            print("\nRecommendations:")
            for rec in report['recommendations']:
                print(f"  - {rec}")
    
    else:
        # Default: compute conformance metrics
        metrics = checker.compute_conformance_metrics(
            args.model_id, 
            args.instance_ids
        )
        
        if 'error' in metrics:
            print(f"[ERROR] {metrics['error']}")
        else:
            print(f"\nConformance Metrics for Model {args.model_id}:")
            print(f"Total Instances: {metrics['total_instances']}")
            print(f"Conforming: {metrics['conforming_instances']} ({metrics['conformance_rate']:.1%})")
            print(f"Average Fitness: {metrics['avg_fitness']:.3f}")
            print(f"Average Precision: {metrics['avg_precision']:.3f}")
            print(f"Average Score: {metrics['avg_conformance_score']:.3f}")


if __name__ == '__main__':
    main()
