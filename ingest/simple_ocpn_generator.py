#!/usr/bin/env python3
"""
Simple OCPN Generator and Iceberg Mapper
Generates basic Object-Centric Petri Nets from complete OCEL data without pm4py dependency

This script:
1. Loads the complete OCEL data directly
2. Generates basic OCPN models using simple algorithms
3. Maps the OCPN data to Iceberg tables
4. Provides OCPN analytics capabilities

Usage:
    python simple_ocpn_generator.py [ocel_file] [catalog_config]

Author: Process Mining Agent
Version: 1.0.0
"""

import pandas as pd
import json
import sys
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import daft
from pyiceberg.catalog import load_catalog
from collections import defaultdict, Counter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class SimpleOCPNGenerator:
    """Simple OCPN generator with Iceberg mapping (no pm4py dependency)."""
    
    def __init__(self, catalog_config_path: Path):
        self.catalog_config_path = catalog_config_path
        self.catalog = self._load_catalog()
        self.discovery_algorithms = {
            'simple_dfg': self._simple_dfg_discovery,
            'activity_based': self._activity_based_discovery,
            'object_centric': self._object_centric_discovery
        }
    
    def _load_catalog(self):
        """Load PyIceberg catalog from config."""
        cfg = {}
        with self.catalog_config_path.open('r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if ':' in line:
                    k, v = line.split(':', 1)
                    cfg[k.strip()] = v.strip()
        return load_catalog('local', **cfg)
    
    def generate_ocpn_from_ocel(self, ocel_file_path: Path, 
                               algorithms: List[str] = None) -> Dict[str, Any]:
        """
        Generate OCPN models from complete OCEL data using simple algorithms.
        
        Args:
            ocel_file_path: Path to complete OCEL JSON file
            algorithms: List of discovery algorithms to use
            
        Returns:
            Dictionary with generated models information
        """
        if algorithms is None:
            algorithms = ['simple_dfg', 'activity_based', 'object_centric']
        
        logger.info(f"Starting OCPN generation from {ocel_file_path}")
        
        try:
            # Load OCEL data directly
            ocel_data = self._load_ocel_direct(ocel_file_path)
            if ocel_data is None:
                return {'error': 'Failed to load OCEL data'}
            
            logger.info(f"Successfully loaded OCEL data")
            
            # Generate models using each algorithm
            generated_models = {}
            
            for algorithm in algorithms:
                if algorithm in self.discovery_algorithms:
                    logger.info(f"Generating OCPN model using {algorithm}...")
                    
                    try:
                        model_data = self.discovery_algorithms[algorithm](ocel_data)
                        if model_data:
                            model_id = self._store_ocpn_model(model_data, algorithm)
                            generated_models[algorithm] = {
                                'model_id': model_id,
                                'algorithm': algorithm,
                                'places_count': len(model_data.get('places', [])),
                                'transitions_count': len(model_data.get('transitions', [])),
                                'arcs_count': len(model_data.get('arcs', [])),
                                'status': 'success'
                            }
                            logger.info(f"[SUCCESS] {algorithm} model generated and stored")
                        else:
                            generated_models[algorithm] = {
                                'algorithm': algorithm,
                                'status': 'failed',
                                'error': 'No model data generated'
                            }
                            logger.warning(f"[WARNING] {algorithm} failed to generate model")
                    
                    except Exception as e:
                        generated_models[algorithm] = {
                            'algorithm': algorithm,
                            'status': 'failed',
                            'error': str(e)
                        }
                        logger.error(f"[ERROR] {algorithm} failed: {e}")
                else:
                    logger.warning(f"[WARNING] Unknown algorithm: {algorithm}")
            
            # Generate summary
            successful_models = [m for m in generated_models.values() if m.get('status') == 'success']
            failed_models = [m for m in generated_models.values() if m.get('status') == 'failed']
            
            summary = {
                'total_algorithms': len(algorithms),
                'successful_models': len(successful_models),
                'failed_models': len(failed_models),
                'models': generated_models,
                'generation_time': datetime.utcnow().isoformat()
            }
            
            logger.info(f"OCPN generation completed: {len(successful_models)}/{len(algorithms)} successful")
            return summary
            
        except Exception as e:
            logger.error(f"OCPN generation failed: {e}")
            return {'error': str(e)}
    
    def _load_ocel_direct(self, ocel_file_path: Path) -> Optional[Dict[str, Any]]:
        """Load OCEL data directly from JSON file."""
        try:
            with open(ocel_file_path, 'r', encoding='utf-8') as f:
                ocel_data = json.load(f)
            
            logger.info(f"Loaded OCEL data: {len(ocel_data.get('events', []))} events, {len(ocel_data.get('objects', []))} objects")
            return ocel_data
            
        except Exception as e:
            logger.error(f"Failed to load OCEL data: {e}")
            return None
    
    def _simple_dfg_discovery(self, ocel_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate OCPN using simple Direct-Follows Graph discovery."""
        try:
            events = ocel_data.get('events', [])
            if not events:
                return None
            
            # Build activity sequence per process instance
            instance_sequences = defaultdict(list)
            
            for event in events:
                # Extract instance ID from event attributes or relationships
                instance_id = None
                
                # Try to get instance ID from attributes
                for attr in event.get('attributes', []):
                    if attr.get('name') in ['requestId', 'orderId', 'instanceId']:
                        instance_id = str(attr.get('value', ''))
                        break
                
                # Fallback: use first object relationship
                if not instance_id and event.get('relationships'):
                    for rel in event['relationships']:
                        obj_id = rel.get('objectId', '')
                        if 'REQUEST' in obj_id or 'ORDER' in obj_id:
                            instance_id = obj_id.split('_')[-1]  # Extract ID
                            break
                
                if instance_id:
                    instance_sequences[instance_id].append({
                        'event_id': event.get('id'),
                        'activity': event.get('type'),
                        'timestamp': event.get('time')
                    })
            
            # Sort events by timestamp within each instance
            for instance_id in instance_sequences:
                instance_sequences[instance_id].sort(key=lambda x: x['timestamp'])
            
            # Build DFG (Direct-Follows Graph)
            dfg = defaultdict(int)
            activities = set()
            
            for instance_id, events in instance_sequences.items():
                if len(events) < 2:
                    continue
                
                for i in range(len(events) - 1):
                    from_activity = events[i]['activity']
                    to_activity = events[i + 1]['activity']
                    dfg[(from_activity, to_activity)] += 1
                    activities.add(from_activity)
                    activities.add(to_activity)
            
            # Convert DFG to Petri net
            places = []
            transitions = []
            arcs = []
            
            # Create places for each activity
            for i, activity in enumerate(activities):
                places.append({
                    'place_id': f'p_{activity}',
                    'label': f'Place_{activity}',
                    'type': 'place'
                })
            
            # Create transitions for each activity
            for activity in activities:
                transitions.append({
                    'transition_id': f't_{activity}',
                    'label': activity,
                    'type': 'transition',
                    'invisible': False
                })
            
            # Create arcs based on DFG
            for (from_activity, to_activity), frequency in dfg.items():
                if frequency > 0:  # Only include significant flows
                    arcs.append({
                        'arc_id': f'arc_{from_activity}_{to_activity}',
                        'src_type': 'place',
                        'src_id': f'p_{from_activity}',
                        'dst_type': 'transition',
                        'dst_id': f't_{to_activity}',
                        'weight': 1
                    })
                    
                    arcs.append({
                        'arc_id': f'arc_{to_activity}_{from_activity}',
                        'src_type': 'transition',
                        'src_id': f't_{from_activity}',
                        'dst_type': 'place',
                        'dst_id': f'p_{to_activity}',
                        'weight': 1
                    })
            
            return {
                'places': places,
                'transitions': transitions,
                'arcs': arcs,
                'algorithm': 'simple_dfg',
                'dfg_edges': len(dfg),
                'activities': len(activities)
            }
            
        except Exception as e:
            logger.error(f"Simple DFG discovery failed: {e}")
            return None
    
    def _activity_based_discovery(self, ocel_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate OCPN using activity-based discovery."""
        try:
            events = ocel_data.get('events', [])
            if not events:
                return None
            
            # Count activity frequencies
            activity_counts = Counter()
            for event in events:
                activity = event.get('type', 'unknown')
                activity_counts[activity] += 1
            
            # Create places and transitions for each activity
            places = []
            transitions = []
            arcs = []
            
            for activity, count in activity_counts.items():
                # Create place for activity
                places.append({
                    'place_id': f'p_{activity}',
                    'label': f'Place_{activity}',
                    'type': 'place'
                })
                
                # Create transition for activity
                transitions.append({
                    'transition_id': f't_{activity}',
                    'label': activity,
                    'type': 'transition',
                    'invisible': False,
                    'frequency': count
                })
                
                # Create self-loop arc
                arcs.append({
                    'arc_id': f'arc_{activity}_self',
                    'src_type': 'place',
                    'src_id': f'p_{activity}',
                    'dst_type': 'transition',
                    'dst_id': f't_{activity}',
                    'weight': 1
                })
                
                arcs.append({
                    'arc_id': f'arc_{activity}_self_back',
                    'src_type': 'transition',
                    'src_id': f't_{activity}',
                    'dst_type': 'place',
                    'dst_id': f'p_{activity}',
                    'weight': 1
                })
            
            return {
                'places': places,
                'transitions': transitions,
                'arcs': arcs,
                'algorithm': 'activity_based',
                'activities': len(activity_counts)
            }
            
        except Exception as e:
            logger.error(f"Activity-based discovery failed: {e}")
            return None
    
    def _object_centric_discovery(self, ocel_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate OCPN using object-centric discovery."""
        try:
            events = ocel_data.get('events', [])
            objects = ocel_data.get('objects', [])
            
            if not events or not objects:
                return None
            
            # Group events by object type
            object_type_events = defaultdict(list)
            
            for event in events:
                for rel in event.get('relationships', []):
                    obj_id = rel.get('objectId', '')
                    if '_' in obj_id:
                        obj_type = obj_id.split('_')[1]  # Extract type (REQUEST, ORDER, etc.)
                        object_type_events[obj_type].append(event)
            
            # Create places and transitions for each object type
            places = []
            transitions = []
            arcs = []
            
            for obj_type, type_events in object_type_events.items():
                # Create place for object type
                places.append({
                    'place_id': f'p_{obj_type}',
                    'label': f'Place_{obj_type}',
                    'type': 'place'
                })
                
                # Create transitions for events involving this object type
                event_types = set()
                for event in type_events:
                    event_types.add(event.get('type', 'unknown'))
                
                for event_type in event_types:
                    transitions.append({
                        'transition_id': f't_{obj_type}_{event_type}',
                        'label': f'{obj_type}_{event_type}',
                        'type': 'transition',
                        'invisible': False,
                        'object_type': obj_type,
                        'event_type': event_type
                    })
                    
                    # Create arcs
                    arcs.append({
                        'arc_id': f'arc_{obj_type}_{event_type}',
                        'src_type': 'place',
                        'src_id': f'p_{obj_type}',
                        'dst_type': 'transition',
                        'dst_id': f't_{obj_type}_{event_type}',
                        'weight': 1
                    })
                    
                    arcs.append({
                        'arc_id': f'arc_{obj_type}_{event_type}_back',
                        'src_type': 'transition',
                        'src_id': f't_{obj_type}_{event_type}',
                        'dst_type': 'place',
                        'dst_id': f'p_{obj_type}',
                        'weight': 1
                    })
            
            return {
                'places': places,
                'transitions': transitions,
                'arcs': arcs,
                'algorithm': 'object_centric',
                'object_types': len(object_type_events)
            }
            
        except Exception as e:
            logger.error(f"Object-centric discovery failed: {e}")
            return None
    
    def _store_ocpn_model(self, model_data: Dict[str, Any], algorithm: str) -> str:
        """Store OCPN model data in Iceberg tables."""
        try:
            model_id = f"model_{algorithm}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            
            # Create model metadata
            model_metadata = {
                'model_id': model_id,
                'version': 1,
                'name': f'OCPN Model - {algorithm}',
                'created_at': datetime.utcnow().isoformat(),
                'source_format': 'OCPN',
                'algorithm': algorithm,
                'raw_pnml': None,
                'notes': f'Generated using {algorithm} algorithm'
            }
            
            # Add model_id to all data
            for place in model_data.get('places', []):
                place['model_id'] = model_id
            
            for transition in model_data.get('transitions', []):
                transition['model_id'] = model_id
            
            for arc in model_data.get('arcs', []):
                arc['model_id'] = model_id
            
            # Store in Iceberg tables
            self._write_to_iceberg_table('ocpn.models', [model_metadata])
            self._write_to_iceberg_table('ocpn.places', model_data.get('places', []))
            self._write_to_iceberg_table('ocpn.transitions', model_data.get('transitions', []))
            self._write_to_iceberg_table('ocpn.arcs', model_data.get('arcs', []))
            
            logger.info(f"OCPN model {model_id} stored successfully")
            return model_id
            
        except Exception as e:
            logger.error(f"Failed to store OCPN model: {e}")
            raise
    
    def _write_to_iceberg_table(self, table_name: str, data: List[Dict[str, Any]]):
        """Write data to Iceberg table using Daft."""
        try:
            if not data:
                return
            
            # Convert to Daft DataFrame
            df = daft.from_pylist(data)
            
            # Load table
            table = self.catalog.load_table(table_name)
            
            # Write to Iceberg
            df.write_iceberg(table, mode="append")
            
            logger.info(f"Written {len(data)} records to {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to write to {table_name}: {e}")
            raise
    
    def get_ocpn_analytics(self) -> Dict[str, Any]:
        """Get analytics about stored OCPN models."""
        try:
            # Load models table
            models_table = self.catalog.load_table('ocpn.models')
            df_models = daft.read_iceberg(models_table)
            models_data = df_models.to_pandas()
            
            # Load places table
            places_table = self.catalog.load_table('ocpn.places')
            df_places = daft.read_iceberg(places_table)
            places_data = df_places.to_pandas()
            
            # Load transitions table
            transitions_table = self.catalog.load_table('ocpn.transitions')
            df_transitions = daft.read_iceberg(transitions_table)
            transitions_data = df_transitions.to_pandas()
            
            # Load arcs table
            arcs_table = self.catalog.load_table('ocpn.arcs')
            df_arcs = daft.read_iceberg(arcs_table)
            arcs_data = df_arcs.to_pandas()
            
            # Generate analytics
            analytics = {
                'total_models': len(models_data),
                'models_by_algorithm': models_data['algorithm'].value_counts().to_dict() if 'algorithm' in models_data.columns else {},
                'total_places': len(places_data),
                'total_transitions': len(transitions_data),
                'total_arcs': len(arcs_data),
                'models': models_data.to_dict('records') if not models_data.empty else []
            }
            
            return analytics
            
        except Exception as e:
            logger.error(f"Failed to get OCPN analytics: {e}")
            return {'error': str(e)}

def main():
    """Main function."""
    if len(sys.argv) < 3:
        print("Usage: python simple_ocpn_generator.py <ocel_file> <catalog_config>")
        print("Example: python simple_ocpn_generator.py data/purchase_to_pay_ocel_complete.json lakehouse/catalogs/local.yaml")
        sys.exit(1)
    
    ocel_file_path = Path(sys.argv[1])
    catalog_config_path = Path(sys.argv[2])
    
    if not ocel_file_path.exists():
        print(f"Error: OCEL file not found: {ocel_file_path}")
        sys.exit(1)
    
    if not catalog_config_path.exists():
        print(f"Error: Catalog config not found: {catalog_config_path}")
        sys.exit(1)
    
    # Create generator
    generator = SimpleOCPNGenerator(catalog_config_path)
    
    # Generate OCPN models
    logger.info("Starting OCPN generation...")
    result = generator.generate_ocpn_from_ocel(ocel_file_path)
    
    if 'error' in result:
        logger.error(f"OCPN generation failed: {result['error']}")
        sys.exit(1)
    
    # Print results
    print("\n" + "=" * 80)
    print("OCPN GENERATION RESULTS")
    print("=" * 80)
    print(f"Total algorithms: {result['total_algorithms']}")
    print(f"Successful models: {result['successful_models']}")
    print(f"Failed models: {result['failed_models']}")
    
    print("\nModel details:")
    for algorithm, model_info in result['models'].items():
        status = model_info.get('status', 'unknown')
        if status == 'success':
            print(f"  [OK] {algorithm}: Model ID {model_info.get('model_id', 'N/A')}")
            print(f"     Places: {model_info.get('places_count', 0)}")
            print(f"     Transitions: {model_info.get('transitions_count', 0)}")
            print(f"     Arcs: {model_info.get('arcs_count', 0)}")
        else:
            print(f"  [FAIL] {algorithm}: {model_info.get('error', 'Unknown error')}")
    
    # Get analytics
    print("\n" + "=" * 80)
    print("OCPN ANALYTICS")
    print("=" * 80)
    analytics = generator.get_ocpn_analytics()
    
    if 'error' not in analytics:
        print(f"Total models in database: {analytics['total_models']}")
        print(f"Total places: {analytics['total_places']}")
        print(f"Total transitions: {analytics['total_transitions']}")
        print(f"Total arcs: {analytics['total_arcs']}")
        
        if analytics['models_by_algorithm']:
            print("\nModels by algorithm:")
            for algorithm, count in analytics['models_by_algorithm'].items():
                print(f"  {algorithm}: {count}")
    else:
        print(f"Analytics error: {analytics['error']}")
    
    logger.info("OCPN generation completed successfully!")

if __name__ == "__main__":
    main()
