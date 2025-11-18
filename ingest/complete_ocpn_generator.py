#!/usr/bin/env python3
"""
Complete OCPN Generator and Iceberg Mapper
Generates Object-Centric Petri Nets from complete OCEL data and maps to Iceberg

This script:
1. Loads the complete OCEL data using pm4py
2. Generates OCPN models using multiple algorithms
3. Maps the OCPN data to Iceberg tables
4. Provides comprehensive OCPN analytics capabilities

Usage:
    python complete_ocpn_generator.py [ocel_file] [catalog_config]

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CompleteOCPNGenerator:
    """Complete OCPN generator with Iceberg mapping."""
    
    def __init__(self, catalog_config_path: Path):
        self.catalog_config_path = catalog_config_path
        self.catalog = self._load_catalog()
        self.discovery_algorithms = {
            'inductive_miner': self._inductive_miner,
            'alpha_miner': self._alpha_miner,
            'heuristic_miner': self._heuristic_miner,
            'ocpn_discovery': self._ocpn_discovery
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
        Generate OCPN models from complete OCEL data using multiple algorithms.
        
        Args:
            ocel_file_path: Path to complete OCEL JSON file
            algorithms: List of discovery algorithms to use
            
        Returns:
            Dictionary with generated models information
        """
        if algorithms is None:
            algorithms = ['inductive_miner', 'alpha_miner', 'heuristic_miner', 'ocpn_discovery']
        
        logger.info(f"Starting OCPN generation from {ocel_file_path}")
        
        try:
            # Load OCEL data using pm4py
            ocel_data = self._load_ocel_with_pm4py(ocel_file_path)
            if ocel_data is None:
                return {'error': 'Failed to load OCEL data with pm4py'}
            
            logger.info(f"Successfully loaded OCEL data with pm4py")
            
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
    
    def _load_ocel_with_pm4py(self, ocel_file_path: Path):
        """Load OCEL data using pm4py."""
        try:
            import pm4py
            
            # Try OCEL 2.0 reader first
            try:
                ocel = pm4py.read_ocel2_json(str(ocel_file_path))
                logger.info("Successfully loaded OCEL 2.0 data with pm4py")
                return ocel
            except Exception as e1:
                logger.warning(f"OCEL 2.0 reader failed: {e1}")
                
                # Fallback to generic OCEL reader
                try:
                    ocel = pm4py.read_ocel(str(ocel_file_path))
                    logger.info("Successfully loaded OCEL data with generic reader")
                    return ocel
                except Exception as e2:
                    logger.error(f"Generic OCEL reader failed: {e2}")
                    return None
        
        except ImportError:
            logger.error("pm4py not available. Please install: pip install pm4py")
            return None
    
    def _inductive_miner(self, ocel_data) -> Optional[Dict[str, Any]]:
        """Generate OCPN using Inductive Miner algorithm."""
        try:
            import pm4py
            from pm4py.algo.discovery.inductive import algorithm as inductive_miner
            
            # Convert OCEL to event log for inductive miner
            event_log = self._convert_ocel_to_event_log(ocel_data)
            
            # Apply inductive miner
            net, initial_marking, final_marking = inductive_miner.apply(event_log)
            
            # Convert to OCPN format
            return self._convert_petri_net_to_ocpn(net, initial_marking, final_marking, 'inductive_miner')
            
        except Exception as e:
            logger.error(f"Inductive miner failed: {e}")
            return None
    
    def _alpha_miner(self, ocel_data) -> Optional[Dict[str, Any]]:
        """Generate OCPN using Alpha Miner algorithm."""
        try:
            import pm4py
            from pm4py.algo.discovery.alpha import algorithm as alpha_miner
            
            # Convert OCEL to event log for alpha miner
            event_log = self._convert_ocel_to_event_log(ocel_data)
            
            # Apply alpha miner
            net, initial_marking, final_marking = alpha_miner.apply(event_log)
            
            # Convert to OCPN format
            return self._convert_petri_net_to_ocpn(net, initial_marking, final_marking, 'alpha_miner')
            
        except Exception as e:
            logger.error(f"Alpha miner failed: {e}")
            return None
    
    def _heuristic_miner(self, ocel_data) -> Optional[Dict[str, Any]]:
        """Generate OCPN using Heuristic Miner algorithm."""
        try:
            import pm4py
            from pm4py.algo.discovery.heuristics import algorithm as heuristic_miner
            
            # Convert OCEL to event log for heuristic miner
            event_log = self._convert_ocel_to_event_log(ocel_data)
            
            # Apply heuristic miner
            net, initial_marking, final_marking = heuristic_miner.apply(event_log)
            
            # Convert to OCPN format
            return self._convert_petri_net_to_ocpn(net, initial_marking, final_marking, 'heuristic_miner')
            
        except Exception as e:
            logger.error(f"Heuristic miner failed: {e}")
            return None
    
    def _ocpn_discovery(self, ocel_data) -> Optional[Dict[str, Any]]:
        """Generate OCPN using Object-Centric Petri Net discovery."""
        try:
            import pm4py
            
            # Try OCEL-specific OCPN discovery
            try:
                from pm4py.algo.discovery.ocel.ocpn import algorithm as ocpn_discovery
                ocpn = ocpn_discovery.apply(ocel_data)
                return self._convert_ocpn_to_tables(ocpn, 'ocpn_discovery')
            except Exception as e1:
                logger.warning(f"OCPN discovery failed: {e1}")
                
                # Fallback to simple model discovery
                try:
                    from pm4py.discovery import discover_petri_net_alpha
                    events = ocel_data.get("events", []) if isinstance(ocel_data, dict) else []
                    log = []
                    for ev in events:
                        log.append([ev.get("type", "event")])
                    net, im, fm = discover_petri_net_alpha(log)
                    return self._convert_petri_net_to_ocpn(net, im, fm, 'ocpn_discovery')
                except Exception as e2:
                    logger.error(f"Fallback OCPN discovery failed: {e2}")
                    return None
            
        except Exception as e:
            logger.error(f"OCPN discovery failed: {e}")
            return None
    
    def _convert_ocel_to_event_log(self, ocel_data):
        """Convert OCEL data to event log format for traditional algorithms."""
        try:
            import pm4py
            
            # Extract events and create event log
            events = []
            if isinstance(ocel_data, dict):
                for event in ocel_data.get('events', []):
                    events.append({
                        'case:concept:name': event.get('id', ''),
                        'concept:name': event.get('type', ''),
                        'time:timestamp': event.get('time', ''),
                        'org:resource': 'system'
                    })
            
            # Convert to pm4py event log
            df = pd.DataFrame(events)
            if not df.empty:
                return pm4py.convert_to_event_log(df)
            else:
                return None
                
        except Exception as e:
            logger.error(f"OCEL to event log conversion failed: {e}")
            return None
    
    def _convert_petri_net_to_ocpn(self, net, initial_marking, final_marking, algorithm: str) -> Dict[str, Any]:
        """Convert Petri net to OCPN table format."""
        try:
            places = []
            transitions = []
            arcs = []
            
            # Extract places
            for place in net.places:
                places.append({
                    'place_id': str(place),
                    'label': str(place),
                    'type': 'place'
                })
            
            # Extract transitions
            for transition in net.transitions:
                transitions.append({
                    'transition_id': str(transition),
                    'label': str(transition),
                    'type': 'transition',
                    'invisible': False
                })
            
            # Extract arcs
            for arc in net.arcs:
                arcs.append({
                    'arc_id': f"{arc.source}_{arc.target}",
                    'src_type': 'place' if arc.source in net.places else 'transition',
                    'src_id': str(arc.source),
                    'dst_type': 'place' if arc.target in net.places else 'transition',
                    'dst_id': str(arc.target),
                    'weight': 1
                })
            
            return {
                'places': places,
                'transitions': transitions,
                'arcs': arcs,
                'algorithm': algorithm,
                'initial_marking': dict(initial_marking),
                'final_marking': dict(final_marking)
            }
            
        except Exception as e:
            logger.error(f"Petri net to OCPN conversion failed: {e}")
            return None
    
    def _convert_ocpn_to_tables(self, ocpn, algorithm: str) -> Dict[str, Any]:
        """Convert OCPN object to table format."""
        try:
            places = []
            transitions = []
            arcs = []
            
            # Extract places from OCPN
            if hasattr(ocpn, 'places'):
                for place in ocpn.places:
                    places.append({
                        'place_id': str(place),
                        'label': str(place),
                        'type': 'place'
                    })
            
            # Extract transitions from OCPN
            if hasattr(ocpn, 'transitions'):
                for transition in ocpn.transitions:
                    transitions.append({
                        'transition_id': str(transition),
                        'label': str(transition),
                        'type': 'transition',
                        'invisible': False
                    })
            
            # Extract arcs from OCPN
            if hasattr(ocpn, 'arcs'):
                for arc in ocpn.arcs:
                    arcs.append({
                        'arc_id': f"{arc.source}_{arc.target}",
                        'src_type': 'place' if hasattr(arc, 'source') else 'transition',
                        'src_id': str(arc.source) if hasattr(arc, 'source') else '',
                        'dst_type': 'place' if hasattr(arc, 'target') else 'transition',
                        'dst_id': str(arc.target) if hasattr(arc, 'target') else '',
                        'weight': 1
                    })
            
            return {
                'places': places,
                'transitions': transitions,
                'arcs': arcs,
                'algorithm': algorithm
            }
            
        except Exception as e:
            logger.error(f"OCPN to tables conversion failed: {e}")
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
        print("Usage: python complete_ocpn_generator.py <ocel_file> <catalog_config>")
        print("Example: python complete_ocpn_generator.py data/purchase_to_pay_ocel_complete.json lakehouse/catalogs/local.yaml")
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
    generator = CompleteOCPNGenerator(catalog_config_path)
    
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
            print(f"  ✅ {algorithm}: Model ID {model_info.get('model_id', 'N/A')}")
            print(f"     Places: {model_info.get('places_count', 0)}")
            print(f"     Transitions: {model_info.get('transitions_count', 0)}")
            print(f"     Arcs: {model_info.get('arcs_count', 0)}")
        else:
            print(f"  ❌ {algorithm}: {model_info.get('error', 'Unknown error')}")
    
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
