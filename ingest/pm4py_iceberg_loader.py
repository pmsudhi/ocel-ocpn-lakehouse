"""
pm4py to Iceberg bridge for loading OCEL and OCPN data
Converts pm4py objects to Iceberg tables
"""

import pm4py
import daft
from pyiceberg.catalog import Catalog
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime
import uuid

class PM4PyIcebergLoader:
    """
    Load pm4py OCEL and OCPN objects into Iceberg tables
    """
    
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
    
    def load_ocel_to_iceberg(self, ocel, model_id: str = None):
        """
        Convert pm4py OCEL object to Iceberg tables
        
        pm4py OCEL structure:
        - ocel.events: DataFrame with event data
        - ocel.objects: DataFrame with object data
        - ocel.relations: DataFrame with event-object relationships
        
        Args:
            ocel: pm4py OCEL object
            model_id: Optional model identifier
        """
        if model_id is None:
            model_id = f"model_{uuid.uuid4().hex[:8]}"
        
        print(f"Loading OCEL to Iceberg with model_id: {model_id}")
        
        # Extract DataFrames from pm4py OCEL
        events_df = ocel.events.copy()
        objects_df = ocel.objects.copy()
        relations_df = ocel.relations.copy()
        
        # Add model_id to all DataFrames
        events_df['model_id'] = model_id
        objects_df['model_id'] = model_id
        relations_df['model_id'] = model_id
        
        # Add metadata
        events_df['loaded_at'] = datetime.now()
        objects_df['loaded_at'] = datetime.now()
        relations_df['loaded_at'] = datetime.now()
        
        # Convert to Daft DataFrames
        events_daft = daft.from_pandas(events_df)
        objects_daft = daft.from_pandas(objects_df)
        relations_daft = daft.from_pandas(relations_df)
        
        try:
            # Write to Iceberg tables
            events_table = self.catalog.load_table('ocel.events')
            events_daft.write_iceberg(events_table, mode="append")
            print(f"✓ Loaded {len(events_df)} events to ocel.events")
            
            objects_table = self.catalog.load_table('ocel.objects')
            objects_daft.write_iceberg(objects_table, mode="append")
            print(f"✓ Loaded {len(objects_df)} objects to ocel.objects")
            
            relations_table = self.catalog.load_table('ocel.event_objects')
            relations_daft.write_iceberg(relations_table, mode="append")
            print(f"✓ Loaded {len(relations_df)} relations to ocel.event_objects")
            
        except Exception as e:
            print(f"Error loading OCEL to Iceberg: {e}")
            raise
    
    def load_ocpn_to_iceberg(self, ocpn, initial_marking, final_marking, model_id: str = None):
        """
        Convert pm4py OCPN (Petri Net) to Iceberg OCPN tables
        
        pm4py Petri Net structure:
        - net.places: Set of places
        - net.transitions: Set of transitions
        - net.arcs: Set of arcs
        
        Args:
            ocpn: pm4py Petri Net object
            initial_marking: Initial marking
            final_marking: Final marking
            model_id: Optional model identifier
        """
        if model_id is None:
            model_id = f"model_{uuid.uuid4().hex[:8]}"
        
        print(f"Loading OCPN to Iceberg with model_id: {model_id}")
        
        # Extract OCPN components
        places_data = self._extract_places(ocpn, model_id)
        transitions_data = self._extract_transitions(ocpn, model_id)
        arcs_data = self._extract_arcs(ocpn, model_id)
        markings_data = self._extract_markings(initial_marking, final_marking, model_id)
        
        try:
            # Write to Iceberg OCPN tables
            places_table = self.catalog.load_table('ocpn.places')
            daft.from_pylist(places_data).write_iceberg(places_table, mode="append")
            print(f"✓ Loaded {len(places_data)} places to ocpn.places")
            
            transitions_table = self.catalog.load_table('ocpn.transitions')
            daft.from_pylist(transitions_data).write_iceberg(transitions_table, mode="append")
            print(f"✓ Loaded {len(transitions_data)} transitions to ocpn.transitions")
            
            arcs_table = self.catalog.load_table('ocpn.arcs')
            daft.from_pylist(arcs_data).write_iceberg(arcs_table, mode="append")
            print(f"✓ Loaded {len(arcs_data)} arcs to ocpn.arcs")
            
            markings_table = self.catalog.load_table('ocpn.markings')
            daft.from_pylist(markings_data).write_iceberg(markings_table, mode="append")
            print(f"✓ Loaded {len(markings_data)} markings to ocpn.markings")
            
        except Exception as e:
            print(f"Error loading OCPN to Iceberg: {e}")
            raise
    
    def _extract_places(self, ocpn, model_id: str) -> List[Dict[str, Any]]:
        """
        Extract places from pm4py Petri Net
        
        Args:
            ocpn: pm4py Petri Net object
            model_id: Model identifier
            
        Returns:
            List of place dictionaries
        """
        places_data = []
        
        for place in ocpn.places:
            place_data = {
                'model_id': model_id,
                'place_id': str(place.name) if hasattr(place, 'name') else str(id(place)),
                'place_name': str(place.name) if hasattr(place, 'name') else f"place_{id(place)}",
                'place_type': 'place',
                'properties': self._extract_place_properties(place),
                'created_at': datetime.now()
            }
            places_data.append(place_data)
        
        return places_data
    
    def _extract_transitions(self, ocpn, model_id: str) -> List[Dict[str, Any]]:
        """
        Extract transitions from pm4py Petri Net
        
        Args:
            ocpn: pm4py Petri Net object
            model_id: Model identifier
            
        Returns:
            List of transition dictionaries
        """
        transitions_data = []
        
        for transition in ocpn.transitions:
            transition_data = {
                'model_id': model_id,
                'transition_id': str(transition.name) if hasattr(transition, 'name') else str(id(transition)),
                'transition_name': str(transition.name) if hasattr(transition, 'name') else f"transition_{id(transition)}",
                'transition_type': 'transition',
                'properties': self._extract_transition_properties(transition),
                'created_at': datetime.now()
            }
            transitions_data.append(transition_data)
        
        return transitions_data
    
    def _extract_arcs(self, ocpn, model_id: str) -> List[Dict[str, Any]]:
        """
        Extract arcs from pm4py Petri Net
        
        Args:
            ocpn: pm4py Petri Net object
            model_id: Model identifier
            
        Returns:
            List of arc dictionaries
        """
        arcs_data = []
        
        for arc in ocpn.arcs:
            arc_data = {
                'model_id': model_id,
                'arc_id': f"arc_{id(arc)}",
                'source_id': str(arc.source.name) if hasattr(arc.source, 'name') else str(id(arc.source)),
                'target_id': str(arc.target.name) if hasattr(arc.target, 'name') else str(id(arc.target)),
                'arc_type': 'arc',
                'weight': getattr(arc, 'weight', 1),
                'properties': self._extract_arc_properties(arc),
                'created_at': datetime.now()
            }
            arcs_data.append(arc_data)
        
        return arcs_data
    
    def _extract_markings(self, initial_marking, final_marking, model_id: str) -> List[Dict[str, Any]]:
        """
        Extract markings from pm4py Petri Net
        
        Args:
            initial_marking: Initial marking
            final_marking: Final marking
            model_id: Model identifier
            
        Returns:
            List of marking dictionaries
        """
        markings_data = []
        
        # Initial marking
        initial_marking_data = {
            'model_id': model_id,
            'marking_id': f"{model_id}_initial",
            'marking_type': 'initial',
            'marking': self._serialize_marking(initial_marking),
            'created_at': datetime.now()
        }
        markings_data.append(initial_marking_data)
        
        # Final marking
        final_marking_data = {
            'model_id': model_id,
            'marking_id': f"{model_id}_final",
            'marking_type': 'final',
            'marking': self._serialize_marking(final_marking),
            'created_at': datetime.now()
        }
        markings_data.append(final_marking_data)
        
        return markings_data
    
    def _extract_place_properties(self, place) -> Dict[str, Any]:
        """Extract properties from a place"""
        properties = {}
        
        if hasattr(place, 'properties'):
            properties.update(place.properties)
        
        return properties
    
    def _extract_transition_properties(self, transition) -> Dict[str, Any]:
        """Extract properties from a transition"""
        properties = {}
        
        if hasattr(transition, 'properties'):
            properties.update(transition.properties)
        
        return properties
    
    def _extract_arc_properties(self, arc) -> Dict[str, Any]:
        """Extract properties from an arc"""
        properties = {}
        
        if hasattr(arc, 'properties'):
            properties.update(arc.properties)
        
        return properties
    
    def _serialize_marking(self, marking) -> Dict[str, Any]:
        """Serialize marking to dictionary"""
        if isinstance(marking, dict):
            return {str(k): v for k, v in marking.items()}
        else:
            return {}
    
    def load_complete_pipeline(self, ocel, ocpn, initial_marking, final_marking, model_id: str = None):
        """
        Load both OCEL and OCPN data to Iceberg
        
        Args:
            ocel: pm4py OCEL object
            ocpn: pm4py Petri Net object
            initial_marking: Initial marking
            final_marking: Final marking
            model_id: Optional model identifier
        """
        if model_id is None:
            model_id = f"model_{uuid.uuid4().hex[:8]}"
        
        print(f"Loading complete pipeline to Iceberg with model_id: {model_id}")
        
        # Load OCEL data
        self.load_ocel_to_iceberg(ocel, model_id)
        
        # Load OCPN data
        self.load_ocpn_to_iceberg(ocpn, initial_marking, final_marking, model_id)
        
        print(f"✓ Complete pipeline loaded to Iceberg")
    
    def get_model_summary(self, model_id: str) -> Dict[str, Any]:
        """
        Get summary of loaded model data
        
        Args:
            model_id: Model identifier
            
        Returns:
            Dictionary with model summary
        """
        try:
            # Count events
            events_table = self.catalog.load_table('ocel.events')
            events_df = daft.read_iceberg(events_table).filter(
                daft.col("model_id") == model_id
            ).to_pandas()
            
            # Count objects
            objects_table = self.catalog.load_table('ocel.objects')
            objects_df = daft.read_iceberg(objects_table).filter(
                daft.col("model_id") == model_id
            ).to_pandas()
            
            # Count places and transitions
            places_table = self.catalog.load_table('ocpn.places')
            places_df = daft.read_iceberg(places_table).filter(
                daft.col("model_id") == model_id
            ).to_pandas()
            
            transitions_table = self.catalog.load_table('ocpn.transitions')
            transitions_df = daft.read_iceberg(transitions_table).filter(
                daft.col("model_id") == model_id
            ).to_pandas()
            
            return {
                'model_id': model_id,
                'events_count': len(events_df),
                'objects_count': len(objects_df),
                'places_count': len(places_df),
                'transitions_count': len(transitions_df),
                'loaded_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"Error getting model summary: {e}")
            return {'error': str(e)}
