"""
pm4py analytics wrapper for Iceberg data
Provides pm4py analytics functions for OCEL data loaded from Iceberg
"""

import pm4py
from pm4py.objects.ocel.obj import OCEL
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
import daft
from pyiceberg.catalog import Catalog

class PM4PyAnalyticsWrapper:
    """
    Wrapper around pm4py analytics functions
    Works with OCEL loaded from Iceberg
    """
    
    def __init__(self, catalog: Catalog, model_id: str = None):
        self.catalog = catalog
        self.model_id = model_id
        self._ocel_cache = None
    
    def _load_ocel_from_iceberg(self) -> OCEL:
        """
        Load OCEL from Iceberg tables and convert to pm4py format
        
        Returns:
            pm4py OCEL object
        """
        if self._ocel_cache is not None:
            return self._ocel_cache
        
        try:
            # Load events from Iceberg
            events_table = self.catalog.load_table('ocel.events')
            events_df = daft.read_iceberg(events_table)
            
            if self.model_id:
                events_df = events_df.filter(daft.col("model_id") == self.model_id)
            
            events_df = events_df.to_pandas()
            
            # Load objects from Iceberg
            objects_table = self.catalog.load_table('ocel.objects')
            objects_df = daft.read_iceberg(objects_table)
            
            if self.model_id:
                objects_df = objects_df.filter(daft.col("model_id") == self.model_id)
            
            objects_df = objects_df.to_pandas()
            
            # Load relations from Iceberg
            relations_table = self.catalog.load_table('ocel.event_objects')
            relations_df = daft.read_iceberg(relations_table)
            
            if self.model_id:
                relations_df = relations_df.filter(daft.col("model_id") == self.model_id)
            
            relations_df = relations_df.to_pandas()
            
            # Create pm4py OCEL object
            ocel = OCEL()
            ocel.events = events_df
            ocel.objects = objects_df
            ocel.relations = relations_df
            
            # Cache the OCEL object
            self._ocel_cache = ocel
            
            return ocel
            
        except Exception as e:
            print(f"Error loading OCEL from Iceberg: {e}")
            raise
    
    def get_process_variants(self, top_n: int = 10) -> List[Tuple[str, int]]:
        """
        Get process variants using pm4py's variant analysis
        
        Args:
            top_n: Number of top variants to return
            
        Returns:
            List of (variant, frequency) tuples
        """
        ocel = self._load_ocel_from_iceberg()
        
        try:
            variants = pm4py.ocel_discover_variants(ocel)
            sorted_variants = sorted(variants.items(), key=lambda x: x[1], reverse=True)
            return sorted_variants[:top_n]
        except Exception as e:
            print(f"Error getting process variants: {e}")
            return []
    
    def get_directly_follows_graph(self) -> Dict[Tuple[str, str], int]:
        """
        Get directly follows graph using pm4py's DFG discovery
        
        Returns:
            Dictionary mapping (from_activity, to_activity) to frequency
        """
        ocel = self._load_ocel_from_iceberg()
        
        try:
            dfg = pm4py.ocel_discover_dfg(ocel)
            return dfg
        except Exception as e:
            print(f"Error getting DFG: {e}")
            return {}
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get performance metrics using pm4py's performance analysis
        
        Returns:
            Dictionary with performance metrics
        """
        ocel = self._load_ocel_from_iceberg()
        
        try:
            # Get performance spectrum
            performance_spectrum = pm4py.ocel_get_performance_spectrum(ocel)
            
            # Get activity performance
            activity_performance = pm4py.ocel_get_activity_performance(ocel)
            
            # Get object performance
            object_performance = pm4py.ocel_get_object_performance(ocel)
            
            return {
                'performance_spectrum': performance_spectrum,
                'activity_performance': activity_performance,
                'object_performance': object_performance
            }
        except Exception as e:
            print(f"Error getting performance metrics: {e}")
            return {}
    
    def check_conformance(self, ocpn) -> Dict[str, Any]:
        """
        Check conformance using pm4py's conformance checking
        
        Args:
            ocpn: pm4py Petri Net object
            
        Returns:
            Dictionary with conformance results
        """
        ocel = self._load_ocel_from_iceberg()
        
        try:
            # Token-based replay conformance
            conformance = pm4py.conformance_diagnostics_token_based_replay(ocel, ocpn)
            
            # Alignment-based conformance
            alignments = pm4py.conformance_diagnostics_alignments(ocel, ocpn)
            
            return {
                'token_based_replay': conformance,
                'alignments': alignments
            }
        except Exception as e:
            print(f"Error checking conformance: {e}")
            return {}
    
    def get_object_centric_metrics(self) -> Dict[str, Any]:
        """
        Get object-centric metrics from pm4py
        
        Returns:
            Dictionary with object-centric metrics
        """
        ocel = self._load_ocel_from_iceberg()
        
        try:
            # Object type activities
            object_type_activities = pm4py.ocel_object_type_activities(ocel)
            
            # Object interactions
            object_interactions = pm4py.ocel_objects_interactions_summary(ocel)
            
            # Object lifecycle
            object_lifecycle = pm4py.ocel_objects_lifecycle(ocel)
            
            # Object-centric performance
            object_performance = pm4py.ocel_get_object_performance(ocel)
            
            return {
                'object_type_activities': object_type_activities,
                'object_interactions': object_interactions,
                'object_lifecycle': object_lifecycle,
                'object_performance': object_performance
            }
        except Exception as e:
            print(f"Error getting object-centric metrics: {e}")
            return {}
    
    def get_activity_statistics(self) -> Dict[str, Any]:
        """
        Get activity statistics
        
        Returns:
            Dictionary with activity statistics
        """
        ocel = self._load_ocel_from_iceberg()
        
        try:
            # Activity frequency
            activity_frequency = pm4py.ocel_get_activity_frequency(ocel)
            
            # Activity performance
            activity_performance = pm4py.ocel_get_activity_performance(ocel)
            
            # Activity co-occurrence
            activity_cooccurrence = pm4py.ocel_get_activity_cooccurrence(ocel)
            
            return {
                'activity_frequency': activity_frequency,
                'activity_performance': activity_performance,
                'activity_cooccurrence': activity_cooccurrence
            }
        except Exception as e:
            print(f"Error getting activity statistics: {e}")
            return {}
    
    def get_case_statistics(self) -> Dict[str, Any]:
        """
        Get case-level statistics
        
        Returns:
            Dictionary with case statistics
        """
        ocel = self._load_ocel_from_iceberg()
        
        try:
            # Case duration
            case_duration = pm4py.ocel_get_case_duration(ocel)
            
            # Case frequency
            case_frequency = pm4py.ocel_get_case_frequency(ocel)
            
            # Case performance
            case_performance = pm4py.ocel_get_case_performance(ocel)
            
            return {
                'case_duration': case_duration,
                'case_frequency': case_frequency,
                'case_performance': case_performance
            }
        except Exception as e:
            print(f"Error getting case statistics: {e}")
            return {}
    
    def get_resource_statistics(self) -> Dict[str, Any]:
        """
        Get resource statistics
        
        Returns:
            Dictionary with resource statistics
        """
        ocel = self._load_ocel_from_iceberg()
        
        try:
            # Resource frequency
            resource_frequency = pm4py.ocel_get_resource_frequency(ocel)
            
            # Resource performance
            resource_performance = pm4py.ocel_get_resource_performance(ocel)
            
            # Resource co-occurrence
            resource_cooccurrence = pm4py.ocel_get_resource_cooccurrence(ocel)
            
            return {
                'resource_frequency': resource_frequency,
                'resource_performance': resource_performance,
                'resource_cooccurrence': resource_cooccurrence
            }
        except Exception as e:
            print(f"Error getting resource statistics: {e}")
            return {}
    
    def get_process_discovery_metrics(self) -> Dict[str, Any]:
        """
        Get process discovery metrics
        
        Returns:
            Dictionary with process discovery metrics
        """
        ocel = self._load_ocel_from_iceberg()
        
        try:
            # Process discovery metrics
            discovery_metrics = pm4py.ocel_get_process_discovery_metrics(ocel)
            
            # Process complexity
            complexity_metrics = pm4py.ocel_get_process_complexity(ocel)
            
            # Process quality
            quality_metrics = pm4py.ocel_get_process_quality(ocel)
            
            return {
                'discovery_metrics': discovery_metrics,
                'complexity_metrics': complexity_metrics,
                'quality_metrics': quality_metrics
            }
        except Exception as e:
            print(f"Error getting process discovery metrics: {e}")
            return {}
    
    def get_comprehensive_analysis(self) -> Dict[str, Any]:
        """
        Get comprehensive analysis combining all metrics
        
        Returns:
            Dictionary with comprehensive analysis
        """
        print("🔍 Running comprehensive process mining analysis...")
        
        analysis = {
            'timestamp': pd.Timestamp.now().isoformat(),
            'model_id': self.model_id,
            'variants': self.get_process_variants(),
            'dfg': self.get_directly_follows_graph(),
            'performance': self.get_performance_metrics(),
            'object_centric': self.get_object_centric_metrics(),
            'activity_stats': self.get_activity_statistics(),
            'case_stats': self.get_case_statistics(),
            'resource_stats': self.get_resource_statistics(),
            'discovery_metrics': self.get_process_discovery_metrics()
        }
        
        print("✓ Comprehensive analysis completed")
        return analysis
    
    def clear_cache(self):
        """Clear the OCEL cache"""
        self._ocel_cache = None
