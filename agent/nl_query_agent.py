#!/usr/bin/env python3
"""
Natural Language Query Agent for Process Mining
Translates natural language questions to ProcessMiningQueryEngine calls
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import json
import re

from .process_mining_queries import ProcessMiningQueryEngine, load_catalog_from_yaml

# Fix Unicode display issues on Windows
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"


class ProcessMiningAgent:
    """Natural language interface to process mining queries."""
    
    def __init__(self, query_engine: ProcessMiningQueryEngine, llm_config: Optional[Dict] = None):
        self.query_engine = query_engine
        self.llm_config = llm_config or self._get_default_llm_config()
        self.query_history = []
        self.intent_patterns = self._load_intent_patterns()
    
    def _get_default_llm_config(self) -> Dict[str, Any]:
        """Get default LLM configuration."""
        return {
            'provider': 'openai',  # or 'anthropic', 'local'
            'model': 'gpt-4',
            'temperature': 0.1,
            'max_tokens': 2000,
            'api_key': None  # Should be set via environment variable
        }
    
    def _load_intent_patterns(self) -> Dict[str, List[str]]:
        """Load intent recognition patterns."""
        return {
            'process_variants': [
                r'what are the most common process variants',
                r'show me process variants',
                r'what are the top variants',
                r'most frequent process patterns',
                r'common process flows'
            ],
            'case_duration': [
                r'how long do cases take',
                r'case duration distribution',
                r'average case duration',
                r'how long are processes',
                r'case timing analysis'
            ],
            'similar_cases': [
                r'find similar cases',
                r'cases like this',
                r'similar instances',
                r'comparable processes',
                r'find cases similar to'
            ],
            'activity_frequencies': [
                r'activity frequencies',
                r'most common activities',
                r'which activities happen most',
                r'activity distribution',
                r'activity usage'
            ],
            'bottlenecks': [
                r'identify bottlenecks',
                r'find bottlenecks',
                r'which activities take longest',
                r'slow activities',
                r'process bottlenecks',
                r'performance issues'
            ],
            'resource_utilization': [
                r'resource utilization',
                r'resource workload',
                r'who is busiest',
                r'resource analysis',
                r'workload distribution'
            ],
            'object_lifecycle': [
                r'object lifecycle',
                r'lifecycle of object',
                r'object history',
                r'object events',
                r'object timeline'
            ],
            'object_interactions': [
                r'object interactions',
                r'how objects interact',
                r'object relationships',
                r'object connections'
            ],
            'conformance': [
                r'conformance check',
                r'does this conform',
                r'conformance analysis',
                r'process compliance',
                r'check conformance'
            ],
            'predictions': [
                r'predict next activity',
                r'what will happen next',
                r'next step prediction',
                r'forecast next activity'
            ],
            'case_outcome': [
                r'will this case complete',
                r'case outcome prediction',
                r'completion probability',
                r'case success prediction'
            ]
        }
    
    def ask(self, question: str) -> Dict[str, Any]:
        """
        Process natural language questions and return results.
        
        Args:
            question: Natural language question
            
        Returns:
            Dictionary with query results and metadata
        """
        print(f"Processing question: {question}")
        
        try:
            # Parse intent and extract parameters
            intent, parameters = self._parse_intent(question)
            
            if not intent:
                return {
                    'error': 'Could not understand the question. Please try rephrasing.',
                    'suggestions': self._get_suggestion_questions()
                }
            
            # Execute query based on intent
            result = self._execute_query(intent, parameters)
            
            # Format results for human consumption
            formatted_result = self._format_results(result, question, intent)
            
            # Store in query history
            self.query_history.append({
                'question': question,
                'intent': intent,
                'parameters': parameters,
                'timestamp': datetime.utcnow().isoformat(),
                'result_summary': self._get_result_summary(result)
            })
            
            return formatted_result
            
        except Exception as e:
            return {
                'error': f'Failed to process question: {e}',
                'question': question
            }
    
    def _parse_intent(self, question: str) -> Tuple[Optional[str], Dict[str, Any]]:
        """Parse question to identify intent and extract parameters."""
        question_lower = question.lower()
        
        # Check against intent patterns
        for intent, patterns in self.intent_patterns.items():
            for pattern in patterns:
                if re.search(pattern, question_lower):
                    parameters = self._extract_parameters(question, intent)
                    return intent, parameters
        
        # Try LLM-based parsing if available
        if self.llm_config.get('provider') and self.llm_config.get('api_key'):
            return self._llm_parse_intent(question)
        
        return None, {}
    
    def _extract_parameters(self, question: str, intent: str) -> Dict[str, Any]:
        """Extract parameters from question based on intent."""
        parameters = {}
        
        if intent == 'process_variants':
            # Extract top_n and min_frequency
            top_match = re.search(r'top (\d+)', question.lower())
            if top_match:
                parameters['top_n'] = int(top_match.group(1))
            else:
                parameters['top_n'] = 10
            
            min_match = re.search(r'minimum frequency (\d+)', question.lower())
            if min_match:
                parameters['min_frequency'] = int(min_match.group(1))
            else:
                parameters['min_frequency'] = 1
        
        elif intent == 'similar_cases':
            # Extract instance ID
            instance_match = re.search(r'instance[_\s]*id[:\s]*([a-zA-Z0-9_-]+)', question, re.IGNORECASE)
            if instance_match:
                parameters['instance_id'] = instance_match.group(1)
            else:
                # Try to find any ID-like pattern
                id_match = re.search(r'([a-zA-Z0-9_-]{8,})', question)
                if id_match:
                    parameters['instance_id'] = id_match.group(1)
        
        elif intent == 'object_lifecycle':
            # Extract object ID
            object_match = re.search(r'object[_\s]*id[:\s]*([a-zA-Z0-9_-]+)', question, re.IGNORECASE)
            if object_match:
                parameters['object_id'] = object_match.group(1)
            else:
                # Try to find any ID-like pattern
                id_match = re.search(r'([a-zA-Z0-9_-]{8,})', question)
                if id_match:
                    parameters['object_id'] = id_match.group(1)
        
        elif intent == 'conformance':
            # Extract instance ID and model ID
            instance_match = re.search(r'instance[_\s]*id[:\s]*([a-zA-Z0-9_-]+)', question, re.IGNORECASE)
            model_match = re.search(r'model[_\s]*id[:\s]*([a-zA-Z0-9_-]+)', question, re.IGNORECASE)
            
            if instance_match:
                parameters['instance_id'] = instance_match.group(1)
            if model_match:
                parameters['model_id'] = model_match.group(1)
        
        elif intent == 'predictions':
            # Extract instance ID
            instance_match = re.search(r'instance[_\s]*id[:\s]*([a-zA-Z0-9_-]+)', question, re.IGNORECASE)
            if instance_match:
                parameters['instance_id'] = instance_match.group(1)
            else:
                id_match = re.search(r'([a-zA-Z0-9_-]{8,})', question)
                if id_match:
                    parameters['instance_id'] = id_match.group(1)
        
        elif intent == 'case_outcome':
            # Extract instance ID
            instance_match = re.search(r'instance[_\s]*id[:\s]*([a-zA-Z0-9_-]+)', question, re.IGNORECASE)
            if instance_match:
                parameters['instance_id'] = instance_match.group(1)
            else:
                id_match = re.search(r'([a-zA-Z0-9_-]{8,})', question)
                if id_match:
                    parameters['instance_id'] = id_match.group(1)
        
        return parameters
    
    def _llm_parse_intent(self, question: str) -> Tuple[Optional[str], Dict[str, Any]]:
        """Use LLM to parse intent and parameters."""
        # This would integrate with OpenAI, Anthropic, or local LLM
        # For now, return None to fall back to pattern matching
        return None, {}
    
    def _execute_query(self, intent: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Execute query based on intent and parameters."""
        try:
            if intent == 'process_variants':
                return self.query_engine.get_process_variants(
                    top_n=parameters.get('top_n', 10),
                    min_frequency=parameters.get('min_frequency', 1),
                    instance_type=parameters.get('instance_type')
                )
            
            elif intent == 'case_duration':
                return self.query_engine.get_case_duration_distribution(
                    instance_type=parameters.get('instance_type')
                )
            
            elif intent == 'similar_cases':
                if 'instance_id' not in parameters:
                    return {'error': 'Instance ID required for similar cases query'}
                return self.query_engine.find_similar_cases(
                    instance_id=parameters['instance_id'],
                    similarity_metric=parameters.get('similarity_metric', 'levenshtein'),
                    max_results=parameters.get('max_results', 10)
                )
            
            elif intent == 'activity_frequencies':
                return self.query_engine.get_activity_frequencies(
                    start_date=parameters.get('start_date'),
                    end_date=parameters.get('end_date')
                )
            
            elif intent == 'bottlenecks':
                return self.query_engine.identify_bottlenecks(
                    threshold_percentile=parameters.get('threshold_percentile', 90)
                )
            
            elif intent == 'resource_utilization':
                return self.query_engine.get_resource_utilization(
                    resource_attribute=parameters.get('resource_attribute', 'user')
                )
            
            elif intent == 'object_lifecycle':
                if 'object_id' not in parameters:
                    return {'error': 'Object ID required for lifecycle query'}
                return self.query_engine.get_object_lifecycle(
                    object_id=parameters['object_id']
                )
            
            elif intent == 'object_interactions':
                return self.query_engine.get_object_interaction_graph()
            
            elif intent == 'conformance':
                if 'instance_id' not in parameters or 'model_id' not in parameters:
                    return {'error': 'Instance ID and Model ID required for conformance check'}
                return self.query_engine.check_conformance(
                    instance_id=parameters['instance_id'],
                    model_id=parameters['model_id']
                )
            
            elif intent == 'predictions':
                if 'instance_id' not in parameters:
                    return {'error': 'Instance ID required for prediction query'}
                return self.query_engine.predict_next_activity(
                    instance_id=parameters['instance_id'],
                    ml_model=parameters.get('ml_model')
                )
            
            elif intent == 'case_outcome':
                if 'instance_id' not in parameters:
                    return {'error': 'Instance ID required for outcome prediction'}
                return self.query_engine.predict_case_outcome(
                    instance_id=parameters['instance_id']
                )
            
            else:
                return {'error': f'Unknown intent: {intent}'}
                
        except Exception as e:
            return {'error': f'Query execution failed: {e}'}
    
    def _format_results(self, result: Dict[str, Any], question: str, intent: str) -> Dict[str, Any]:
        """Format query results for human consumption."""
        if 'error' in result:
            return {
                'question': question,
                'intent': intent,
                'error': result['error'],
                'timestamp': datetime.utcnow().isoformat()
            }
        
        # Format based on intent
        if intent == 'process_variants':
            return self._format_variants_result(result, question)
        elif intent == 'case_duration':
            return self._format_duration_result(result, question)
        elif intent == 'similar_cases':
            return self._format_similar_cases_result(result, question)
        elif intent == 'activity_frequencies':
            return self._format_activity_frequencies_result(result, question)
        elif intent == 'bottlenecks':
            return self._format_bottlenecks_result(result, question)
        elif intent == 'resource_utilization':
            return self._format_resource_utilization_result(result, question)
        elif intent == 'object_lifecycle':
            return self._format_lifecycle_result(result, question)
        elif intent == 'object_interactions':
            return self._format_interactions_result(result, question)
        elif intent == 'conformance':
            return self._format_conformance_result(result, question)
        elif intent == 'predictions':
            return self._format_predictions_result(result, question)
        elif intent == 'case_outcome':
            return self._format_outcome_result(result, question)
        else:
            return {
                'question': question,
                'intent': intent,
                'raw_result': result,
                'timestamp': datetime.utcnow().isoformat()
            }
    
    def _format_variants_result(self, result: Dict[str, Any], question: str) -> Dict[str, Any]:
        """Format process variants result."""
        variants = result.get('variants', [])
        
        if not variants:
            return {
                'question': question,
                'answer': 'No process variants found.',
                'timestamp': datetime.utcnow().isoformat()
            }
        
        # Create summary
        total_variants = result.get('total_variants', len(variants))
        most_common = variants[0] if variants else None
        
        answer = f"Found {total_variants} process variants. "
        if most_common:
            answer += f"The most common variant is '{most_common['pattern']}' with {most_common['frequency']} occurrences."
        
        return {
            'question': question,
            'answer': answer,
            'summary': {
                'total_variants': total_variants,
                'most_common_pattern': most_common['pattern'] if most_common else None,
                'most_common_frequency': most_common['frequency'] if most_common else 0
            },
            'top_variants': variants[:5],  # Show top 5
            'query_time_ms': result.get('query_time_ms'),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _format_duration_result(self, result: Dict[str, Any], question: str) -> Dict[str, Any]:
        """Format case duration result."""
        if 'error' in result:
            return {
                'question': question,
                'answer': f"Could not analyze case durations: {result['error']}",
                'timestamp': datetime.utcnow().isoformat()
            }
        
        total_cases = result.get('total_cases', 0)
        mean_duration = result.get('mean_duration_seconds', 0)
        median_duration = result.get('median_duration_seconds', 0)
        
        # Convert to human-readable format
        mean_hours = mean_duration / 3600
        median_hours = median_duration / 3600
        
        answer = f"Analyzed {total_cases} cases. "
        answer += f"Average duration: {mean_hours:.1f} hours. "
        answer += f"Median duration: {median_hours:.1f} hours."
        
        return {
            'question': question,
            'answer': answer,
            'summary': {
                'total_cases': total_cases,
                'mean_duration_hours': mean_hours,
                'median_duration_hours': median_hours,
                'min_duration_hours': result.get('min_duration_seconds', 0) / 3600,
                'max_duration_hours': result.get('max_duration_seconds', 0) / 3600
            },
            'percentiles': result.get('percentiles', {}),
            'query_time_ms': result.get('query_time_ms'),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _format_similar_cases_result(self, result: Dict[str, Any], question: str) -> Dict[str, Any]:
        """Format similar cases result."""
        if 'error' in result:
            return {
                'question': question,
                'answer': f"Could not find similar cases: {result['error']}",
                'timestamp': datetime.utcnow().isoformat()
            }
        
        similar_cases = result.get('similar_cases', [])
        target_instance = result.get('target_instance', 'Unknown')
        
        if not similar_cases:
            answer = f"No similar cases found for instance {target_instance}."
        else:
            top_similarity = similar_cases[0]['similarity_score'] if similar_cases else 0
            answer = f"Found {len(similar_cases)} similar cases for instance {target_instance}. "
            answer += f"Most similar case has {top_similarity:.1%} similarity."
        
        return {
            'question': question,
            'answer': answer,
            'summary': {
                'target_instance': target_instance,
                'similar_cases_count': len(similar_cases),
                'highest_similarity': similar_cases[0]['similarity_score'] if similar_cases else 0
            },
            'similar_cases': similar_cases[:5],  # Show top 5
            'query_time_ms': result.get('query_time_ms'),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _format_activity_frequencies_result(self, result: Dict[str, Any], question: str) -> Dict[str, Any]:
        """Format activity frequencies result."""
        if 'error' in result:
            return {
                'question': question,
                'answer': f"Could not get activity frequencies: {result['error']}",
                'timestamp': datetime.utcnow().isoformat()
            }
        
        activities = result.get('activities', [])
        total_activities = result.get('total_activities', 0)
        
        if not activities:
            answer = "No activity data available."
        else:
            most_frequent = activities[0] if activities else None
            answer = f"Found {total_activities} activities. "
            if most_frequent:
                answer += f"Most frequent activity is '{most_frequent['activity_name']}' with {most_frequent['total_occurrences']} occurrences."
        
        return {
            'question': question,
            'answer': answer,
            'summary': {
                'total_activities': total_activities,
                'most_frequent_activity': activities[0]['activity_name'] if activities else None,
                'most_frequent_count': activities[0]['total_occurrences'] if activities else 0
            },
            'top_activities': activities[:10],  # Show top 10
            'query_time_ms': result.get('query_time_ms'),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _format_bottlenecks_result(self, result: Dict[str, Any], question: str) -> Dict[str, Any]:
        """Format bottlenecks result."""
        if 'error' in result:
            return {
                'question': question,
                'answer': f"Could not identify bottlenecks: {result['error']}",
                'timestamp': datetime.utcnow().isoformat()
            }
        
        bottlenecks = result.get('bottlenecks', [])
        total_bottlenecks = result.get('total_bottlenecks', 0)
        
        if not bottlenecks:
            answer = "No bottlenecks identified. Process performance looks good!"
        else:
            worst_bottleneck = bottlenecks[0] if bottlenecks else None
            answer = f"Identified {total_bottlenecks} bottlenecks. "
            if worst_bottleneck:
                duration_hours = worst_bottleneck['avg_duration_seconds'] / 3600
                answer += f"Worst bottleneck is '{worst_bottleneck['activity_name']}' with {duration_hours:.1f} hours average duration."
        
        return {
            'question': question,
            'answer': answer,
            'summary': {
                'total_bottlenecks': total_bottlenecks,
                'worst_bottleneck': bottlenecks[0]['activity_name'] if bottlenecks else None,
                'worst_duration_hours': bottlenecks[0]['avg_duration_seconds'] / 3600 if bottlenecks else 0
            },
            'bottlenecks': bottlenecks[:5],  # Show top 5
            'query_time_ms': result.get('query_time_ms'),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _format_resource_utilization_result(self, result: Dict[str, Any], question: str) -> Dict[str, Any]:
        """Format resource utilization result."""
        if 'error' in result:
            return {
                'question': question,
                'answer': f"Could not get resource utilization: {result['error']}",
                'timestamp': datetime.utcnow().isoformat()
            }
        
        resources = result.get('resources', [])
        total_resources = result.get('total_resources', 0)
        
        if not resources:
            answer = "No resource utilization data available."
        else:
            busiest_resource = resources[0] if resources else None
            answer = f"Analyzed {total_resources} resources. "
            if busiest_resource:
                answer += f"Busiest resource is '{busiest_resource['resource_name']}' with {busiest_resource['utilization_score']:.1%} utilization."
        
        return {
            'question': question,
            'answer': answer,
            'summary': {
                'total_resources': total_resources,
                'busiest_resource': resources[0]['resource_name'] if resources else None,
                'highest_utilization': resources[0]['utilization_score'] if resources else 0
            },
            'resources': resources[:10],  # Show top 10
            'query_time_ms': result.get('query_time_ms'),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _format_lifecycle_result(self, result: Dict[str, Any], question: str) -> Dict[str, Any]:
        """Format object lifecycle result."""
        if 'error' in result:
            return {
                'question': question,
                'answer': f"Could not get object lifecycle: {result['error']}",
                'timestamp': datetime.utcnow().isoformat()
            }
        
        object_id = result.get('object_id', 'Unknown')
        lifecycle_events = result.get('lifecycle_events', [])
        total_events = result.get('total_events', 0)
        
        answer = f"Object {object_id} has {total_events} lifecycle events. "
        if lifecycle_events:
            first_event = lifecycle_events[0]
            last_event = lifecycle_events[-1]
            answer += f"First event: {first_event['event_type']} at {first_event['timestamp']}. "
            answer += f"Last event: {last_event['event_type']} at {last_event['timestamp']}."
        
        return {
            'question': question,
            'answer': answer,
            'summary': {
                'object_id': object_id,
                'object_type': result.get('object_type'),
                'total_events': total_events,
                'first_event': lifecycle_events[0]['event_type'] if lifecycle_events else None,
                'last_event': lifecycle_events[-1]['event_type'] if lifecycle_events else None
            },
            'lifecycle_events': lifecycle_events[:10],  # Show first 10
            'query_time_ms': result.get('query_time_ms'),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _format_interactions_result(self, result: Dict[str, Any], question: str) -> Dict[str, Any]:
        """Format object interactions result."""
        if 'error' in result:
            return {
                'question': question,
                'answer': f"Could not get object interactions: {result['error']}",
                'timestamp': datetime.utcnow().isoformat()
            }
        
        interactions = result.get('interactions', [])
        total_interactions = result.get('total_interactions', 0)
        
        if not interactions:
            answer = "No object interactions found."
        else:
            most_common = interactions[0] if interactions else None
            answer = f"Found {total_interactions} object interactions. "
            if most_common:
                answer += f"Most common interaction is {most_common['source_type']} -> {most_common['target_type']} with {most_common['interaction_count']} occurrences."
        
        return {
            'question': question,
            'answer': answer,
            'summary': {
                'total_interactions': total_interactions,
                'most_common_interaction': f"{interactions[0]['source_type']} -> {interactions[0]['target_type']}" if interactions else None,
                'most_common_count': interactions[0]['interaction_count'] if interactions else 0
            },
            'interactions': interactions[:10],  # Show top 10
            'query_time_ms': result.get('query_time_ms'),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _format_conformance_result(self, result: Dict[str, Any], question: str) -> Dict[str, Any]:
        """Format conformance result."""
        if 'error' in result:
            return {
                'question': question,
                'answer': f"Could not check conformance: {result['error']}",
                'timestamp': datetime.utcnow().isoformat()
            }
        
        score = result.get('score', 0)
        fitness = result.get('fitness', 0)
        precision = result.get('precision', 0)
        deviations = result.get('deviations', [])
        
        if score > 0.8:
            answer = f"Instance conforms well to the model (score: {score:.1%}). "
        elif score > 0.5:
            answer = f"Instance partially conforms to the model (score: {score:.1%}). "
        else:
            answer = f"Instance does not conform well to the model (score: {score:.1%}). "
        
        answer += f"Fitness: {fitness:.1%}, Precision: {precision:.1%}. "
        if deviations:
            answer += f"Found {len(deviations)} deviations."
        
        return {
            'question': question,
            'answer': answer,
            'summary': {
                'conformance_score': score,
                'fitness': fitness,
                'precision': precision,
                'deviation_count': len(deviations)
            },
            'deviations': deviations[:5],  # Show first 5
            'query_time_ms': result.get('query_time_ms'),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _format_predictions_result(self, result: Dict[str, Any], question: str) -> Dict[str, Any]:
        """Format predictions result."""
        if 'error' in result:
            return {
                'question': question,
                'answer': f"Could not make prediction: {result['error']}",
                'timestamp': datetime.utcnow().isoformat()
            }
        
        predictions = result.get('predictions', [])
        current_activity = result.get('current_activity', 'Unknown')
        
        if not predictions:
            answer = f"No predictions available for current activity '{current_activity}'."
        else:
            top_prediction = predictions[0]
            answer = f"Based on current activity '{current_activity}', "
            answer += f"most likely next activity is '{top_prediction['next_activity']}' "
            answer += f"with {top_prediction['probability']:.1%} probability."
        
        return {
            'question': question,
            'answer': answer,
            'summary': {
                'current_activity': current_activity,
                'top_prediction': predictions[0]['next_activity'] if predictions else None,
                'top_probability': predictions[0]['probability'] if predictions else 0
            },
            'predictions': predictions[:3],  # Show top 3
            'query_time_ms': result.get('query_time_ms'),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _format_outcome_result(self, result: Dict[str, Any], question: str) -> Dict[str, Any]:
        """Format case outcome result."""
        if 'error' in result:
            return {
                'question': question,
                'answer': f"Could not predict outcome: {result['error']}",
                'timestamp': datetime.utcnow().isoformat()
            }
        
        completion_probability = result.get('completion_probability', 0)
        current_status = result.get('current_status', 'Unknown')
        risk_factors = result.get('risk_factors', [])
        
        if completion_probability > 0.8:
            answer = f"Case is likely to complete successfully ({completion_probability:.1%} probability). "
        elif completion_probability > 0.5:
            answer = f"Case has moderate chance of completion ({completion_probability:.1%} probability). "
        else:
            answer = f"Case has low chance of completion ({completion_probability:.1%} probability). "
        
        answer += f"Current status: {current_status}. "
        if risk_factors:
            answer += f"Risk factors: {', '.join(risk_factors)}."
        
        return {
            'question': question,
            'answer': answer,
            'summary': {
                'completion_probability': completion_probability,
                'current_status': current_status,
                'risk_factors': risk_factors,
                'confidence': result.get('confidence', 'unknown')
            },
            'query_time_ms': result.get('query_time_ms'),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _get_result_summary(self, result: Dict[str, Any]) -> str:
        """Get a brief summary of the query result."""
        if 'error' in result:
            return f"Error: {result['error']}"
        
        # Extract key metrics based on result structure
        if 'variants' in result:
            return f"Found {len(result['variants'])} process variants"
        elif 'total_cases' in result:
            return f"Analyzed {result['total_cases']} cases"
        elif 'similar_cases' in result:
            return f"Found {len(result['similar_cases'])} similar cases"
        elif 'activities' in result:
            return f"Found {len(result['activities'])} activities"
        elif 'bottlenecks' in result:
            return f"Identified {len(result['bottlenecks'])} bottlenecks"
        elif 'resources' in result:
            return f"Analyzed {len(result['resources'])} resources"
        else:
            return "Query completed successfully"
    
    def _get_suggestion_questions(self) -> List[str]:
        """Get suggested questions for users."""
        return [
            "What are the most common process variants?",
            "Which activities take the longest?",
            "Show me bottlenecks in the process",
            "Find cases similar to instance X",
            "What's the resource utilization?",
            "How long do cases typically take?",
            "Which activities happen most frequently?",
            "Show me object interactions",
            "Check conformance for instance X against model Y",
            "Predict next activity for instance X"
        ]
    
    def get_query_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent query history."""
        return self.query_history[-limit:]
    
    def clear_history(self):
        """Clear query history."""
        self.query_history = []
    
    def get_agent_stats(self) -> Dict[str, Any]:
        """Get agent statistics."""
        return {
            'total_queries': len(self.query_history),
            'intent_patterns': len(self.intent_patterns),
            'supported_intents': list(self.intent_patterns.keys()),
            'llm_configured': bool(self.llm_config.get('api_key')),
            'last_query': self.query_history[-1] if self.query_history else None
        }


def main():
    """Main function for testing the NL agent."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Natural Language Process Mining Agent')
    parser.add_argument('--question', required=True, help='Natural language question to ask')
    parser.add_argument('--history', action='store_true', help='Show query history')
    parser.add_argument('--stats', action='store_true', help='Show agent statistics')
    parser.add_argument('--clear-history', action='store_true', help='Clear query history')
    
    args = parser.parse_args()
    
    # Initialize query engine and agent
    catalog_path = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    cat = load_catalog_from_yaml('local', catalog_path)
    query_engine = ProcessMiningQueryEngine(cat)
    agent = ProcessMiningAgent(query_engine)
    
    if args.history:
        history = agent.get_query_history()
        print(f"Query History ({len(history)} queries):")
        for i, query in enumerate(history, 1):
            print(f"  {i}. {query['question']} -> {query['result_summary']}")
    
    elif args.stats:
        stats = agent.get_agent_stats()
        print(f"Agent Statistics: {json.dumps(stats, indent=2)}")
    
    elif args.clear_history:
        agent.clear_history()
        print("Query history cleared.")
    
    else:
        # Process the question
        result = agent.ask(args.question)
        print(f"Question: {args.question}")
        print(f"Answer: {result.get('answer', 'No answer available')}")
        
        if 'summary' in result:
            print(f"Summary: {json.dumps(result['summary'], indent=2)}")
        
        if 'error' in result:
            print(f"Error: {result['error']}")


if __name__ == '__main__':
    main()
