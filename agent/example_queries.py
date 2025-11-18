#!/usr/bin/env python3
"""
Example Queries for Process Mining Agent
Demonstrates various types of natural language queries
"""

# Example questions for different process mining capabilities

PROCESS_VARIANT_QUERIES = [
    "What are the most common process variants?",
    "Show me the top 10 process variants",
    "What are the most frequent process patterns?",
    "Which process flows happen most often?",
    "Show me process variants with minimum frequency 5"
]

CASE_DURATION_QUERIES = [
    "How long do cases typically take?",
    "What's the average case duration?",
    "Show me case duration distribution",
    "How long are purchase order processes?",
    "What's the median case duration?"
]

SIMILAR_CASES_QUERIES = [
    "Find cases similar to instance REQ_001",
    "Show me cases like this one",
    "Find comparable processes to instance ORDER_123",
    "What cases are similar to REQ_456?",
    "Find instances with similar patterns"
]

ACTIVITY_QUERIES = [
    "What are the most common activities?",
    "Which activities happen most frequently?",
    "Show me activity frequencies",
    "What activities are used most?",
    "Which processes are most active?"
]

BOTTLENECK_QUERIES = [
    "Identify bottlenecks in the process",
    "Which activities take the longest?",
    "Find process bottlenecks",
    "Show me slow activities",
    "What are the performance issues?",
    "Which steps are causing delays?"
]

RESOURCE_QUERIES = [
    "What's the resource utilization?",
    "Who is the busiest resource?",
    "Show me resource workload",
    "Which users are most active?",
    "How is the workload distributed?",
    "Who has the highest utilization?"
]

OBJECT_LIFECYCLE_QUERIES = [
    "Show me the lifecycle of object OBJ_123",
    "What's the history of object REQ_456?",
    "Show me all events for object ORDER_789",
    "What happened to object VENDOR_001?",
    "Show me object timeline for OBJ_999"
]

OBJECT_INTERACTION_QUERIES = [
    "How do objects interact?",
    "Show me object relationships",
    "What are the object connections?",
    "How do different objects work together?",
    "Show me object interaction patterns"
]

CONFORMANCE_QUERIES = [
    "Check conformance for instance REQ_001 against model MODEL_123",
    "Does this instance conform to the model?",
    "Show me conformance analysis",
    "Check process compliance",
    "How well does this case fit the model?"
]

PREDICTION_QUERIES = [
    "Predict next activity for instance REQ_001",
    "What will happen next in this case?",
    "What's the next step for instance ORDER_123?",
    "Forecast the next activity",
    "What should happen next?"
]

OUTCOME_QUERIES = [
    "Will this case complete successfully?",
    "What's the completion probability for instance REQ_001?",
    "Will this process finish?",
    "What's the success rate for this case?",
    "Is this case likely to succeed?"
]

# Complex multi-part queries
COMPLEX_QUERIES = [
    "Show me the top 5 process variants and identify bottlenecks in each",
    "Find similar cases to REQ_001 and check their conformance to MODEL_123",
    "What are the most common activities and which resources are busiest?",
    "Show me case duration distribution and predict outcomes for running cases",
    "Identify bottlenecks, check resource utilization, and suggest optimizations"
]

# Domain-specific queries for Purchase-to-Pay process
P2P_SPECIFIC_QUERIES = [
    "What are the most common purchase order variants?",
    "How long do invoice processes take?",
    "Which vendors have the most bottlenecks?",
    "Show me purchase request patterns",
    "What's the approval process efficiency?",
    "Which approvers are busiest?",
    "How do purchase orders interact with invoices?",
    "What's the vendor performance analysis?",
    "Show me payment process bottlenecks",
    "Which purchase categories take longest?"
]

# Performance and optimization queries
PERFORMANCE_QUERIES = [
    "What's the overall process performance?",
    "Show me process efficiency metrics",
    "Which processes need optimization?",
    "What are the key performance indicators?",
    "How can we improve process speed?",
    "What's the process throughput?",
    "Show me cycle time analysis",
    "Which steps add the most value?"
]

# Compliance and quality queries
COMPLIANCE_QUERIES = [
    "Are all processes following the standard?",
    "Show me compliance violations",
    "Which cases deviate from the norm?",
    "What are the quality issues?",
    "Show me process conformance rates",
    "Which processes need standardization?",
    "What are the audit findings?",
    "Show me process quality metrics"
]

# All query categories
QUERY_CATEGORIES = {
    'process_variants': PROCESS_VARIANT_QUERIES,
    'case_duration': CASE_DURATION_QUERIES,
    'similar_cases': SIMILAR_CASES_QUERIES,
    'activities': ACTIVITY_QUERIES,
    'bottlenecks': BOTTLENECK_QUERIES,
    'resources': RESOURCE_QUERIES,
    'object_lifecycle': OBJECT_LIFECYCLE_QUERIES,
    'object_interactions': OBJECT_INTERACTION_QUERIES,
    'conformance': CONFORMANCE_QUERIES,
    'predictions': PREDICTION_QUERIES,
    'outcomes': OUTCOME_QUERIES,
    'complex': COMPLEX_QUERIES,
    'p2p_specific': P2P_SPECIFIC_QUERIES,
    'performance': PERFORMANCE_QUERIES,
    'compliance': COMPLIANCE_QUERIES
}

def get_example_queries(category: str = None) -> list:
    """Get example queries for a specific category or all categories."""
    if category:
        return QUERY_CATEGORIES.get(category, [])
    else:
        all_queries = []
        for queries in QUERY_CATEGORIES.values():
            all_queries.extend(queries)
        return all_queries

def get_categories() -> list:
    """Get all available query categories."""
    return list(QUERY_CATEGORIES.keys())

def print_example_queries(category: str = None, limit: int = 5):
    """Print example queries for a category."""
    queries = get_example_queries(category)
    
    if category:
        print(f"\nExample {category} queries:")
    else:
        print(f"\nExample queries (showing {limit} per category):")
    
    if category:
        for i, query in enumerate(queries[:limit], 1):
            print(f"  {i}. {query}")
    else:
        for cat, cat_queries in QUERY_CATEGORIES.items():
            print(f"\n{cat.upper()}:")
            for i, query in enumerate(cat_queries[:limit], 1):
                print(f"  {i}. {query}")

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Example Process Mining Queries')
    parser.add_argument('--category', help='Show queries for specific category')
    parser.add_argument('--limit', type=int, default=5, help='Number of examples to show')
    parser.add_argument('--list-categories', action='store_true', help='List all categories')
    
    args = parser.parse_args()
    
    if args.list_categories:
        print("Available query categories:")
        for category in get_categories():
            print(f"  - {category}")
    else:
        print_example_queries(args.category, args.limit)
