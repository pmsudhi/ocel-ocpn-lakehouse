"""
Basic usage examples for the OCEL/OCPN Lakehouse package.

This script demonstrates how to:
1. Bootstrap the lakehouse
2. Load data
3. Run analytics
4. Query using natural language
"""

from pathlib import Path
from lakehouse import (
    bootstrap_lakehouse,
    load_catalog_from_yaml,
    ProcessMiningQueryEngine,
    ProcessMiningAgent,
    PM4PyIcebergLoader,
    discover_process_variants,
    analyze_process_performance
)
import pm4py


def example_bootstrap():
    """Example: Bootstrap the lakehouse with all required tables."""
    print("=" * 80)
    print("Example 1: Bootstrapping the Lakehouse")
    print("=" * 80)
    
    # Bootstrap with default local catalog
    catalog_config = Path(__file__).parent.parent / 'catalogs' / 'local.yaml'
    success = bootstrap_lakehouse(
        catalog_name='local',
        catalog_config_path=catalog_config
    )
    
    if success:
        print("✅ Lakehouse bootstrapped successfully!")
    else:
        print("❌ Bootstrap failed")
    
    return success


def example_load_catalog():
    """Example: Load a catalog configuration."""
    print("\n" + "=" * 80)
    print("Example 2: Loading Catalog")
    print("=" * 80)
    
    catalog_config = Path(__file__).parent.parent / 'catalogs' / 'local.yaml'
    catalog = load_catalog_from_yaml('local', catalog_config)
    
    # List namespaces
    namespaces = catalog.list_namespaces()
    print(f"Available namespaces: {namespaces}")
    
    return catalog


def example_load_data(catalog):
    """Example: Load OCEL data into Iceberg."""
    print("\n" + "=" * 80)
    print("Example 3: Loading OCEL Data")
    print("=" * 80)
    
    # Load OCEL file (replace with your file path)
    ocel_file = Path("data/your_data.jsonocel")
    
    if not ocel_file.exists():
        print(f"⚠️  OCEL file not found: {ocel_file}")
        print("   Skipping data load example")
        return None
    
    # Load with pm4py
    ocel = pm4py.read_ocel(str(ocel_file))
    print(f"✅ Loaded OCEL with {len(ocel.events)} events")
    
    # Load into Iceberg
    loader = PM4PyIcebergLoader(catalog)
    loader.load_ocel_to_iceberg(ocel, model_id='example_model')
    print("✅ Data loaded into Iceberg")
    
    return ocel


def example_query_engine(catalog):
    """Example: Use the query engine."""
    print("\n" + "=" * 80)
    print("Example 4: Using Query Engine")
    print("=" * 80)
    
    query_engine = ProcessMiningQueryEngine(catalog)
    
    # Get process variants
    variants = query_engine.get_process_variants(top_n=5)
    print(f"✅ Found {len(variants)} process variants")
    
    # Get activity frequencies
    activities = query_engine.get_activity_frequencies()
    print(f"✅ Found {len(activities)} activities")
    
    return query_engine


def example_analytics(catalog):
    """Example: Run analytics."""
    print("\n" + "=" * 80)
    print("Example 5: Running Analytics")
    print("=" * 80)
    
    # Discover process variants
    variants = discover_process_variants(catalog)
    print(f"✅ Process variants discovered")
    
    # Analyze performance
    performance = analyze_process_performance(catalog)
    print(f"✅ Performance analysis completed")
    
    return variants, performance


def example_nl_agent(query_engine):
    """Example: Natural language queries."""
    print("\n" + "=" * 80)
    print("Example 6: Natural Language Queries")
    print("=" * 80)
    
    # Initialize agent (requires LLM API key)
    llm_config = {
        'provider': 'openai',
        'model': 'gpt-4',
        'api_key': None  # Set your API key here or via environment variable
    }
    
    agent = ProcessMiningAgent(query_engine, llm_config)
    
    # Ask questions
    questions = [
        "What are the most common process variants?",
        "Which activities take the longest?",
        "Show me bottlenecks in the process"
    ]
    
    for question in questions:
        print(f"\nQ: {question}")
        try:
            result = agent.ask(question)
            print(f"A: {result}")
        except Exception as e:
            print(f"⚠️  Error: {e}")
            print("   (This requires a valid LLM API key)")


def main():
    """Run all examples."""
    print("\n" + "=" * 80)
    print("OCEL/OCPN Lakehouse - Basic Usage Examples")
    print("=" * 80)
    
    # Example 1: Bootstrap
    if not example_bootstrap():
        print("❌ Bootstrap failed. Cannot continue with examples.")
        return
    
    # Example 2: Load catalog
    catalog = example_load_catalog()
    
    # Example 3: Load data (optional)
    # ocel = example_load_data(catalog)
    
    # Example 4: Query engine
    query_engine = example_query_engine(catalog)
    
    # Example 5: Analytics
    # variants, performance = example_analytics(catalog)
    
    # Example 6: Natural language (optional, requires API key)
    # example_nl_agent(query_engine)
    
    print("\n" + "=" * 80)
    print("Examples completed!")
    print("=" * 80)


if __name__ == '__main__':
    main()

