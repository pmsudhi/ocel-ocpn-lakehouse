# OCEL/OCPN Lakehouse

A production-ready data lakehouse system for Object-Centric Event Logs (OCEL) and Object-Centric Petri Nets (OCPN) with advanced analytics capabilities built on Apache Iceberg.

## Features

- **Complete OCEL 2.0 Support**: Full attribute preservation and compliance
- **OCPN Support**: Object-Centric Petri Net modeling and analysis
- **Apache Iceberg Integration**: Scalable, ACID-compliant data storage
- **Process Mining Analytics**: Process discovery, conformance checking, and performance analysis
- **Natural Language Interface**: Query your process data using natural language
- **Production Ready**: Optimized partitioning, sorting, and performance tuning

## Installation

### Using pip

```bash
pip install ocel-ocpn-lakehouse
```

### Using uv (recommended)

```bash
uv pip install ocel-ocpn-lakehouse
```

### From source

```bash
git clone https://github.com/your-org/ocel-ocpn-lakehouse.git
cd ocel-ocpn-lakehouse
pip install -e .
```

## Quick Start

### 1. Bootstrap the Lakehouse

```python
from lakehouse import bootstrap_lakehouse, load_catalog_from_yaml
from pathlib import Path

# Bootstrap all required tables
bootstrap_lakehouse(
    catalog_name='local',
    catalog_config_path=Path('catalogs/local.yaml')
)
```

### 2. Load Data

```python
from lakehouse import PM4PyIcebergLoader
import pm4py

# Load OCEL data
ocel = pm4py.read_ocel('your_data.jsonocel')

# Load into Iceberg
loader = PM4PyIcebergLoader(catalog)
loader.load_ocel_to_iceberg(ocel, model_id='my_model')
```

### 3. Run Analytics

```python
from lakehouse import (
    ProcessMiningQueryEngine,
    discover_process_variants,
    analyze_process_performance
)

# Initialize query engine
query_engine = ProcessMiningQueryEngine(catalog)

# Discover process variants
variants = discover_process_variants(catalog)

# Analyze performance
performance = analyze_process_performance(catalog)
```

### 4. Natural Language Queries

```python
from lakehouse import ProcessMiningAgent

# Initialize agent
agent = ProcessMiningAgent(
    query_engine,
    llm_config={
        'provider': 'openai',
        'model': 'gpt-4',
        'api_key': 'your-api-key'
    }
)

# Ask questions
result = agent.ask("What are the most common process variants?")
print(result)
```

## Package Structure

```
lakehouse/
├── agent/              # Process mining query engine and NL interface
├── analytics/          # Process discovery, conformance, cost analysis
├── ingest/             # Data loading and bootstrap utilities
├── ops/                # Operations and maintenance tools
├── queries/            # Query templates and validation
├── schemas/           # OCEL/OCPN schema definitions
└── catalogs/          # Catalog configuration files
```

## Core Components

### Agent Module

- `ProcessMiningQueryEngine`: Query engine for process mining data
- `ProcessMiningAgent`: Natural language interface for queries
- `QueryOptimizer`: Query optimization utilities

### Analytics Module

- `PM4PyAnalyticsWrapper`: pm4py integration for Iceberg data
- `ConformanceChecker`: Conformance checking against models
- Process discovery functions: Variant analysis, performance metrics
- Cost analysis functions: ROI calculations, optimization opportunities

### Ingest Module

- `bootstrap_lakehouse()`: Initialize all required tables
- `load_catalog_from_yaml()`: Load catalog configuration
- `PM4PyIcebergLoader`: Load pm4py objects into Iceberg

## Documentation

For detailed documentation, see:
- [Developer Guide](docs/DEVELOPER_GUIDE.md)
- [User Guide](docs/USER_GUIDE.md)
- [Administrator Guide](docs/ADMINISTRATOR_GUIDE.md)
- [Setup Runbook](docs/SETUP_RUNBOOK.md)

## Requirements

- Python 3.9+
- Apache Iceberg catalog (local file system or S3)
- 8GB+ RAM recommended
- 10GB+ disk space

## License

MIT License

## Contributing

Contributions are welcome! Please see our contributing guidelines for details.

## Support

For issues and questions:
- GitHub Issues: https://github.com/your-org/ocel-ocpn-lakehouse/issues
- Documentation: https://github.com/your-org/ocel-ocpn-lakehouse/docs

