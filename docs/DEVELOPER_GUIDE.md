# Developer Guide - OCEL/OCPN Lakehouse

## Overview

This guide provides comprehensive information for developers working with the OCEL/OCPN Lakehouse system. It covers development setup, architecture, APIs, and best practices.

## 🏗️ System Architecture

### Core Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources │    │   ETL Layer     │    │   Storage Layer │
│   (CSV/JSON)    │───▶│   (Daft)        │───▶│   (Iceberg)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Analytics     │    │   Query Engine  │    │   Dashboards    │
│   (Python)      │◀───│   (Daft)        │◀───│   (Streamlit)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Technology Stack

- **Storage**: Apache Iceberg with Parquet format
- **Query Engine**: Daft (Rust-powered, Python-native)
- **Catalog**: PyIceberg (SQLite local, S3 production)
- **Analytics**: Python-based process mining algorithms
- **Dashboards**: Streamlit web applications

## 🚀 Development Setup

### Prerequisites

- Python 3.9+
- 8GB+ RAM
- 10GB+ disk space
- Git

### Installation

```bash
# Clone repository
git clone <repository-url>
cd ProcessMining

# Install dependencies
uv sync

# Bootstrap system
python lakehouse/ingest/production_bootstrap.py

# Load sample data
python lakehouse/ingest/complete_ocel_loader.py
```

### Development Environment

```bash
# Activate virtual environment
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate     # Windows

# Run tests
python -m pytest tests/

# Start development server
cd dashboard
streamlit run streamlit_app.py
```

## 📁 Project Structure

```
lakehouse/
├── agent/                    # Process mining agent
│   ├── __init__.py
│   ├── config.yaml
│   ├── example_queries.py
│   ├── nl_query_agent.py
│   ├── process_mining_queries.py
│   └── query_optimizer.py
├── analytics/               # Analytics modules
│   ├── advanced_process_mining_analytics.py
│   ├── conformance_checking.py
│   ├── cost_analysis.py
│   ├── ocpn_discovery.py
│   ├── predictive_analytics.py
│   ├── process_discovery.py
│   ├── process_mining_dashboard.py
│   └── process_mining_insights_engine.py
├── ingest/                  # Data ingestion
│   ├── bootstrap_tables.py
│   ├── complete_bootstrap.py
│   ├── complete_ocel_loader.py
│   ├── production_bootstrap.py
│   └── ...
├── ops/                     # Operations
│   ├── maintenance_system.py
│   ├── materialized_views.py
│   ├── performance_optimization.py
│   └── ...
├── queries/                 # Query templates
│   ├── test_queries.py
│   ├── validation_summary.py
│   └── ...
├── schemas/                 # Data schemas
│   ├── complete_ocel_schemas.yaml
│   ├── complete_ocpn_schemas.yaml
│   └── ...
└── docs/                    # Documentation
    └── README.md
```

## 🔧 Core APIs

### Process Mining Query Engine

```python
from lakehouse.agent.process_mining_queries import ProcessMiningQueryEngine

# Initialize query engine
query_engine = ProcessMiningQueryEngine(catalog)

# Get process variants
variants = query_engine.get_process_variants(top_n=10)

# Get activity frequencies
activities = query_engine.get_activity_frequencies()

# Check conformance
conformance = query_engine.check_conformance(instance_id, model_id)
```

### Natural Language Agent

```python
from lakehouse.agent.nl_query_agent import ProcessMiningAgent

# Initialize agent
agent = ProcessMiningAgent(query_engine, llm_config)

# Ask questions
result = agent.ask("What are the most common process variants?")
result = agent.ask("Which activities take the longest?")
result = agent.ask("Show me bottlenecks in the purchase order process")
```

### Analytics Modules

```python
from lakehouse.analytics.process_discovery import ProcessDiscovery
from lakehouse.analytics.conformance_checking import ConformanceChecker

# Process discovery
discovery = ProcessDiscovery(catalog)
variants = discovery.discover_variants()

# Conformance checking
checker = ConformanceChecker(catalog)
deviations = checker.find_deviations(model_id)
```

## 📊 Data Models

### OCEL Tables

- **events**: Event log with timestamps and attributes
- **objects**: Object instances with lifecycle tracking
- **relationships**: Object-to-object relationships
- **event_attributes**: Event-level attributes
- **object_attributes**: Object-level attributes

### OCPN Tables

- **models**: Petri net model definitions
- **places**: Place nodes in the net
- **transitions**: Transition nodes in the net
- **arcs**: Connections between places and transitions
- **markings**: Current state markings
- **layout**: Visual layout information

## 🛠️ Development Workflow

### 1. Adding New Analytics

```python
# Create new analytics module
class NewAnalytics:
    def __init__(self, catalog):
        self.catalog = catalog
    
    def analyze(self, parameters):
        # Implementation
        pass
```

### 2. Adding New Queries

```python
# Add to ProcessMiningQueryEngine
def new_query_method(self, parameters):
    """Documentation for the new query method."""
    # Implementation using Daft queries
    pass
```

### 3. Adding New Schemas

```yaml
# Add to schemas/complete_ocel_schemas.yaml
new_table:
  schema:
  - name: column_name (TYPE)
  - name: another_column (TYPE)
  partition: IDENTITY(column_name)
  sort: (column_name, timestamp)
```

## 🧪 Testing

### Running Tests

```bash
# Run all tests
python -m pytest tests/

# Run specific test
python -m pytest tests/test_process_mining_agent.py

# Run with coverage
python -m pytest --cov=lakehouse tests/
```

### Test Structure

```
tests/
├── test_process_mining_agent.py    # Agent functionality
├── test_analytics.py               # Analytics modules
├── test_queries.py                 # Query engine
└── test_integration.py             # End-to-end tests
```

## 📈 Performance Optimization

### Query Optimization

- Use materialized views for common queries
- Implement proper partitioning strategies
- Cache frequently accessed data
- Monitor query performance

### Best Practices

- Use Daft for data processing
- Implement proper error handling
- Add comprehensive logging
- Follow Python coding standards

## 🔍 Debugging

### Common Issues

1. **Import Errors**: Check Python path and dependencies
2. **Query Failures**: Verify table schemas and data
3. **Performance Issues**: Check partitioning and indexing
4. **Memory Issues**: Monitor resource usage

### Debug Tools

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Check system health
from lakehouse.ops.maintenance_system import MaintenanceSystem
maintenance = MaintenanceSystem(catalog)
health = maintenance.check_system_health()
```

## 📚 Additional Resources

- [Daft Documentation](https://www.daft.ai/)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Process Mining Best Practices](https://www.processmining.org/)

## 🤝 Contributing

### Code Standards

- Follow PEP 8 style guide
- Add comprehensive docstrings
- Include unit tests for new features
- Update documentation

### Pull Request Process

1. Fork the repository
2. Create feature branch
3. Implement changes with tests
4. Submit pull request
5. Address review feedback

---

*Last Updated: 2024-12-19*
*Version: Production Ready*
*Status: ACTIVE DEVELOPMENT*
