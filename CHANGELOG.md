# Changelog

All notable changes to the `ocel-ocpn-lakehouse` package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-12-19

### Added
- Initial release as standalone Python package
- Complete OCEL 2.0 support with full attribute preservation
- OCPN (Object-Centric Petri Net) support
- Apache Iceberg integration for scalable data storage
- Process mining analytics:
  - Process discovery and variant analysis
  - Conformance checking
  - Performance analysis
  - Cost analysis and ROI calculations
- Natural language query interface via LLM integration
- Query engine with optimization
- Bootstrap utilities for table creation
- pm4py integration for analytics
- Comprehensive documentation

### Package Structure
- `lakehouse.agent`: Query engine and natural language interface
- `lakehouse.analytics`: Process discovery, conformance, and cost analysis
- `lakehouse.ingest`: Data loading and bootstrap utilities
- `lakehouse.ops`: Operations and maintenance tools

### Installation
- Package can be installed via `pip install ocel-ocpn-lakehouse`
- Supports Python 3.9+
- Optional dependencies for development and dashboard features

## [Unreleased]

### Planned
- Additional analytics algorithms
- Enhanced dashboard features
- Performance optimizations
- Extended documentation and examples

