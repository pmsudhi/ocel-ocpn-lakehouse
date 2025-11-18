# User Guide - OCEL/OCPN Lakehouse

## Overview

This guide provides comprehensive information for end users of the OCEL/OCPN Lakehouse system. It covers accessing the system, running analyses, interpreting results, and using the interactive dashboard.

## 🚀 Getting Started

### Accessing the System

1. **Web Dashboard**: Navigate to the Streamlit dashboard
2. **Command Line**: Use Python scripts for batch processing
3. **API**: Programmatic access through Python APIs

### Prerequisites

- Web browser (Chrome, Firefox, Safari, Edge)
- Basic understanding of process mining concepts
- Access to the system (provided by administrator)

## 🎛️ Interactive Dashboard

### Dashboard Access

```bash
# Start the dashboard
cd dashboard
streamlit run streamlit_app.py
```

### Dashboard Features

#### 1. **Process Overview**
- Total events and objects
- Process performance metrics
- System health indicators

#### 2. **Variant Analysis**
- Process variant discovery
- Frequency analysis
- Pattern visualization

#### 3. **Performance Analysis**
- Activity duration analysis
- Bottleneck identification
- Resource utilization

#### 4. **Cost Analysis**
- Process cost breakdown
- Optimization opportunities
- ROI calculations

#### 5. **Predictive Analytics**
- Outcome prediction
- Risk assessment
- Forecasting

### Dashboard Navigation

1. **Sidebar Menu**: Select analysis type
2. **Main Panel**: View results and visualizations
3. **Filters**: Apply date ranges, object types, etc.
4. **Export**: Download results as CSV/JSON

## 📊 Running Analyses

### 1. Process Discovery

**Purpose**: Identify process variants and patterns

**Steps**:
1. Navigate to "Process Discovery" in dashboard
2. Select analysis parameters
3. Click "Run Analysis"
4. Review results and visualizations

**Results**:
- Process variants with frequencies
- Activity sequences
- Pattern visualizations

### 2. Performance Analysis

**Purpose**: Analyze process performance and bottlenecks

**Steps**:
1. Go to "Performance Analysis"
2. Set time period and filters
3. Run analysis
4. Review performance metrics

**Results**:
- Activity duration statistics
- Bottleneck identification
- Performance trends

### 3. Cost Analysis

**Purpose**: Analyze process costs and optimization opportunities

**Steps**:
1. Access "Cost Analysis" section
2. Configure cost parameters
3. Run analysis
4. Review cost breakdown

**Results**:
- Cost per activity
- Total process costs
- Optimization recommendations

### 4. Conformance Checking

**Purpose**: Check process conformance to models

**Steps**:
1. Select "Conformance Checking"
2. Choose process model
3. Run conformance analysis
4. Review deviations

**Results**:
- Conformance metrics
- Deviation details
- Compliance reports

## 📈 Interpreting Results

### Process Variants

**What to Look For**:
- **High-frequency variants**: Standard processes
- **Low-frequency variants**: Exceptions or errors
- **Complex variants**: Optimization opportunities

**Action Items**:
- Standardize high-frequency variants
- Investigate low-frequency variants
- Simplify complex variants

### Performance Metrics

**Key Metrics**:
- **Cycle Time**: Total process duration
- **Waiting Time**: Time between activities
- **Processing Time**: Actual work time
- **Resource Utilization**: Resource efficiency

**Interpretation**:
- High cycle time → Process inefficiency
- High waiting time → Bottlenecks
- Low resource utilization → Over-provisioning

### Cost Analysis

**Cost Components**:
- **Direct Costs**: Labor, materials, systems
- **Indirect Costs**: Overhead, administration
- **Opportunity Costs**: Delays, inefficiencies

**Optimization Opportunities**:
- High-cost activities
- Inefficient processes
- Resource waste

## 🎯 Best Practices

### 1. Data Quality

**Before Analysis**:
- Verify data completeness
- Check for missing values
- Validate timestamps
- Ensure data consistency

**During Analysis**:
- Use appropriate filters
- Set realistic time periods
- Consider data limitations

### 2. Analysis Selection

**Choose the Right Analysis**:
- **Process Discovery**: Understanding process structure
- **Performance Analysis**: Identifying bottlenecks
- **Cost Analysis**: Finding optimization opportunities
- **Conformance Checking**: Ensuring compliance

### 3. Result Interpretation

**Context Matters**:
- Consider business context
- Account for seasonal variations
- Understand process constraints
- Validate with domain experts

## 🔧 Advanced Features

### Custom Queries

**Natural Language Interface**:
```
"What are the most common process variants?"
"Which activities take the longest?"
"Show me bottlenecks in the purchase order process"
"Find cases similar to instance X"
```

**Programmatic Access**:
```python
from lakehouse.agent.nl_query_agent import ProcessMiningAgent

agent = ProcessMiningAgent(query_engine, llm_config)
result = agent.ask("Your question here")
```

### Batch Processing

**Command Line Interface**:
```bash
# Run comprehensive analysis
python process_mining_analysis/comprehensive_p2p_analysis.py

# Run specific analysis
python process_mining_analysis/variant_analysis.py

# Generate reports
python results/reports/executive_report_generator.py
```

### Export and Reporting

**Export Formats**:
- CSV for data analysis
- JSON for programmatic use
- PNG for presentations
- HTML for reports

**Report Generation**:
- Executive summaries
- Detailed analysis reports
- Performance dashboards
- Cost optimization reports

## 🚨 Troubleshooting

### Common Issues

#### 1. **Dashboard Not Loading**
- Check if Streamlit is running
- Verify port availability
- Check browser compatibility

#### 2. **Analysis Errors**
- Verify data availability
- Check parameter settings
- Review error messages

#### 3. **Slow Performance**
- Reduce data scope
- Use filters effectively
- Check system resources

### Getting Help

1. **Check Documentation**: Review relevant guides
2. **System Health**: Run diagnostic tools
3. **Contact Support**: Reach out to administrators
4. **Community Forums**: Check user communities

## 📚 Additional Resources

### Learning Materials

- **Process Mining Basics**: Understanding the fundamentals
- **OCEL/OCPN Concepts**: Object-centric process mining
- **Analytics Interpretation**: Making sense of results
- **Best Practices**: Industry standards and guidelines

### External Resources

- [Process Mining Community](https://www.processmining.org/)
- [OCEL Standard](https://ocel-standard.org/)
- [Process Mining Tools](https://www.processmining-tools.com/)

## 🎉 Success Stories

### Typical Use Cases

1. **Process Optimization**: Reducing cycle times by 30%
2. **Cost Reduction**: Identifying 20% cost savings
3. **Compliance**: Achieving 100% conformance
4. **Predictive Analytics**: Early warning systems

### Business Impact

- **Operational Efficiency**: Streamlined processes
- **Cost Savings**: Reduced operational costs
- **Compliance**: Improved regulatory adherence
- **Strategic Insights**: Data-driven decisions

---

*Last Updated: 2024-12-19*
*Version: Production Ready*
*Status: FULLY OPERATIONAL*
