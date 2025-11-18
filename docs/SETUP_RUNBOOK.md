# Setup Runbook - OCEL/OCPN Lakehouse

## Overview

This runbook provides step-by-step instructions for setting up the OCEL/OCPN Lakehouse system from scratch. It covers installation, configuration, data loading, and validation.

## 🎯 Prerequisites

### System Requirements

- **OS**: Linux (Ubuntu 20.04+), macOS (10.15+), Windows 10+
- **Python**: 3.9+ (3.11+ recommended)
- **RAM**: 8 GB minimum, 16 GB recommended
- **Storage**: 50 GB minimum, 200 GB recommended
- **CPU**: 4 cores minimum, 8 cores recommended

### Software Dependencies

- **Git**: For repository cloning
- **UV**: Python package manager (recommended)
- **Docker**: Optional, for containerized deployment

## 🚀 Installation Steps

### Step 1: System Preparation

#### Linux (Ubuntu/Debian)

```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Install required packages
sudo apt install -y python3.11 python3.11-venv python3-pip git curl

# Install UV (Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc

# Verify installation
python3 --version
uv --version
git --version
```

#### macOS

```bash
# Install Homebrew (if not installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install required packages
brew install python@3.11 git

# Install UV
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.zshrc

# Verify installation
python3 --version
uv --version
git --version
```

#### Windows

```powershell
# Install Python 3.11 from python.org
# Install Git from git-scm.com
# Install UV
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Verify installation
python --version
uv --version
git --version
```

### Step 2: Repository Setup

```bash
# Clone repository
git clone <repository-url> ProcessMining
cd ProcessMining

# Verify repository structure
ls -la
```

### Step 3: Environment Setup

```bash
# Create virtual environment
uv venv

# Activate virtual environment
# Linux/macOS:
source .venv/bin/activate
# Windows:
.venv\Scripts\activate

# Install dependencies
uv sync

# Verify installation
python -c "import pm4py, pandas, streamlit; print('Dependencies installed successfully')"
```

### Step 4: System Bootstrap

```bash
# Bootstrap the system
python lakehouse/ingest/production_bootstrap.py

# Verify bootstrap
ls -la warehouse/
```

### Step 5: Data Loading

```bash
# Load sample data
python lakehouse/ingest/complete_ocel_loader.py

# Verify data loading
python -c "
import sqlite3
conn = sqlite3.connect('warehouse/catalog.db')
cursor = conn.cursor()
cursor.execute('SELECT name FROM sqlite_master WHERE type=\"table\"')
tables = cursor.fetchall()
print(f'Loaded {len(tables)} tables')
conn.close()
"
```

### Step 6: System Validation

```bash
# Run system validation
python lakehouse/queries/final_system_validation.py

# Check system health
python lakehouse/ops/maintenance_system.py --check-health
```

## ⚙️ Configuration

### Step 1: Basic Configuration

```bash
# Create configuration directory
mkdir -p lakehouse/config

# Create basic configuration
cat > lakehouse/config/local.yaml <<EOF
catalog:
  type: "sqlite"
  uri: "warehouse/catalog.db"
  
performance:
  max_workers: 4
  cache_size_mb: 1024
  
logging:
  level: "INFO"
  file: "logs/app.log"
EOF
```

### Step 2: Environment Variables

```bash
# Create environment file
cat > .env <<EOF
# Database Configuration
DATABASE_URL=sqlite:///warehouse/catalog.db
ICEBERG_CATALOG=local

# Performance Settings
MAX_WORKERS=4
CACHE_SIZE_MB=1024

# Security
SECRET_KEY=your-secret-key-here
ALLOWED_HOSTS=localhost,127.0.0.1

# Logging
LOG_LEVEL=INFO
LOG_FILE=logs/app.log
EOF
```

### Step 3: Service Configuration (Optional)

#### Linux (systemd)

```bash
# Create systemd service
sudo tee /etc/systemd/system/processmining.service > /dev/null <<EOF
[Unit]
Description=Process Mining Lakehouse
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$(pwd)
ExecStart=$(pwd)/.venv/bin/streamlit run dashboard/streamlit_app.py --server.port 8501 --server.address 0.0.0.0
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable service
sudo systemctl daemon-reload
sudo systemctl enable processmining
sudo systemctl start processmining
```

#### macOS (launchd)

```bash
# Create launchd plist
cat > ~/Library/LaunchAgents/com.processmining.plist <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.processmining</string>
    <key>ProgramArguments</key>
    <array>
        <string>$(pwd)/.venv/bin/streamlit</string>
        <string>run</string>
        <string>dashboard/streamlit_app.py</string>
        <string>--server.port</string>
        <string>8501</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$(pwd)</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
</dict>
</plist>
EOF

# Load service
launchctl load ~/Library/LaunchAgents/com.processmining.plist
```

## 🧪 Testing

### Step 1: Unit Tests

```bash
# Run unit tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=lakehouse --cov-report=html
```

### Step 2: Integration Tests

```bash
# Run integration tests
python lakehouse/queries/production_validation.py

# Test data quality
python lakehouse/ops/data_quality.py --validate
```

### Step 3: Performance Tests

```bash
# Run performance tests
python lakehouse/ops/performance_optimization.py --benchmark

# Test query performance
python lakehouse/queries/test_queries.py
```

## 🎛️ Dashboard Setup

### Step 1: Start Dashboard

```bash
# Start Streamlit dashboard
cd dashboard
streamlit run streamlit_app.py
```

### Step 2: Access Dashboard

- **URL**: http://localhost:8501
- **Default Port**: 8501
- **Custom Port**: `streamlit run streamlit_app.py --server.port 8502`

### Step 3: Verify Dashboard

1. **Open Browser**: Navigate to http://localhost:8501
2. **Check Navigation**: Verify all menu items work
3. **Test Features**: Run sample analyses
4. **Verify Data**: Check data is loading correctly

## 📊 Data Validation

### Step 1: Data Completeness

```bash
# Check data completeness
python -c "
import sqlite3
conn = sqlite3.connect('warehouse/catalog.db')
cursor = conn.cursor()

# Check event count
cursor.execute('SELECT COUNT(*) FROM events')
event_count = cursor.fetchone()[0]
print(f'Events: {event_count}')

# Check object count
cursor.execute('SELECT COUNT(*) FROM objects')
object_count = cursor.fetchone()[0]
print(f'Objects: {object_count}')

conn.close()
"
```

### Step 2: Data Quality

```bash
# Run data quality checks
python lakehouse/ops/data_quality.py --validate

# Check for missing values
python lakehouse/ops/data_quality.py --completeness
```

### Step 3: Performance Validation

```bash
# Test query performance
python lakehouse/queries/test_queries.py

# Benchmark system
python lakehouse/ops/performance_optimization.py --benchmark
```

## 🔧 Troubleshooting

### Common Issues

#### 1. **Dependencies Not Installing**

**Error**: `ModuleNotFoundError` or import errors

**Solution**:
```bash
# Reinstall dependencies
uv sync --reinstall

# Check Python version
python --version

# Verify virtual environment
which python
```

#### 2. **Database Connection Issues**

**Error**: Database connection failures

**Solution**:
```bash
# Check database file
ls -la warehouse/catalog.db

# Recreate database
rm warehouse/catalog.db
python lakehouse/ingest/production_bootstrap.py
```

#### 3. **Port Already in Use**

**Error**: Port 8501 already in use

**Solution**:
```bash
# Find process using port
lsof -i :8501

# Kill process
kill -9 <PID>

# Use different port
streamlit run streamlit_app.py --server.port 8502
```

#### 4. **Memory Issues**

**Error**: Out of memory errors

**Solution**:
```bash
# Check memory usage
free -h

# Reduce data scope
# Use filters in analysis
# Increase system memory
```

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG

# Run with verbose output
python lakehouse/ingest/complete_ocel_loader.py --verbose

# Check logs
tail -f logs/app.log
```

## 📈 Performance Optimization

### Step 1: System Tuning

```bash
# Optimize system settings
python lakehouse/ops/performance_optimization.py --tune

# Update materialized views
python lakehouse/ops/materialized_views.py --refresh
```

### Step 2: Configuration Tuning

```yaml
# lakehouse/agent/config.yaml
performance:
  max_workers: 8
  cache_size_mb: 2048
  chunk_size: 50000
  
query_cache:
  enabled: true
  ttl_seconds: 7200
  max_size_mb: 2048
```

### Step 3: Monitoring Setup

```bash
# Set up monitoring
python lakehouse/ops/maintenance_system.py --setup-monitoring

# Create monitoring script
cat > monitor.sh <<'EOF'
#!/bin/bash
python lakehouse/ops/maintenance_system.py --check-health
EOF
chmod +x monitor.sh
```

## ✅ Validation Checklist

### System Setup
- [ ] Python 3.9+ installed
- [ ] Dependencies installed successfully
- [ ] Virtual environment activated
- [ ] Repository cloned correctly

### Database Setup
- [ ] Database created successfully
- [ ] Tables created with correct schemas
- [ ] Data loaded without errors
- [ ] Data quality validation passed

### Dashboard Setup
- [ ] Streamlit dashboard accessible
- [ ] All menu items working
- [ ] Data visualization working
- [ ] Export functionality working

### Performance
- [ ] Query response times < 1 second
- [ ] Memory usage < 80%
- [ ] CPU usage < 80%
- [ ] Disk usage < 80%

### Security
- [ ] Access controls configured
- [ ] Sensitive data protected
- [ ] Logging enabled
- [ ] Backup procedures in place

## 🎉 Success Criteria

### Technical Success
- ✅ System boots without errors
- ✅ All tests pass
- ✅ Dashboard accessible and functional
- ✅ Data quality score > 95%

### Performance Success
- ✅ Query response times < 1 second
- ✅ System resources < 80% utilization
- ✅ Data processing completes successfully
- ✅ No critical errors in logs

### Business Success
- ✅ Users can access system
- ✅ Analyses produce meaningful results
- ✅ Reports generate correctly
- ✅ System ready for production use

---

*Last Updated: 2024-12-19*
*Version: Production Ready*
*Status: FULLY OPERATIONAL*
