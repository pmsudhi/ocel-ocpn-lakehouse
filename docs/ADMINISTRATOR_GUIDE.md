# Administrator Guide - OCEL/OCPN Lakehouse

## Overview

This guide provides comprehensive information for system administrators managing the OCEL/OCPN Lakehouse system. It covers installation, configuration, monitoring, maintenance, and troubleshooting.

## 🏗️ System Architecture

### Infrastructure Components

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

### System Requirements

#### Minimum Requirements
- **CPU**: 4 cores, 2.4 GHz
- **RAM**: 8 GB
- **Storage**: 50 GB SSD
- **OS**: Linux, macOS, Windows
- **Python**: 3.9+

#### Recommended Requirements
- **CPU**: 8 cores, 3.0 GHz
- **RAM**: 16 GB
- **Storage**: 200 GB SSD
- **OS**: Linux (Ubuntu 20.04+)
- **Python**: 3.11+

#### Production Requirements
- **CPU**: 16+ cores, 3.5 GHz
- **RAM**: 32+ GB
- **Storage**: 1 TB+ SSD
- **OS**: Linux (RHEL 8+, Ubuntu 20.04+)
- **Python**: 3.11+

## 🚀 Installation

### 1. System Preparation

```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Install required packages
sudo apt install -y python3.11 python3.11-venv python3-pip git

# Create application user
sudo useradd -m -s /bin/bash processmining
sudo usermod -aG sudo processmining
```

### 2. Application Installation

```bash
# Switch to application user
sudo su - processmining

# Clone repository
git clone <repository-url> ProcessMining
cd ProcessMining

# Install dependencies
uv sync

# Bootstrap system
python lakehouse/ingest/production_bootstrap.py

# Load initial data
python lakehouse/ingest/complete_ocel_loader.py
```

### 3. Service Configuration

```bash
# Create systemd service
sudo tee /etc/systemd/system/processmining.service > /dev/null <<EOF
[Unit]
Description=Process Mining Lakehouse
After=network.target

[Service]
Type=simple
User=processmining
WorkingDirectory=/home/processmining/ProcessMining
ExecStart=/home/processmining/ProcessMining/.venv/bin/streamlit run dashboard/streamlit_app.py --server.port 8501 --server.address 0.0.0.0
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable processmining
sudo systemctl start processmining
```

## ⚙️ Configuration

### 1. Environment Variables

```bash
# Create environment file
cat > /home/processmining/ProcessMining/.env <<EOF
# Database Configuration
DATABASE_URL=sqlite:///warehouse/catalog.db
ICEBERG_CATALOG=local

# Performance Settings
MAX_WORKERS=4
CACHE_SIZE_MB=1024
QUERY_TIMEOUT_SECONDS=300

# Security Settings
SECRET_KEY=your-secret-key-here
ALLOWED_HOSTS=localhost,127.0.0.1

# Logging
LOG_LEVEL=INFO
LOG_FILE=/var/log/processmining/app.log
EOF
```

### 2. Catalog Configuration

```yaml
# lakehouse/catalogs/local.yaml
catalog:
  type: "sqlite"
  uri: "warehouse/catalog.db"
  
# lakehouse/catalogs/s3.yaml (for production)
catalog:
  type: "s3"
  uri: "s3://your-bucket/processmining"
  aws_access_key_id: "your-access-key"
  aws_secret_access_key: "your-secret-key"
  region: "us-west-2"
```

### 3. Performance Tuning

```python
# lakehouse/agent/config.yaml
materialized_views:
  refresh_strategy: "daily"
  refresh_time: "02:00"
  incremental_after_load: true
  
query_cache:
  enabled: true
  ttl_seconds: 3600
  max_size_mb: 1024
  
performance:
  max_workers: 4
  chunk_size: 10000
  memory_limit_mb: 8192
```

## 📊 Monitoring

### 1. System Health Monitoring

```bash
# Check system status
python lakehouse/ops/maintenance_system.py --check-health

# Monitor performance
python lakehouse/ops/performance_optimization.py --monitor

# Check data quality
python lakehouse/ops/data_quality.py --validate
```

### 2. Log Monitoring

```bash
# View application logs
tail -f /var/log/processmining/app.log

# View system logs
journalctl -u processmining -f

# Check error logs
grep ERROR /var/log/processmining/app.log
```

### 3. Performance Metrics

**Key Metrics to Monitor**:
- **Query Performance**: Response times < 1 second
- **Data Quality**: 100% score target
- **System Resources**: CPU < 80%, Memory < 80%
- **Storage Usage**: < 80% of available space
- **Error Rate**: < 1% of requests

### 4. Alerting Configuration

```bash
# Create monitoring script
cat > /home/processmining/monitor.sh <<'EOF'
#!/bin/bash

# Check service status
if ! systemctl is-active --quiet processmining; then
    echo "Process Mining service is down!" | mail -s "Alert: Service Down" admin@company.com
fi

# Check disk space
DISK_USAGE=$(df /home/processmining | awk 'NR==2 {print $5}' | sed 's/%//')
if [ $DISK_USAGE -gt 80 ]; then
    echo "Disk usage is ${DISK_USAGE}%" | mail -s "Alert: High Disk Usage" admin@company.com
fi

# Check memory usage
MEMORY_USAGE=$(free | awk 'NR==2{printf "%.0f", $3*100/$2}')
if [ $MEMORY_USAGE -gt 80 ]; then
    echo "Memory usage is ${MEMORY_USAGE}%" | mail -s "Alert: High Memory Usage" admin@company.com
fi
EOF

chmod +x /home/processmining/monitor.sh

# Add to crontab
echo "*/5 * * * * /home/processmining/monitor.sh" | crontab -u processmining -
```

## 🔧 Maintenance

### 1. Daily Maintenance

```bash
# Daily maintenance script
cat > /home/processmining/daily_maintenance.sh <<'EOF'
#!/bin/bash

# Update materialized views
python lakehouse/ops/materialized_views.py --refresh

# Clean up old logs
find /var/log/processmining -name "*.log" -mtime +7 -delete

# Optimize database
python lakehouse/ops/performance_optimization.py --optimize

# Check system health
python lakehouse/ops/maintenance_system.py --check-health
EOF

chmod +x /home/processmining/daily_maintenance.sh

# Schedule daily maintenance
echo "0 2 * * * /home/processmining/daily_maintenance.sh" | crontab -u processmining -
```

### 2. Weekly Maintenance

```bash
# Weekly maintenance script
cat > /home/processmining/weekly_maintenance.sh <<'EOF'
#!/bin/bash

# Full system backup
python lakehouse/ops/backup_system.py --full-backup

# Update system packages
sudo apt update && sudo apt upgrade -y

# Reboot if needed
if [ -f /var/run/reboot-required ]; then
    sudo reboot
fi
EOF

chmod +x /home/processmining/weekly_maintenance.sh

# Schedule weekly maintenance
echo "0 3 * * 0 /home/processmining/weekly_maintenance.sh" | crontab -u processmining -
```

### 3. Backup and Recovery

```bash
# Backup script
cat > /home/processmining/backup.sh <<'EOF'
#!/bin/bash

BACKUP_DIR="/backup/processmining/$(date +%Y%m%d)"
mkdir -p $BACKUP_DIR

# Backup application code
tar -czf $BACKUP_DIR/application.tar.gz /home/processmining/ProcessMining

# Backup database
cp /home/processmining/ProcessMining/warehouse/catalog.db $BACKUP_DIR/

# Backup configuration
cp -r /home/processmining/ProcessMining/lakehouse/catalogs $BACKUP_DIR/

# Clean up old backups (keep 30 days)
find /backup/processmining -type d -mtime +30 -exec rm -rf {} \;
EOF

chmod +x /home/processmining/backup.sh

# Schedule daily backups
echo "0 1 * * * /home/processmining/backup.sh" | crontab -u processmining -
```

## 🚨 Troubleshooting

### Common Issues

#### 1. **Service Won't Start**

**Symptoms**: Service fails to start or crashes immediately

**Diagnosis**:
```bash
# Check service status
systemctl status processmining

# Check logs
journalctl -u processmining -n 50

# Check port availability
netstat -tlnp | grep 8501
```

**Solutions**:
- Check port conflicts
- Verify dependencies
- Review configuration files
- Check file permissions

#### 2. **Performance Issues**

**Symptoms**: Slow queries, high resource usage

**Diagnosis**:
```bash
# Check system resources
top
htop
iostat

# Check query performance
python lakehouse/ops/performance_optimization.py --analyze
```

**Solutions**:
- Optimize queries
- Increase system resources
- Tune configuration
- Update materialized views

#### 3. **Data Quality Issues**

**Symptoms**: Missing data, incorrect results

**Diagnosis**:
```bash
# Check data quality
python lakehouse/ops/data_quality.py --validate

# Check data completeness
python lakehouse/ops/data_quality.py --completeness
```

**Solutions**:
- Fix data sources
- Update data processing
- Re-run data loading
- Validate data schemas

### Emergency Procedures

#### 1. **System Recovery**

```bash
# Stop all services
sudo systemctl stop processmining

# Restore from backup
cd /home/processmining
tar -xzf /backup/processmining/latest/application.tar.gz

# Restore database
cp /backup/processmining/latest/catalog.db warehouse/

# Restart services
sudo systemctl start processmining
```

#### 2. **Data Recovery**

```bash
# Rebuild from source data
python lakehouse/ingest/complete_ocel_loader.py --rebuild

# Validate data integrity
python lakehouse/ops/data_quality.py --validate
```

## 🔒 Security

### 1. Access Control

```bash
# Create user groups
sudo groupadd processmining-users
sudo groupadd processmining-admins

# Add users to groups
sudo usermod -aG processmining-users username
sudo usermod -aG processmining-admins admin-username
```

### 2. Network Security

```bash
# Configure firewall
sudo ufw allow 22/tcp
sudo ufw allow 8501/tcp
sudo ufw enable

# Configure SSL (if needed)
# Install certificates
# Configure reverse proxy
```

### 3. Data Security

```bash
# Encrypt sensitive data
# Configure access controls
# Implement audit logging
# Regular security updates
```

## 📈 Scaling

### 1. Horizontal Scaling

- **Load Balancer**: Distribute requests
- **Multiple Instances**: Run multiple dashboard instances
- **Database Clustering**: Scale storage layer
- **Caching Layer**: Add Redis/Memcached

### 2. Vertical Scaling

- **Increase Resources**: More CPU, RAM, storage
- **Optimize Configuration**: Tune parameters
- **Upgrade Hardware**: Better performance
- **SSD Storage**: Faster I/O

## 📞 Support

### Internal Support

- **Documentation**: Check all guides
- **Logs**: Review system and application logs
- **Monitoring**: Check system health
- **Community**: Internal knowledge base

### External Support

- **Vendor Support**: Technology providers
- **Community Forums**: User communities
- **Professional Services**: Consulting support
- **Training**: Additional training resources

---

*Last Updated: 2024-12-19*
*Version: Production Ready*
*Status: FULLY OPERATIONAL*
