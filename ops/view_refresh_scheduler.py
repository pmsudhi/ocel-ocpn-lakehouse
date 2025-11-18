#!/usr/bin/env python3
"""
Materialized View Refresh Scheduler
Automatically refreshes materialized views based on configuration
"""

import os
import sys
import time
import schedule
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import json

from materialized_views import MaterializedViewManager, load_catalog_from_yaml

# Fix Unicode display issues on Windows
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"


class ViewRefreshScheduler:
    """Schedules and manages materialized view refreshes."""
    
    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config = self._load_config()
        self.catalog_path = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
        self.catalog = load_catalog_from_yaml('local', self.catalog_path)
        self.view_manager = MaterializedViewManager(self.catalog)
        self.last_refresh = {}
        self.refresh_stats = {
            'total_refreshes': 0,
            'successful_refreshes': 0,
            'failed_refreshes': 0,
            'last_full_refresh': None
        }
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        default_config = {
            'materialized_views': {
                'refresh_strategy': 'daily',
                'refresh_time': '02:00',
                'incremental_after_load': True,
                'full_refresh_interval_days': 7
            },
            'query_cache': {
                'enabled': True,
                'ttl_seconds': 3600,
                'max_size_mb': 1024
            },
            'process_discovery': {
                'default_algorithm': 'inductive_miner',
                'min_variant_frequency': 0.01
            }
        }
        
        if self.config_path.exists():
            try:
                with self.config_path.open('r', encoding='utf-8') as f:
                    import yaml
                    user_config = yaml.safe_load(f)
                    # Merge with defaults
                    for key, value in user_config.items():
                        if key in default_config:
                            default_config[key].update(value)
                        else:
                            default_config[key] = value
            except Exception as e:
                print(f"[WARNING] Could not load config from {self.config_path}: {e}")
                print("Using default configuration")
        
        return default_config
    
    def schedule_refreshes(self):
        """Schedule materialized view refreshes based on configuration."""
        refresh_time = self.config['materialized_views']['refresh_time']
        refresh_strategy = self.config['materialized_views']['refresh_strategy']
        
        if refresh_strategy == 'daily':
            schedule.every().day.at(refresh_time).do(self._daily_refresh)
            print(f"Scheduled daily refresh at {refresh_time}")
        elif refresh_strategy == 'hourly':
            schedule.every().hour.do(self._hourly_refresh)
            print("Scheduled hourly refresh")
        elif refresh_strategy == 'weekly':
            schedule.every().week.at(refresh_time).do(self._weekly_refresh)
            print(f"Scheduled weekly refresh at {refresh_time}")
        else:
            print(f"[WARNING] Unknown refresh strategy: {refresh_strategy}")
    
    def _daily_refresh(self):
        """Perform daily materialized view refresh."""
        print(f"\n[{datetime.now()}] Starting daily materialized view refresh...")
        self._refresh_views(incremental=True)
    
    def _hourly_refresh(self):
        """Perform hourly materialized view refresh."""
        print(f"\n[{datetime.now()}] Starting hourly materialized view refresh...")
        self._refresh_views(incremental=True)
    
    def _weekly_refresh(self):
        """Perform weekly full materialized view refresh."""
        print(f"\n[{datetime.now()}] Starting weekly full materialized view refresh...")
        self._refresh_views(incremental=False)
    
    def _refresh_views(self, incremental: bool = True):
        """Refresh materialized views."""
        try:
            start_time = datetime.now()
            
            if incremental:
                print("Performing incremental refresh...")
                # For incremental, only refresh views that need updates
                success = self._incremental_refresh()
            else:
                print("Performing full refresh...")
                # For full refresh, rebuild all views
                success = self.view_manager.refresh_all_views()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.refresh_stats['total_refreshes'] += 1
            
            if success:
                self.refresh_stats['successful_refreshes'] += 1
                self.refresh_stats['last_full_refresh'] = end_time
                print(f"[SUCCESS] Materialized views refreshed in {duration:.2f} seconds")
            else:
                self.refresh_stats['failed_refreshes'] += 1
                print(f"[FAILED] Materialized view refresh failed after {duration:.2f} seconds")
            
            # Save refresh statistics
            self._save_refresh_stats()
            
        except Exception as e:
            print(f"[ERROR] Materialized view refresh failed: {e}")
            self.refresh_stats['failed_refreshes'] += 1
            self._save_refresh_stats()
    
    def _incremental_refresh(self) -> bool:
        """Perform incremental refresh of materialized views."""
        try:
            # Check if we need to refresh based on data changes
            if self._should_refresh():
                print("Data changes detected, refreshing materialized views...")
                return self.view_manager.refresh_all_views()
            else:
                print("No data changes detected, skipping refresh")
                return True
        except Exception as e:
            print(f"[ERROR] Incremental refresh failed: {e}")
            return False
    
    def _should_refresh(self) -> bool:
        """Check if materialized views need refresh based on data changes."""
        try:
            # Check last refresh time
            last_refresh = self.refresh_stats.get('last_full_refresh')
            if not last_refresh:
                return True
            
            # Check if enough time has passed
            refresh_interval = self.config['materialized_views'].get('full_refresh_interval_days', 7)
            if isinstance(last_refresh, str):
                last_refresh = datetime.fromisoformat(last_refresh)
            
            time_since_refresh = datetime.now() - last_refresh
            if time_since_refresh.days >= refresh_interval:
                return True
            
            # Check for new data in source tables
            events_table = self.catalog.load_table('ocel.events')
            # This is a simplified check - in production, you'd check actual data timestamps
            return False
            
        except Exception as e:
            print(f"[WARNING] Could not determine if refresh needed: {e}")
            return True  # Default to refresh if uncertain
    
    def _save_refresh_stats(self):
        """Save refresh statistics to file."""
        try:
            stats_file = Path(__file__).parents[1] / 'refresh_stats.json'
            with stats_file.open('w', encoding='utf-8') as f:
                json.dump(self.refresh_stats, f, indent=2, default=str)
        except Exception as e:
            print(f"[WARNING] Could not save refresh stats: {e}")
    
    def _load_refresh_stats(self):
        """Load refresh statistics from file."""
        try:
            stats_file = Path(__file__).parents[1] / 'refresh_stats.json'
            if stats_file.exists():
                with stats_file.open('r', encoding='utf-8') as f:
                    self.refresh_stats = json.load(f)
        except Exception as e:
            print(f"[WARNING] Could not load refresh stats: {e}")
    
    def run_scheduler(self):
        """Run the scheduler continuously."""
        print("Starting materialized view refresh scheduler...")
        print(f"Configuration: {self.config['materialized_views']}")
        
        # Load previous stats
        self._load_refresh_stats()
        
        # Schedule refreshes
        self.schedule_refreshes()
        
        # Run initial refresh if needed
        if self._should_refresh():
            print("Performing initial refresh...")
            self._refresh_views(incremental=False)
        
        print("Scheduler running. Press Ctrl+C to stop.")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            print("\nScheduler stopped by user")
        except Exception as e:
            print(f"[ERROR] Scheduler error: {e}")
    
    def manual_refresh(self, view_name: Optional[str] = None):
        """Manually refresh a specific view or all views."""
        print(f"Manual refresh requested for: {view_name or 'all views'}")
        
        if view_name:
            # Refresh specific view
            if view_name == 'dfg_matrix':
                success = self.view_manager.refresh_dfg_matrix()
            elif view_name == 'activity_metrics':
                success = self.view_manager.refresh_activity_metrics()
            elif view_name == 'process_variants':
                success = self.view_manager.refresh_process_variants()
            else:
                print(f"[ERROR] Unknown view: {view_name}")
                return False
        else:
            # Refresh all views
            success = self.view_manager.refresh_all_views()
        
        if success:
            print(f"[SUCCESS] Manual refresh completed for {view_name or 'all views'}")
        else:
            print(f"[FAILED] Manual refresh failed for {view_name or 'all views'}")
        
        return success
    
    def get_refresh_status(self):
        """Get current refresh status and statistics."""
        print("\n" + "=" * 60)
        print("MATERIALIZED VIEW REFRESH STATUS")
        print("=" * 60)
        print(f"Total refreshes: {self.refresh_stats['total_refreshes']}")
        print(f"Successful: {self.refresh_stats['successful_refreshes']}")
        print(f"Failed: {self.refresh_stats['failed_refreshes']}")
        
        if self.refresh_stats['last_full_refresh']:
            print(f"Last full refresh: {self.refresh_stats['last_full_refresh']}")
        else:
            print("Last full refresh: Never")
        
        # Check view freshness
        try:
            for view_name in ['ocel.dfg_matrix', 'ocel.activity_metrics', 'ocel.process_variants']:
                if self.catalog.table_exists(view_name):
                    table = self.catalog.load_table(view_name)
                    snapshot_count = len(list(table.history))
                    print(f"{view_name}: {snapshot_count} snapshots")
                else:
                    print(f"{view_name}: Not created")
        except Exception as e:
            print(f"[WARNING] Could not check view status: {e}")


def main():
    """Main function for the refresh scheduler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Materialized View Refresh Scheduler')
    parser.add_argument('--config', type=Path, default=Path('lakehouse/agent/config.yaml'),
                       help='Configuration file path')
    parser.add_argument('--manual-refresh', type=str, choices=['dfg_matrix', 'activity_metrics', 'process_variants', 'all'],
                       help='Manually refresh a specific view')
    parser.add_argument('--status', action='store_true',
                       help='Show refresh status and exit')
    parser.add_argument('--daemon', action='store_true',
                       help='Run as daemon (continuous scheduling)')
    
    args = parser.parse_args()
    
    scheduler = ViewRefreshScheduler(args.config)
    
    if args.status:
        scheduler.get_refresh_status()
    elif args.manual_refresh:
        scheduler.manual_refresh(args.manual_refresh)
    elif args.daemon:
        scheduler.run_scheduler()
    else:
        # Default: show status and run one refresh
        scheduler.get_refresh_status()
        print("\nRunning one-time refresh...")
        scheduler._refresh_views(incremental=False)


if __name__ == '__main__':
    main()
