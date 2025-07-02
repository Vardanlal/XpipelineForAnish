import os
import logging
import json
import shutil
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path
import yaml
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/storage.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataManager:
    """
    Comprehensive data manager tool that handles all storage, organization, 
    archival, and data management operations.
    """
    
    def __init__(self, base_dir: str = 'data'):
        """Initialize the DataManager with base directory."""
        self.base_dir = Path(base_dir)
        self.ensure_directories()
        self.config = self._load_config()
        
    def ensure_directories(self):
        """Ensure all necessary directories exist."""
        directories = [
            self.base_dir / 'raw',
            self.base_dir / 'analyzed',
            self.base_dir / 'processed',
            self.base_dir / 'archived',
            self.base_dir / 'backups',
            self.base_dir / 'logs',
            self.base_dir / 'insights',
            self.base_dir / 'reports',
            self.base_dir / 'temp'
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _load_config(self) -> Optional[Dict]:
        """Load configuration from config.yaml."""
        try:
            with open('config.yaml', 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading config: {str(e)}")
            return None
    
    def store_tweet_data(self, tweets: List[Dict], username: str, data_type: str = 'raw') -> Dict:
        """
        Store tweet data in organized files under output/{date}/tweets/raw/{username}/
        """
        try:
            if not tweets:
                return {
                    "status": "error",
                    "message": f"No tweets to store for {username}"
                }
            date_str = datetime.now().strftime('%Y-%m-%d')
            base_dir = Path(f'output/{date_str}/tweets/raw/{username}') if data_type == 'raw' else Path(f'output/{date_str}/tweets/analyzed/{username}')
            base_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{username}_{data_type}_{timestamp}.json"
            filepath = base_dir / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(tweets, f, ensure_ascii=False, indent=2)
            logger.info(f"Stored {len(tweets)} tweets for {username} in {filepath}")
            return {
                "status": "success",
                "message": f"Successfully stored {len(tweets)} tweets for {username}",
                "filepath": str(filepath),
                "filename": filename,
                "data_type": data_type,
                "tweet_count": len(tweets)
            }
        except Exception as e:
            logger.error(f"Error storing tweet data for {username}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error storing tweet data for {username}: {str(e)}"
            }
    
    def store_analysis_results(self, analysis_data: Dict, username: str) -> Dict:
        """
        Store analysis results for a user under output/{date}/tweets/analyzed/{username}/
        """
        try:
            date_str = datetime.now().strftime('%Y-%m-%d')
            base_dir = Path(f'output/{date_str}/tweets/analyzed/{username}')
            base_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{username}_analysis_{timestamp}.json"
            filepath = base_dir / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(analysis_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Stored analysis results for {username} in {filepath}")
            return {
                "status": "success",
                "message": f"Successfully stored analysis results for {username}",
                "filepath": str(filepath),
                "filename": filename
            }
        except Exception as e:
            logger.error(f"Error storing analysis results for {username}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error storing analysis results for {username}: {str(e)}"
            }
    
    def store_insights_report(self, insights_data: Dict, username: str) -> Dict:
        """
        Store insights report for a user.
        
        Args:
            insights_data (Dict): Insights data to store
            username (str): Twitter username
            
        Returns:
            Dict: Storage results
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{username}_insights_{timestamp}.json"
            filepath = self.base_dir / 'insights' / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(insights_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Stored insights report for {username} in {filepath}")
            
            return {
                "status": "success",
                "message": f"Successfully stored insights report for {username}",
                "filepath": str(filepath),
                "filename": filename
            }
            
        except Exception as e:
            logger.error(f"Error storing insights report for {username}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error storing insights report for {username}: {str(e)}"
            }
    
    def organize_data_by_date(self, date: str = None) -> Dict:
        """
        Organize data files by date structure.
        
        Args:
            date (str): Date in YYYY-MM-DD format, defaults to today
            
        Returns:
            Dict: Organization results
        """
        try:
            if date is None:
                date = datetime.now().strftime('%Y-%m-%d')
            
            date_dir = self.base_dir / date
            date_dir.mkdir(exist_ok=True)
            
            # Create subdirectories for the date
            subdirs = ['raw', 'analyzed', 'processed', 'insights']
            for subdir in subdirs:
                (date_dir / subdir).mkdir(exist_ok=True)
            
            # Move files to appropriate date directories
            moved_files = {
                'raw': [],
                'analyzed': [],
                'processed': [],
                'insights': []
            }
            
            # Move files from main directories
            for subdir in subdirs:
                source_dir = self.base_dir / subdir
                if source_dir.exists():
                    for file in source_dir.glob('*.json'):
                        if self._should_move_file(file, date):
                            new_path = date_dir / subdir / file.name
                            shutil.move(str(file), str(new_path))
                            moved_files[subdir].append(file.name)
            
            return {
                "status": "success",
                "message": f"Organized data for {date}",
                "date": date,
                "moved_files": moved_files
            }
            
        except Exception as e:
            logger.error(f"Error organizing data: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def _should_move_file(self, file_path: Path, target_date: str) -> bool:
        """Determine if a file should be moved to the target date directory."""
        try:
            # Check if file name contains the target date
            return target_date.replace('-', '') in file_path.name
        except Exception:
            return False
    
    def cleanup_old_data(self, days_to_keep: int = 30) -> Dict:
        """
        Clean up data older than specified days.
        
        Args:
            days_to_keep (int): Number of days to keep data
            
        Returns:
            Dict: Cleanup results
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            deleted_files = []
            
            # Clean up date-based directories
            for date_dir in self.base_dir.iterdir():
                if date_dir.is_dir() and date_dir.name not in ['archived', 'backups', 'logs']:
                    try:
                        dir_date = datetime.strptime(date_dir.name, '%Y-%m-%d')
                        if dir_date < cutoff_date:
                            shutil.rmtree(str(date_dir))
                            deleted_files.append(date_dir.name)
                    except ValueError:
                        # Skip directories that don't match date format
                        continue
            
            # Clean up loose files in main directories
            for subdir in ['raw', 'analyzed', 'processed', 'temp']:
                subdir_path = self.base_dir / subdir
                if subdir_path.exists():
                    for file in subdir_path.glob('*.json'):
                        if self._is_file_old(file, cutoff_date):
                            file.unlink()
                            deleted_files.append(str(file))
            
            return {
                "status": "success",
                "message": f"Cleaned up data older than {days_to_keep} days",
                "deleted_files": deleted_files,
                "cutoff_date": cutoff_date.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error cleaning up old data: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def _is_file_old(self, file_path: Path, cutoff_date: datetime) -> bool:
        """Check if a file is older than the cutoff date."""
        try:
            file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
            return file_time < cutoff_date
        except Exception:
            return False
    
    def create_backup(self, backup_name: str = None) -> Dict:
        """
        Create a backup of the data directory.
        
        Args:
            backup_name (str): Name for the backup, defaults to timestamp
            
        Returns:
            Dict: Backup results
        """
        try:
            if backup_name is None:
                backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            backup_dir = self.base_dir / 'backups' / backup_name
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Copy data files to backup
            for item in self.base_dir.iterdir():
                if item.is_dir() and item.name not in ['backups', 'archived', 'logs']:
                    shutil.copytree(str(item), str(backup_dir / item.name))
                elif item.is_file():
                    shutil.copy2(str(item), str(backup_dir / item.name))
            
            return {
                "status": "success",
                "message": f"Created backup: {backup_name}",
                "backup_path": str(backup_dir)
            }
            
        except Exception as e:
            logger.error(f"Error creating backup: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def archive_data(self, archive_name: str = None) -> Dict:
        """
        Archive data by compressing it.
        
        Args:
            archive_name (str): Name for the archive, defaults to timestamp
            
        Returns:
            Dict: Archive results
        """
        try:
            if archive_name is None:
                archive_name = f"archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            archive_path = self.base_dir / 'archived' / f"{archive_name}.zip"
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create zip archive
            shutil.make_archive(
                str(archive_path.with_suffix('')),
                'zip',
                str(self.base_dir),
                '.'
            )
            
            return {
                "status": "success",
                "message": f"Created archive: {archive_name}",
                "archive_path": str(archive_path)
            }
            
        except Exception as e:
            logger.error(f"Error creating archive: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def get_data_summary(self) -> Dict:
        """
        Get a summary of data storage usage.
        
        Returns:
            Dict: Data summary including file counts and sizes
        """
        try:
            summary = {
                "total_size": 0,
                "file_counts": {},
                "directory_sizes": {}
            }
            
            for item in self.base_dir.rglob('*'):
                if item.is_file():
                    size = item.stat().st_size
                    summary["total_size"] += size
                    
                    # Count files by directory
                    parent_dir = item.parent.name
                    if parent_dir not in summary["file_counts"]:
                        summary["file_counts"][parent_dir] = 0
                        summary["directory_sizes"][parent_dir] = 0
                    
                    summary["file_counts"][parent_dir] += 1
                    summary["directory_sizes"][parent_dir] += size
            
            # Convert sizes to human readable format
            summary["total_size_mb"] = summary["total_size"] / (1024 * 1024)
            
            return {
                "status": "success",
                "summary": summary
            }
            
        except Exception as e:
            logger.error(f"Error getting data summary: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def validate_data_integrity(self) -> Dict:
        """
        Validate the integrity of stored data files.
        
        Returns:
            Dict: Validation results
        """
        try:
            validation_results = {
                "valid_files": [],
                "invalid_files": [],
                "total_files": 0
            }
            
            for json_file in self.base_dir.rglob('*.json'):
                validation_results["total_files"] += 1
                
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        json.load(f)
                    validation_results["valid_files"].append(str(json_file))
                except Exception as e:
                    validation_results["invalid_files"].append({
                        "file": str(json_file),
                        "error": str(e)
                    })
            
            return {
                "status": "success",
                "validation_results": validation_results
            }
            
        except Exception as e:
            logger.error(f"Error validating data integrity: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def retrieve_user_data(self, username: str, data_type: str = 'all') -> Dict:
        """
        Retrieve stored data for a specific user.
        
        Args:
            username (str): Twitter username
            data_type (str): Type of data to retrieve (raw, analyzed, insights, all)
            
        Returns:
            Dict: Retrieved data
        """
        try:
            retrieved_data = {}
            
            if data_type in ['raw', 'all']:
                raw_files = list(self.base_dir.glob(f'raw/{username}_raw_*.json'))
                if raw_files:
                    latest_raw = max(raw_files, key=lambda x: x.stat().st_mtime)
                    with open(latest_raw, 'r', encoding='utf-8') as f:
                        retrieved_data['raw'] = json.load(f)
            
            if data_type in ['analyzed', 'all']:
                analyzed_files = list(self.base_dir.glob(f'analyzed/{username}_analyzed_*.json'))
                if analyzed_files:
                    latest_analyzed = max(analyzed_files, key=lambda x: x.stat().st_mtime)
                    with open(latest_analyzed, 'r', encoding='utf-8') as f:
                        retrieved_data['analyzed'] = json.load(f)
            
            if data_type in ['insights', 'all']:
                insights_files = list(self.base_dir.glob(f'insights/{username}_insights_*.json'))
                if insights_files:
                    latest_insights = max(insights_files, key=lambda x: x.stat().st_mtime)
                    with open(latest_insights, 'r', encoding='utf-8') as f:
                        retrieved_data['insights'] = json.load(f)
            
            return {
                "status": "success",
                "username": username,
                "data_type": data_type,
                "data": retrieved_data
            }
            
        except Exception as e:
            logger.error(f"Error retrieving data for {username}: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def export_data(self, username: str, export_format: str = 'json') -> Dict:
        """
        Export user data in specified format.
        
        Args:
            username (str): Twitter username
            export_format (str): Export format (json, csv)
            
        Returns:
            Dict: Export results
        """
        try:
            # Retrieve all user data
            user_data = self.retrieve_user_data(username, 'all')
            if user_data["status"] != "success":
                return user_data
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if export_format == 'json':
                filename = f"{username}_export_{timestamp}.json"
                filepath = self.base_dir / 'reports' / filename
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(user_data["data"], f, ensure_ascii=False, indent=2)
            
            elif export_format == 'csv':
                # This would require additional CSV processing logic
                filename = f"{username}_export_{timestamp}.csv"
                filepath = self.base_dir / 'reports' / filename
                
                # Placeholder for CSV export
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write("Export format not yet implemented\n")
            
            return {
                "status": "success",
                "message": f"Exported data for {username} in {export_format} format",
                "filepath": str(filepath),
                "filename": filename
            }
            
        except Exception as e:
            logger.error(f"Error exporting data for {username}: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def get_storage_statistics(self) -> Dict:
        """
        Get detailed storage statistics.
        
        Returns:
            Dict: Storage statistics
        """
        try:
            stats = {
                "total_files": 0,
                "total_size_bytes": 0,
                "total_size_mb": 0,
                "by_directory": {},
                "by_user": {},
                "recent_activity": []
            }
            
            # Collect statistics
            for item in self.base_dir.rglob('*'):
                if item.is_file():
                    size = item.stat().st_size
                    stats["total_files"] += 1
                    stats["total_size_bytes"] += size
                    
                    # By directory
                    parent_dir = item.parent.name
                    if parent_dir not in stats["by_directory"]:
                        stats["by_directory"][parent_dir] = {"files": 0, "size": 0}
                    stats["by_directory"][parent_dir]["files"] += 1
                    stats["by_directory"][parent_dir]["size"] += size
                    
                    # By user (extract username from filename)
                    if '_' in item.name:
                        username = item.name.split('_')[0]
                        if username not in stats["by_user"]:
                            stats["by_user"][username] = {"files": 0, "size": 0}
                        stats["by_user"][username]["files"] += 1
                        stats["by_user"][username]["size"] += size
                    
                    # Recent activity
                    mtime = datetime.fromtimestamp(item.stat().st_mtime)
                    if mtime > datetime.now() - timedelta(days=7):
                        stats["recent_activity"].append({
                            "file": str(item),
                            "modified": mtime.isoformat(),
                            "size": size
                        })
            
            stats["total_size_mb"] = stats["total_size_bytes"] / (1024 * 1024)
            
            return {
                "status": "success",
                "statistics": stats
            }
            
        except Exception as e:
            logger.error(f"Error getting storage statistics: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            } 