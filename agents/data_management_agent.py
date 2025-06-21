import os
import logging
import json
import shutil
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

class DataManagementAgent:
    """
    Agent responsible for managing data organization, cleanup, and archival.
    Handles data lifecycle management and storage optimization.
    """
    
    def __init__(self, base_dir: str = 'data'):
        """Initialize the DataManagementAgent."""
        self.base_dir = Path(base_dir)
        self.ensure_directories()
        
    def ensure_directories(self):
        """Ensure all necessary directories exist."""
        directories = [
            self.base_dir / 'raw',
            self.base_dir / 'analyzed',
            self.base_dir / 'processed',
            self.base_dir / 'archived',
            self.base_dir / 'backups',
            self.base_dir / 'logs'
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
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
            subdirs = ['raw', 'analyzed', 'processed']
            for subdir in subdirs:
                (date_dir / subdir).mkdir(exist_ok=True)
            
            # Move files to appropriate date directories
            moved_files = {
                'raw': [],
                'analyzed': [],
                'processed': []
            }
            
            # Move raw files
            raw_dir = self.base_dir / 'raw'
            for file in raw_dir.glob('*.json'):
                if self._should_move_file(file, date):
                    new_path = date_dir / 'raw' / file.name
                    shutil.move(str(file), str(new_path))
                    moved_files['raw'].append(file.name)
            
            # Move analyzed files
            analyzed_dir = self.base_dir / 'analyzed'
            for file in analyzed_dir.glob('*.json'):
                if self._should_move_file(file, date):
                    new_path = date_dir / 'analyzed' / file.name
                    shutil.move(str(file), str(new_path))
                    moved_files['analyzed'].append(file.name)
            
            # Move processed files
            processed_dir = self.base_dir / 'processed'
            for file in processed_dir.glob('*.json'):
                if self._should_move_file(file, date):
                    new_path = date_dir / 'processed' / file.name
                    shutil.move(str(file), str(new_path))
                    moved_files['processed'].append(file.name)
            
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
                if date_dir.is_dir() and date_dir.name != 'archived' and date_dir.name != 'backups':
                    try:
                        dir_date = datetime.strptime(date_dir.name, '%Y-%m-%d')
                        if dir_date < cutoff_date:
                            shutil.rmtree(str(date_dir))
                            deleted_files.append(date_dir.name)
                    except ValueError:
                        # Skip directories that don't match date format
                        continue
            
            # Clean up loose files in main directories
            for subdir in ['raw', 'analyzed', 'processed']:
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
                if item.is_dir() and item.name not in ['backups', 'archived']:
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