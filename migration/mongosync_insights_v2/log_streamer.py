"""
Log file streaming module for real-time log monitoring and parsing.
"""
import json
import re
import logging
import threading
import time
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class LogParser:
    """Parses mongosync log entries using regex patterns."""
    
    def __init__(self):
        # Pre-compile all regex patterns once
        self.patterns = {
            'replication_progress': re.compile(r"Replication progress", re.IGNORECASE),
            'version_info': re.compile(r"Version info", re.IGNORECASE),
            'operation_stats': re.compile(r"Operation duration stats", re.IGNORECASE),
            'sent_response': re.compile(r"sent response", re.IGNORECASE),
            'phase_transitions': re.compile(
                r"Starting initializing collections and indexes phase|"
                r"Starting initializing partitions phase|"
                r"Starting collection copy phase|"
                r"Starting change event application phase|"
                r"Commit handler called",
                re.IGNORECASE
            ),
            'mongosync_options': re.compile(r"Mongosync Options", re.IGNORECASE),
            'hidden_flags': re.compile(r"Mongosync HiddenFlags", re.IGNORECASE)
        }
    
    def parse_line(self, line: str) -> Optional[Dict[str, Any]]:
        """
        Parse a single log line and return categorized data.
        
        Args:
            line: Log line to parse
            
        Returns:
            Dictionary with metric type as key and parsed JSON object as value,
            or None if line doesn't match any pattern or is invalid
        """
        line = line.strip()
        
        if not line or not line.startswith('{'):
            return None
        
        try:
            json_obj = json.loads(line)
            message = json_obj.get('message', '')
            
            # Check which pattern matches
            for pattern_name, pattern in self.patterns.items():
                if pattern.search(message):
                    result = {'type': pattern_name, 'data': json_obj}
                    
                    # Filter out time and level fields for options and hidden flags
                    if pattern_name in ('mongosync_options', 'hidden_flags'):
                        filtered_obj = {
                            k: v for k, v in json_obj.items() 
                            if k not in ('time', 'level')
                        }
                        result['data'] = filtered_obj
                    
                    return result
            
            return None
            
        except json.JSONDecodeError:
            # Skip invalid JSON lines silently
            return None
        except Exception as e:
            logger.warning(f"Error parsing log line: {e}")
            return None


class LogFileMonitor:
    """
    Monitors a log file for new entries and maintains parsed metrics.
    Thread-safe implementation for concurrent access.
    """
    
    def __init__(self, file_path: str, update_interval: float = 1.0, max_buffer_size: int = 1000, read_from_start: bool = False):
        """
        Initialize log file monitor.
        
        Args:
            file_path: Path to the log file to monitor
            update_interval: Polling interval in seconds
            max_buffer_size: Maximum number of entries to buffer per metric type
            read_from_start: If True, read from beginning of file; if False, start at end (tail behavior)
        """
        self.file_path = Path(file_path)
        self.update_interval = update_interval
        self.max_buffer_size = max_buffer_size
        self.read_from_start = read_from_start
        
        self.parser = LogParser()
        self.lock = threading.Lock()
        
        # Thread-safe storage for parsed metrics
        self.metrics = {
            'replication_progress': [],
            'version_info': [],
            'operation_stats': [],
            'sent_response': [],
            'phase_transitions': [],
            'mongosync_options': [],
            'hidden_flags': []
        }
        
        # Storage for raw log lines (for display in bottom frame)
        self.raw_lines = []
        
        # File position tracking
        self.file_position = 0
        self.file_size = 0
        self.last_modified = 0
        
        # Control flags
        self.running = False
        self.monitor_thread = None
        
        # Statistics
        self.total_lines_processed = 0
        self.invalid_json_count = 0
    
    def start(self):
        """Start the monitoring thread. Waits for file to appear if it does not exist yet."""
        if self.running:
            logger.warning("Monitor is already running")
            return

        # Initialize file position (or zero if file does not exist yet)
        try:
            if self.file_path.exists():
                self.file_size = self.file_path.stat().st_size
                if self.read_from_start:
                    self.file_position = 0
                    logger.info(f"Starting monitor at beginning of file (position: 0, size: {self.file_size})")
                else:
                    self.file_position = self.file_size
                    logger.info(f"Starting monitor at end of file (position: {self.file_position}, size: {self.file_size})")
            else:
                self.file_position = 0
                self.file_size = 0
                logger.info(f"Log file does not exist yet, will poll for {self.file_path}")
        except Exception as e:
            logger.error(f"Error initializing file position: {e}")
            return

        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info(f"Started monitoring log file: {self.file_path}")
    
    def stop(self):
        """Stop the monitoring thread."""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
        logger.info("Stopped monitoring log file")
    
    def _monitor_loop(self):
        """Main monitoring loop running in background thread."""
        while self.running:
            try:
                self._check_file()
                time.sleep(self.update_interval)
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                time.sleep(self.update_interval)
    
    def _check_file(self):
        """Check for new lines in the log file."""
        try:
            if not self.file_path.exists():
                logger.warning(f"Log file not found: {self.file_path}")
                return
            
            current_size = self.file_path.stat().st_size
            
            # Handle file rotation (size decreased)
            if current_size < self.file_size:
                logger.info("Detected file rotation, resetting position")
                with self.lock:
                    self.file_position = 0
                    self.file_size = current_size
                    # Optionally clear metrics or keep them
                    # For now, we'll keep existing metrics
            
            # Read new lines if file has grown
            if current_size > self.file_position:
                self._read_new_lines()
                self.file_size = current_size
                
        except PermissionError:
            logger.error(f"Permission denied reading log file: {self.file_path}")
        except Exception as e:
            logger.error(f"Error checking file: {e}")
    
    def _read_new_lines(self):
        """Read and parse new lines from the log file."""
        try:
            with open(self.file_path, 'r', encoding='utf-8', errors='replace') as f:
                # Seek to last known position
                f.seek(self.file_position)
                
                # Read all new lines
                new_lines = []
                for line in f:
                    new_lines.append(line)
                    self.total_lines_processed += 1
                
                # Update file position
                self.file_position = f.tell()
                
                # Parse new lines
                if new_lines:
                    logger.debug(f"Read {len(new_lines)} new lines from position {self.file_position - sum(len(line) for line in new_lines)}")
                    self._parse_lines(new_lines)
                else:
                    logger.debug("No new lines to parse")
                    
        except Exception as e:
            logger.error(f"Error reading new lines: {e}", exc_info=True)
    
    def _parse_lines(self, lines: List[str]):
        """Parse lines and update metrics storage."""
        parsed_count = 0
        with self.lock:
            for line in lines:
                # Store raw line for display (strip whitespace for cleaner display)
                stripped_line = line.strip()
                if stripped_line:
                    self.raw_lines.append(stripped_line)
                    
                    # Enforce buffer size limit for raw lines
                    if len(self.raw_lines) > self.max_buffer_size:
                        excess = len(self.raw_lines) - self.max_buffer_size
                        self.raw_lines = self.raw_lines[excess:]
                
                parsed = self.parser.parse_line(line)
                
                if parsed is None:
                    # Check if it's invalid JSON (starts with { but failed to parse)
                    if stripped_line.startswith('{'):
                        self.invalid_json_count += 1
                    continue
                
                metric_type = parsed['type']
                data = parsed['data']
                
                # Add to appropriate metric list
                if metric_type in self.metrics:
                    self.metrics[metric_type].append(data)
                    parsed_count += 1
                    
                    # Enforce buffer size limit
                    if len(self.metrics[metric_type]) > self.max_buffer_size:
                        # Remove oldest entries
                        excess = len(self.metrics[metric_type]) - self.max_buffer_size
                        self.metrics[metric_type] = self.metrics[metric_type][excess:]
        
        if parsed_count > 0:
            logger.debug(f"Parsed {parsed_count} metric entries from {len(lines)} lines")
    
    def get_new_metrics(self, last_sync_position: Dict[str, int] = None) -> Dict[str, List]:
        """
        Get new metrics since last sync.
        
        Args:
            last_sync_position: Dictionary mapping metric types to last seen index
            
        Returns:
            Dictionary of new metrics since last sync
        """
        if last_sync_position is None:
            last_sync_position = {}
        
        with self.lock:
            new_metrics = {}
            
            for metric_type, metric_list in self.metrics.items():
                last_index = last_sync_position.get(metric_type, 0)
                
                if last_index < len(metric_list):
                    new_metrics[metric_type] = metric_list[last_index:]
                else:
                    new_metrics[metric_type] = []
            
            return new_metrics
    
    def get_all_metrics(self) -> Dict[str, List]:
        """Get all current metrics (thread-safe copy)."""
        with self.lock:
            return {k: v.copy() for k, v in self.metrics.items()}
    
    def get_all_raw_lines(self) -> List[str]:
        """Get all current raw log lines (thread-safe copy)."""
        with self.lock:
            return self.raw_lines.copy()
    
    def get_new_raw_lines(self, last_index: int = 0) -> List[str]:
        """Get new raw log lines since last index."""
        with self.lock:
            if last_index < len(self.raw_lines):
                return self.raw_lines[last_index:]
            return []
    
    def get_raw_lines_count(self) -> int:
        """Get current count of raw log lines."""
        with self.lock:
            return len(self.raw_lines)
    
    def get_sync_position(self) -> Dict[str, int]:
        """Get current sync position (length of each metric list)."""
        with self.lock:
            return {k: len(v) for k, v in self.metrics.items()}
    
    def get_full_sync_position(self) -> Dict[str, Any]:
        """Get current sync position including raw lines count."""
        with self.lock:
            position = {k: len(v) for k, v in self.metrics.items()}
            position['raw_lines'] = len(self.raw_lines)
            return position
    
    def get_stats(self) -> Dict[str, Any]:
        """Get monitoring statistics."""
        with self.lock:
            return {
                'total_lines_processed': self.total_lines_processed,
                'invalid_json_count': self.invalid_json_count,
                'file_position': self.file_position,
                'file_size': self.file_size,
                'metrics_count': {k: len(v) for k, v in self.metrics.items()},
                'running': self.running
            }


# Global monitor instance (singleton pattern)
_monitor_instance: Optional[LogFileMonitor] = None
_monitor_lock = threading.Lock()


def get_monitor(file_path: str = None, update_interval: float = 1.0, max_buffer_size: int = 1000) -> LogFileMonitor:
    """
    Get or create the global log file monitor instance.
    
    Args:
        file_path: Path to log file (required for first call)
        update_interval: Polling interval in seconds
        max_buffer_size: Maximum buffer size per metric
        
    Returns:
        LogFileMonitor instance
    """
    global _monitor_instance
    
    with _monitor_lock:
        # Detect if this is an uploaded file (should read from start)
        read_from_start = file_path and 'mongosync_upload' in file_path
        
        if _monitor_instance is None:
            if file_path is None:
                raise ValueError("file_path is required for first call")
            logger.info(f"Creating new monitor for {file_path}, read_from_start={read_from_start}")
            _monitor_instance = LogFileMonitor(file_path, update_interval, max_buffer_size, read_from_start)
            _monitor_instance.start()
        elif file_path is not None and _monitor_instance.file_path != Path(file_path):
            # File path changed, recreate monitor
            logger.info(f"File path changed, recreating monitor for {file_path}, read_from_start={read_from_start}")
            _monitor_instance.stop()
            _monitor_instance = LogFileMonitor(file_path, update_interval, max_buffer_size, read_from_start)
            _monitor_instance.start()
        
        return _monitor_instance


def stop_monitor():
    """Stop the global monitor instance."""
    global _monitor_instance
    
    with _monitor_lock:
        if _monitor_instance is not None:
            _monitor_instance.stop()
            _monitor_instance = None
