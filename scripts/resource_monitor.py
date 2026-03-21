#!/usr/bin/env python3
"""
Resource monitoring utility for tracking memory and disk usage

Uses psutil to monitor the main process and all subprocesses.
Runs in a background thread and tracks peak/average resource usage.
"""

import threading
import time
import shutil
from pathlib import Path

try:
    import psutil
except ImportError:
    psutil = None


class ResourceMonitor:
    """
    Monitor system resources (RAM and disk) during script execution.
    
    Tracks the main Python process and all child processes spawned via subprocess.
    Polls periodically in a background thread to capture peak usage.
    """
    
    def __init__(self, working_dir=None, poll_interval=1.0):
        """
        Initialize the resource monitor.
        
        Args:
            working_dir: Directory to monitor for disk usage (default: current directory)
            poll_interval: How often to poll resources in seconds (default: 1.0)
        """
        self.poll_interval = poll_interval
        self.working_dir = Path(working_dir) if working_dir else Path.cwd()
        
        # Monitoring state
        self.monitoring = False
        self.monitor_thread = None
        
        # Metrics
        self.memory_samples = []  # List of (timestamp, total_mb, main_mb, children_mb)
        self.start_disk_free = None
        self.end_disk_free = None
        self.start_time = None
        self.end_time = None
        
        # psutil availability
        self.psutil_available = psutil is not None
        
        if not self.psutil_available:
            print("[WARNING] psutil not available - memory monitoring disabled")
            print("          Install with: pip install psutil --break-system-packages")
    
    def start(self):
        """Start monitoring resources"""
        self.start_time = time.time()
        
        # Record starting disk space
        try:
            disk_usage = shutil.disk_usage(self.working_dir)
            self.start_disk_free = disk_usage.free
        except Exception as e:
            print(f"[WARNING] Could not read disk usage: {e}")
            self.start_disk_free = None
        
        # Start memory monitoring thread if psutil is available
        if self.psutil_available:
            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()
    
    def stop(self):
        """
        Stop monitoring and return statistics.
        
        Returns:
            dict: Resource usage statistics
        """
        self.end_time = time.time()
        
        # Stop monitoring thread
        if self.monitoring:
            self.monitoring = False
            if self.monitor_thread:
                self.monitor_thread.join(timeout=2.0)
        
        # Record ending disk space
        try:
            disk_usage = shutil.disk_usage(self.working_dir)
            self.end_disk_free = disk_usage.free
        except Exception as e:
            print(f"[WARNING] Could not read disk usage: {e}")
            self.end_disk_free = None
        
        # Calculate statistics
        return self._calculate_stats()
    
    def _monitor_loop(self):
        """Background thread that periodically samples memory usage"""
        try:
            main_process = psutil.Process()
            
            while self.monitoring:
                try:
                    timestamp = time.time() - self.start_time
                    
                    # Get main process memory
                    main_mem = main_process.memory_info().rss / (1024 * 1024)  # MB
                    
                    # Get all child processes memory
                    children_mem = 0
                    try:
                        children = main_process.children(recursive=True)
                        for child in children:
                            try:
                                children_mem += child.memory_info().rss / (1024 * 1024)
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                pass
                    except Exception:
                        pass
                    
                    total_mem = main_mem + children_mem
                    
                    # Record sample
                    self.memory_samples.append((timestamp, total_mem, main_mem, children_mem))
                    
                except Exception as e:
                    # Continue monitoring even if one sample fails
                    pass
                
                time.sleep(self.poll_interval)
                
        except Exception as e:
            print(f"[WARNING] Memory monitoring thread error: {e}")
    
    def _calculate_stats(self):
        """Calculate statistics from collected samples"""
        stats = {
            'elapsed_seconds': self.end_time - self.start_time if self.end_time and self.start_time else 0,
            'psutil_available': self.psutil_available,
        }
        
        # Memory statistics
        if self.psutil_available and self.memory_samples:
            total_mems = [s[1] for s in self.memory_samples]
            main_mems = [s[2] for s in self.memory_samples]
            children_mems = [s[3] for s in self.memory_samples]
            
            stats['memory'] = {
                'peak_total_mb': max(total_mems),
                'avg_total_mb': sum(total_mems) / len(total_mems),
                'peak_main_mb': max(main_mems),
                'avg_main_mb': sum(main_mems) / len(main_mems),
                'peak_children_mb': max(children_mems),
                'avg_children_mb': sum(children_mems) / len(children_mems),
                'samples': len(self.memory_samples),
            }
            
            # Find when peak occurred
            peak_idx = total_mems.index(max(total_mems))
            stats['memory']['peak_at_seconds'] = self.memory_samples[peak_idx][0]
        else:
            stats['memory'] = None
        
        # Disk statistics
        if self.start_disk_free is not None and self.end_disk_free is not None:
            disk_used = self.start_disk_free - self.end_disk_free
            stats['disk'] = {
                'start_free_gb': self.start_disk_free / (1024**3),
                'end_free_gb': self.end_disk_free / (1024**3),
                'used_gb': disk_used / (1024**3),
            }
        else:
            stats['disk'] = None
        
        return stats


def format_memory_mb(mb):
    """Format memory in MB to human-readable string"""
    if mb >= 1024:
        return f"{mb/1024:.2f} GB"
    else:
        return f"{mb:.1f} MB"


def format_time_seconds(seconds):
    """Format seconds to human-readable string"""
    if seconds >= 60:
        minutes = int(seconds / 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        return f"{seconds:.1f}s"


def print_resource_summary(stats, stage_name=None):
    """
    Print a formatted resource usage summary.
    
    Args:
        stats: Statistics dictionary from ResourceMonitor.stop()
        stage_name: Optional name of the stage (e.g., "Contours Generation")
    """
    print()
    print("=" * 70)
    if stage_name:
        print(f"RESOURCE USAGE - {stage_name}")
    else:
        print("RESOURCE USAGE SUMMARY")
    print("=" * 70)
    
    # Memory statistics
    if stats.get('psutil_available') and stats.get('memory'):
        mem = stats['memory']
        print("Memory (RAM):")
        print(f"  Peak total:          {format_memory_mb(mem['peak_total_mb'])}")
        print(f"  Average total:       {format_memory_mb(mem['avg_total_mb'])}")
        print(f"    Peak at:           {format_time_seconds(mem['peak_at_seconds'])} into processing")
        print()
        print(f"  Peak main process:   {format_memory_mb(mem['peak_main_mb'])}")
        print(f"  Peak subprocesses:   {format_memory_mb(mem['peak_children_mb'])}")
        print(f"    (Samples taken:    {mem['samples']} over {format_time_seconds(stats['elapsed_seconds'])})")
    else:
        if not stats.get('psutil_available'):
            print("Memory (RAM):        Not monitored (psutil not available)")
        else:
            print("Memory (RAM):        No samples collected")
    
    print()
    
    # Disk statistics
    if stats.get('disk'):
        disk = stats['disk']
        print("Disk Space:")
        print(f"  Starting free:       {disk['start_free_gb']:.1f} GB")
        print(f"  Ending free:         {disk['end_free_gb']:.1f} GB")
        
        if disk['used_gb'] > 0:
            print(f"  Space used:          {disk['used_gb']:.1f} GB")
        elif disk['used_gb'] < 0:
            print(f"  Space freed:         {abs(disk['used_gb']):.1f} GB")
        else:
            print(f"  Space used:          0.0 GB (no change)")
    else:
        print("Disk Space:          Not monitored")
    
    print()
    print("=" * 70)
    print()
