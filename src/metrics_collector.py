import time
import psutil
from typing import Dict, List
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class MetricsCollector:
    def __init__(self):
        self.start_time = None
        self.metrics_history = []
        
    def start_collection(self):
        """Start metrics collection."""
        self.start_time = time.time()
        self.metrics_history = []
        
    def collect_current_metrics(self) -> Dict:
        """Collect current system metrics."""
        cpu_metrics = psutil.cpu_times_percent()
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        metrics = {
            'timestamp': datetime.now(),
            'elapsed_time': time.time() - self.start_time if self.start_time else 0,
            'cpu': {
                'user': cpu_metrics.user,
                'system': cpu_metrics.system,
                'idle': cpu_metrics.idle,
                'percent': psutil.cpu_percent(interval=1)
            },
            'memory': {
                'total': memory.total,
                'available': memory.available,
                'used': memory.used,
                'percent': memory.percent
            },
            'disk': {
                'total': disk.total,
                'used': disk.used,
                'free': disk.free,
                'percent': disk.percent
            }
        }
        
        # Add network I/O stats
        net_io = psutil.net_io_counters()
        metrics['network'] = {
            'bytes_sent': net_io.bytes_sent,
            'bytes_recv': net_io.bytes_recv,
            'packets_sent': net_io.packets_sent,
            'packets_recv': net_io.packets_recv
        }
        
        self.metrics_history.append(metrics)
        return metrics
    
    def get_metrics_summary(self) -> Dict:
        """Get summary of collected metrics."""
        if not self.metrics_history:
            return {}
            
        cpu_percentages = [m['cpu']['percent'] for m in self.metrics_history]
        memory_percentages = [m['memory']['percent'] for m in self.metrics_history]
        
        return {
            'duration': self.metrics_history[-1]['elapsed_time'],
            'cpu': {
                'avg_utilization': sum(cpu_percentages) / len(cpu_percentages),
                'peak_utilization': max(cpu_percentages),
                'min_utilization': min(cpu_percentages)
            },
            'memory': {
                'avg_utilization': sum(memory_percentages) / len(memory_percentages),
                'peak_utilization': max(memory_percentages),
                'min_utilization': min(memory_percentages),
                'peak_used': max(m['memory']['used'] for m in self.metrics_history)
            },
            'network': {
                'total_bytes_sent': self.metrics_history[-1]['network']['bytes_sent'],
                'total_bytes_recv': self.metrics_history[-1]['network']['bytes_recv'],
                'total_packets_sent': self.metrics_history[-1]['network']['packets_sent'],
                'total_packets_recv': self.metrics_history[-1]['network']['packets_recv']
            }
        }
    
    def log_current_metrics(self):
        """Log current metrics in a human-readable format."""
        metrics = self.collect_current_metrics()
        
        logger.info("Current System Metrics:")
        logger.info(f"CPU Usage: {metrics['cpu']['percent']}%")
        logger.info(f"Memory Usage: {metrics['memory']['percent']}%")
        logger.info(f"Disk Usage: {metrics['disk']['percent']}%")
        logger.info(f"Network - Sent: {metrics['network']['bytes_sent'] / 1024**2:.2f} MB, "
                   f"Received: {metrics['network']['bytes_recv'] / 1024**2:.2f} MB") 