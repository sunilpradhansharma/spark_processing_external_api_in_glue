import os
import time
import psutil
import logging
from datetime import datetime
import matplotlib.pyplot as plt
from typing import List, Tuple
import json

class ProcessMonitor:
    def __init__(self, pid: int = None):
        self.pid = pid or os.getpid()
        self.process = psutil.Process(self.pid)
        self.start_time = datetime.now()
        self.metrics: List[dict] = []
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename=os.path.join('logs', 'monitor.log')
        )
        
    def collect_metrics(self) -> dict:
        """Collect current process metrics"""
        cpu_percent = self.process.cpu_percent()
        memory_info = self.process.memory_info()
        
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'cpu_percent': cpu_percent,
            'memory_rss_mb': memory_info.rss / 1024 / 1024,
            'memory_vms_mb': memory_info.vms / 1024 / 1024,
            'threads': self.process.num_threads(),
            'open_files': len(self.process.open_files()),
            'connections': len(self.process.connections())
        }
        
        self.metrics.append(metrics)
        return metrics
        
    def log_metrics(self, metrics: dict) -> None:
        """Log collected metrics"""
        logging.info(
            "CPU: %.1f%%, Memory: %.1fMB, Threads: %d, Files: %d, Connections: %d",
            metrics['cpu_percent'],
            metrics['memory_rss_mb'],
            metrics['threads'],
            metrics['open_files'],
            metrics['connections']
        )
        
    def save_metrics(self) -> None:
        """Save collected metrics to file"""
        output_file = os.path.join('logs', f'metrics_{datetime.now():%Y%m%d_%H%M%S}.json')
        with open(output_file, 'w') as f:
            json.dump(self.metrics, f, indent=2)
            
    def plot_metrics(self) -> None:
        """Generate plots of collected metrics"""
        timestamps = [datetime.fromisoformat(m['timestamp']) for m in self.metrics]
        elapsed = [(t - timestamps[0]).total_seconds() / 60 for t in timestamps]
        
        metrics_to_plot = [
            ('cpu_percent', 'CPU Usage (%)', 'CPU'),
            ('memory_rss_mb', 'Memory Usage (MB)', 'Memory'),
            ('threads', 'Number of Threads', 'Threads')
        ]
        
        fig, axes = plt.subplots(len(metrics_to_plot), 1, figsize=(10, 12))
        fig.suptitle('Process Metrics Over Time')
        
        for (metric, title, ylabel), ax in zip(metrics_to_plot, axes):
            values = [m[metric] for m in self.metrics]
            ax.plot(elapsed, values)
            ax.set_xlabel('Time (minutes)')
            ax.set_ylabel(ylabel)
            ax.set_title(title)
            ax.grid(True)
            
        plt.tight_layout()
        plt.savefig(os.path.join('logs', f'metrics_{datetime.now():%Y%m%d_%H%M%S}.png'))
        
    def monitor(self, interval: int = 5) -> None:
        """Main monitoring loop"""
        try:
            while True:
                metrics = self.collect_metrics()
                self.log_metrics(metrics)
                time.sleep(interval)
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        finally:
            self.save_metrics()
            self.plot_metrics()
            
def main():
    """Main function"""
    # Ensure logs directory exists
    os.makedirs('logs', exist_ok=True)
    
    # Start monitoring
    monitor = ProcessMonitor()
    monitor.monitor()

if __name__ == "__main__":
    main() 