import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime
import json
import threading
import logging
from collections import deque
import boto3
from statistics import mean, median

@dataclass
class ProcessingMetrics:
    """Metrics for processing performance"""
    total_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    start_time: float = field(default_factory=time.time)
    api_latencies: deque = field(default_factory=lambda: deque(maxlen=1000))
    rate_limit_hits: int = 0
    retry_count: int = 0
    batch_sizes: List[int] = field(default_factory=list)
    memory_usage: List[Dict] = field(default_factory=list)
    errors: Dict[str, int] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_api_latency(self, latency: float) -> None:
        """Record API call latency"""
        with self._lock:
            self.api_latencies.append(latency)

    def record_batch_size(self, size: int) -> None:
        """Record batch processing size"""
        with self._lock:
            self.batch_sizes.append(size)

    def record_error(self, error_type: str) -> None:
        """Record error occurrence"""
        with self._lock:
            self.errors[error_type] = self.errors.get(error_type, 0) + 1
            self.failed_records += 1

    def record_success(self) -> None:
        """Record successful processing"""
        with self._lock:
            self.processed_records += 1

    def record_rate_limit(self) -> None:
        """Record rate limit hit"""
        with self._lock:
            self.rate_limit_hits += 1

    def record_retry(self) -> None:
        """Record retry attempt"""
        with self._lock:
            self.retry_count += 1

    def record_memory(self, usage: Dict) -> None:
        """Record memory usage"""
        with self._lock:
            self.memory_usage.append({
                'timestamp': datetime.now().isoformat(),
                **usage
            })

    def get_statistics(self) -> Dict:
        """Get current processing statistics"""
        with self._lock:
            duration = time.time() - self.start_time
            latencies = list(self.api_latencies)
            
            stats = {
                'total_records': self.total_records,
                'processed_records': self.processed_records,
                'failed_records': self.failed_records,
                'success_rate': (self.processed_records / self.total_records * 100 
                               if self.total_records > 0 else 0),
                'processing_duration': duration,
                'records_per_second': self.processed_records / duration if duration > 0 else 0,
                'api_latency': {
                    'mean': mean(latencies) if latencies else 0,
                    'median': median(latencies) if latencies else 0,
                    'min': min(latencies) if latencies else 0,
                    'max': max(latencies) if latencies else 0
                },
                'rate_limit_hits': self.rate_limit_hits,
                'retry_count': self.retry_count,
                'batch_statistics': {
                    'mean_size': mean(self.batch_sizes) if self.batch_sizes else 0,
                    'total_batches': len(self.batch_sizes)
                },
                'error_distribution': self.errors
            }
            return stats

class CloudWatchMetricsPublisher:
    """Publishes metrics to CloudWatch"""
    def __init__(self, namespace: str):
        self.namespace = namespace
        self.cloudwatch = boto3.client('cloudwatch')
        self.logger = logging.getLogger(__name__)

    def publish_metric(self, metric_name: str, value: float, unit: str = 'Count',
                      dimensions: Optional[List[Dict]] = None) -> None:
        """Publish a single metric to CloudWatch"""
        try:
            metric_data = {
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Timestamp': datetime.utcnow()
            }
            if dimensions:
                metric_data['Dimensions'] = dimensions

            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[metric_data]
            )
        except Exception as e:
            self.logger.error(f"Failed to publish metric {metric_name}: {str(e)}")

    def publish_processing_metrics(self, metrics: ProcessingMetrics) -> None:
        """Publish processing metrics to CloudWatch"""
        stats = metrics.get_statistics()
        
        # Publish core metrics
        self.publish_metric('ProcessedRecords', stats['processed_records'])
        self.publish_metric('FailedRecords', stats['failed_records'])
        self.publish_metric('SuccessRate', stats['success_rate'], 'Percent')
        self.publish_metric('ProcessingSpeed', stats['records_per_second'], 'Count/Second')
        
        # API metrics
        self.publish_metric('APILatencyAvg', stats['api_latency']['mean'], 'Milliseconds')
        self.publish_metric('RateLimitHits', stats['rate_limit_hits'])
        self.publish_metric('RetryCount', stats['retry_count'])
        
        # Batch metrics
        self.publish_metric('AverageBatchSize', stats['batch_statistics']['mean_size'])
        
        # Error metrics
        for error_type, count in stats['error_distribution'].items():
            self.publish_metric(
                f'Errors{error_type}',
                count,
                dimensions=[{'Name': 'ErrorType', 'Value': error_type}]
            )

class MetricsLogger:
    """Logs metrics to file and console"""
    def __init__(self, metrics: ProcessingMetrics, log_interval: int = 60):
        self.metrics = metrics
        self.log_interval = log_interval
        self.logger = logging.getLogger(__name__)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._logging_loop, daemon=True)

    def start(self) -> None:
        """Start metrics logging"""
        self._thread.start()

    def stop(self) -> None:
        """Stop metrics logging"""
        self._stop_event.set()
        self._thread.join()
        self._log_metrics()  # Final log

    def _logging_loop(self) -> None:
        """Continuous logging loop"""
        while not self._stop_event.is_set():
            self._log_metrics()
            time.sleep(self.log_interval)

    def _log_metrics(self) -> None:
        """Log current metrics"""
        stats = self.metrics.get_statistics()
        
        self.logger.info(f"""
Processing Statistics:
- Progress: {stats['processed_records']:,}/{stats['total_records']:,} records ({stats['success_rate']:.2f}%)
- Speed: {stats['records_per_second']:.2f} records/second
- API Latency: {stats['api_latency']['mean']:.2f}ms (avg)
- Rate Limit Hits: {stats['rate_limit_hits']:,}
- Retries: {stats['retry_count']:,}
- Errors: {sum(stats['error_distribution'].values()):,}
""")

    def save_metrics(self, filepath: str) -> None:
        """Save metrics to JSON file"""
        stats = self.metrics.get_statistics()
        try:
            with open(filepath, 'w') as f:
                json.dump(stats, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save metrics to {filepath}: {str(e)}") 