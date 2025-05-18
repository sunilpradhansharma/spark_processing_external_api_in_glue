import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import threading
import logging
from collections import deque, defaultdict
import boto3
from statistics import mean, median, stdev

class ProcessingMetrics:
    """Thread-safe metrics collection without using locks"""
    def __init__(self):
        self.total_records = 0
        self.processed_records = 0
        self.failed_records = 0
        self.start_time = datetime.now().timestamp()
        self.api_latencies = deque(maxlen=1000)
        self.rate_limit_hits = 0
        self.retry_count = 0
        self.batch_sizes = []
        self.memory_usage = []
        self.errors = defaultdict(int)
        
    def record_api_latency(self, latency: float) -> None:
        """Record API call latency"""
        self.api_latencies.append(latency)
        
    def record_rate_limit_hit(self) -> None:
        """Record rate limit hit"""
        self.rate_limit_hits += 1
        
    def record_retry(self) -> None:
        """Record retry attempt"""
        self.retry_count += 1
        
    def record_batch_size(self, size: int) -> None:
        """Record batch size"""
        self.batch_sizes.append(size)
        
    def record_memory_usage(self, usage: float) -> None:
        """Record memory usage"""
        self.memory_usage.append(usage)
        
    def record_error(self, error_type: str) -> None:
        """Record error occurrence"""
        self.errors[error_type] += 1
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        duration = datetime.now().timestamp() - self.start_time
        return {
            "total_records": self.total_records,
            "processed_records": self.processed_records,
            "failed_records": self.failed_records,
            "duration_seconds": duration,
            "records_per_second": self.processed_records / duration if duration > 0 else 0,
            "api_latencies": list(self.api_latencies),
            "rate_limit_hits": self.rate_limit_hits,
            "retry_count": self.retry_count,
            "batch_sizes": self.batch_sizes,
            "memory_usage": self.memory_usage,
            "errors": dict(self.errors)
        }

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
        stats = metrics.get_metrics()
        
        # Publish core metrics
        self.publish_metric('ProcessedRecords', stats['processed_records'])
        self.publish_metric('FailedRecords', stats['failed_records'])
        self.publish_metric('SuccessRate', stats['processed_records'] / stats['total_records'] * 100, 'Percent')
        self.publish_metric('ProcessingSpeed', stats['records_per_second'], 'Count/Second')
        
        # API metrics
        self.publish_metric('APILatencyAvg', mean(stats['api_latencies']) if stats['api_latencies'] else 0, 'Milliseconds')
        self.publish_metric('RateLimitHits', stats['rate_limit_hits'])
        self.publish_metric('RetryCount', stats['retry_count'])
        
        # Batch metrics
        self.publish_metric('AverageBatchSize', mean(stats['batch_sizes']) if stats['batch_sizes'] else 0)
        
        # Error metrics
        for error_type, count in stats['errors'].items():
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
        stats = self.metrics.get_metrics()
        
        self.logger.info(f"""
Processing Statistics:
- Progress: {stats['processed_records']:,}/{stats['total_records']:,} records ({stats['processed_records'] / stats['total_records'] * 100:.2f}%)
- Speed: {stats['records_per_second']:.2f} records/second
- API Latency: {mean(stats['api_latencies']) if stats['api_latencies'] else 0:.2f}ms (avg)
- Rate Limit Hits: {stats['rate_limit_hits']:,}
- Retries: {stats['retry_count']:,}
- Errors: {sum(stats['errors'].values()):,}
""")

    def save_metrics(self, filepath: str) -> None:
        """Save metrics to JSON file"""
        stats = self.metrics.get_metrics()
        try:
            with open(filepath, 'w') as f:
                json.dump(stats, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save metrics to {filepath}: {str(e)}") 