import boto3
import time
import logging
import logging.handlers
import random
import asyncio
import aiohttp
import aiodns
import socket
import signal
import cProfile
import pstats
import io
import os
import json
import gzip
import queue
import threading
import contextlib
import psutil
import statistics
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator, Any, Dict, List, Optional, NamedTuple, Set, Union, Tuple, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from collections import deque
from ratelimit import limits, sleep_and_retry, RateLimitException
from dotenv import load_dotenv
from tqdm import tqdm
from smart_open import open
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
from prometheus_client import Counter, Histogram, Gauge
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import requests

from src.exceptions import APIError, RateLimitExceeded
from src.metrics import ProcessingMetrics
from src.monitor import TelemetryManager

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', 'processor.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize OpenTelemetry tracer
tracer = trace.get_tracer(__name__)

# Initialize Prometheus metrics
REQUEST_LATENCY = Histogram('api_request_latency_seconds', 'Request latency in seconds',
                          ['endpoint', 'status'])
REQUEST_COUNT = Counter('api_request_total', 'Total requests', ['endpoint', 'status'])
ACTIVE_CONNECTIONS = Gauge('api_active_connections', 'Number of active connections')
CIRCUIT_BREAKER_STATE = Gauge('circuit_breaker_state', 'Circuit breaker state (0=open, 1=closed)')
CACHE_HIT_RATIO = Gauge('cache_hit_ratio', 'Cache hit ratio')

@dataclass
class Config:
    """Configuration class to hold all settings"""
    s3_bucket: str
    s3_key: str
    api_endpoint: str
    batch_size: int = 1000
    max_workers: int = 20
    rate_limit_calls: int = 100
    rate_limit_period: int = 60
    chunk_size: int = 8192  # 8KB chunks for reading from S3
    max_retries: int = 3
    rate_limit: int = 5000  # requests per second
    log_dir: str = "logs"  # Directory for log files

class MemoryEfficientQueue:
    """A memory-efficient queue with size limit"""
    def __init__(self, maxsize: int = 10000):
        self.queue = queue.Queue(maxsize=maxsize)
        self.processed_count = 0
        
    def put(self, item: Any) -> None:
        self.queue.put(item)
        
    def get(self) -> Any:
        return self.queue.get()
        
    def increment_processed(self) -> None:
        self.processed_count += 1

class S3StreamReader:
    """Memory efficient S3 reader"""
    def __init__(self, bucket: str, key: str, chunk_size: int = 8192):
        self.bucket = bucket
        self.key = key
        self.chunk_size = chunk_size
        self.s3 = boto3.client('s3')
        self._count_records()
        
    def _count_records(self) -> None:
        """Count total records in S3 file"""
        with open(f"s3://{self.bucket}/{self.key}") as f:
            self.total_records = sum(1 for _ in f)
        
    def read_records(self) -> Iterator[str]:
        """Stream records from S3 file"""
        with open(f"s3://{self.bucket}/{self.key}") as f:
            for line in f:
                yield line.strip()

class CircuitBreaker:
    """Circuit breaker implementation for API calls"""
    def __init__(self, failure_threshold: int = 5, reset_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open
        
    def record_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.failure_threshold:
            self.state = "open"
            
    def record_success(self):
        self.failures = 0
        self.state = "closed"
        
    def can_execute(self) -> bool:
        if self.state == "closed":
            return True
        
        if self.state == "open":
            if time.time() - self.last_failure_time > self.reset_timeout:
                self.state = "half-open"
                return True
            return False
            
        # half-open state
        return True

class AdaptiveRateLimiter:
    """Adaptive rate limiting based on API response times and errors"""
    def __init__(self, initial_rate: int = 100, window_size: int = 60):
        self.current_rate = initial_rate
        self.window_size = window_size
        self.response_times = deque(maxlen=1000)
        self.error_counts = deque(maxlen=1000)
        self.last_adjustment = time.time()
        
    def update_metrics(self, response_time: float, is_error: bool):
        self.response_times.append(response_time)
        self.error_counts.append(1 if is_error else 0)
        
        if time.time() - self.last_adjustment > self.window_size:
            self._adjust_rate()
            
    def _adjust_rate(self):
        if not self.response_times:
            return
            
        error_rate = sum(self.error_counts) / len(self.error_counts)
        avg_response_time = statistics.mean(self.response_times)
        
        if error_rate > 0.1:  # More than 10% errors
            self.current_rate = max(10, self.current_rate * 0.8)
        elif error_rate < 0.01 and avg_response_time < 0.5:  # Less than 1% errors and good response time
            self.current_rate = min(self.current_rate * 1.2, 1000)
            
        self.last_adjustment = time.time()
        
    def get_current_rate(self) -> int:
        return int(self.current_rate)

class Priority(Enum):
    LOW = 0
    MEDIUM = 1
    HIGH = 2
    CRITICAL = 3

class Telemetry:
    """Telemetry collection for API performance monitoring"""
    def __init__(self):
        self.request_times = deque(maxlen=10000)
        self.error_rates = deque(maxlen=1000)
        self.throughput = deque(maxlen=100)
        self.last_throughput_time = time.time()
        self.request_count = 0
        self._lock = threading.Lock()

    def record_request(self, duration: float, success: bool, size: int):
        with self._lock:
            self.request_times.append(duration)
            self.error_rates.append(0 if success else 1)
            self.request_count += 1

            current_time = time.time()
            if current_time - self.last_throughput_time >= 60:  # Calculate throughput per minute
                self.throughput.append(self.request_count)
                self.request_count = 0
                self.last_throughput_time = current_time

    def get_metrics(self) -> Dict:
        with self._lock:
            return {
                'latency': {
                    'p50': np.percentile(self.request_times, 50) if self.request_times else 0,
                    'p90': np.percentile(self.request_times, 90) if self.request_times else 0,
                    'p99': np.percentile(self.request_times, 99) if self.request_times else 0,
                },
                'error_rate': sum(self.error_rates) / len(self.error_rates) if self.error_rates else 0,
                'throughput': {
                    'current': self.request_count,
                    'average': statistics.mean(self.throughput) if self.throughput else 0
                }
            }

class PredictiveRateLimiter:
    """Predictive rate limiting using time series analysis"""
    def __init__(self, initial_rate: int = 100, history_size: int = 1000):
        self.current_rate = initial_rate
        self.success_history = deque(maxlen=history_size)
        self.latency_history = deque(maxlen=history_size)
        self.rate_history = deque(maxlen=history_size)
        self.last_adjustment = time.time()
        
    def predict_optimal_rate(self) -> int:
        if len(self.success_history) < 10:
            return self.current_rate
            
        recent_success_rate = sum(self.success_history[-10:]) / 10
        recent_latency = statistics.mean(list(self.latency_history)[-10:])
        
        # Use exponential moving average for prediction
        alpha = 0.3
        predicted_rate = self.current_rate
        
        if recent_success_rate > 0.95 and recent_latency < 0.5:
            predicted_rate = self.current_rate * (1 + alpha)
        elif recent_success_rate < 0.8 or recent_latency > 1.0:
            predicted_rate = self.current_rate * (1 - alpha)
            
        return int(max(10, min(predicted_rate, 1000)))
        
    def update_metrics(self, success: bool, latency: float):
        self.success_history.append(1 if success else 0)
        self.latency_history.append(latency)
        self.rate_history.append(self.current_rate)
        
        if time.time() - self.last_adjustment > 5:  # Adjust every 5 seconds
            self.current_rate = self.predict_optimal_rate()
            self.last_adjustment = time.time()

class APIClient:
    def __init__(self, endpoint: str, rate_limit: int, max_retries: int):
        self.endpoint = endpoint
        self.session = requests.Session()
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        
    @sleep_and_retry
    @limits(calls=1, period=1)
    def process_record(self, record: Dict) -> Dict:
        try:
            response = self.session.post(
                self.endpoint,
                json=record,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise APIError(f"API request failed: {str(e)}")

class AsyncAPIClient:
    """Enhanced async API client with ML-based optimization"""
    
    def __init__(self,
                 endpoint: str,
                 rate_limit_calls: int = 100,
                 rate_limit_period: int = 60,
                 max_concurrent_requests: int = 50,
                 timeout: int = 30,
                 max_retries: int = 3,
                 compress_threshold: int = 1024):
        
        self.endpoint = endpoint
        self.rate_limit_calls = rate_limit_calls
        self.rate_limit_period = rate_limit_period
        self.max_concurrent_requests = max_concurrent_requests
        self.timeout = timeout
        self.compress_threshold = compress_threshold
        
        self._session = None
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.circuit_breaker = CircuitBreaker()
        self.predictive_limiter = PredictiveRateLimiter(initial_rate=rate_limit_calls)
        self.telemetry = Telemetry()
        self.retry_strategy = RetryStrategy(max_retries=max_retries)
        self.connection_pool = AdaptiveConnectionPool()
        self.fault_isolation = FaultIsolation()
        self.health_check = HealthCheck()
        self.performance_predictor = PerformancePredictor()
        self.performance_profiler = PerformanceProfiler()
        self.analytics_engine = AnalyticsEngine()
        self.api_client = APIClient(endpoint, rate_limit_calls, max_retries)
        
        # Initialize health checks
        self.health_check.add_check("api", self._check_api_health)
        
        # Setup signal handlers
        self._setup_signal_handlers()
        
        # Enhanced logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.handlers.RotatingFileHandler(
            'logs/api_client.log', maxBytes=10*1024*1024, backupCount=5)
        self.logger.addHandler(handler)

    async def __aenter__(self):
        """Async context manager entry"""
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self._session:
            await self._session.close()

    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers"""
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._handle_shutdown)
            
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Received shutdown signal, cleaning up...")
        asyncio.create_task(self._cleanup())
        
    async def _cleanup(self):
        """Cleanup resources"""
        if self._session:
            await self._session.close()

    async def _check_api_health(self) -> Dict:
        """Check API endpoint health"""
        try:
            async with self._session.get(f"{self.endpoint}/health") as response:
                return {"status": response.status, "response_time": response.elapsed.total_seconds()}
        except Exception as e:
            raise RuntimeError(f"API health check failed: {str(e)}")

    async def process_record(self, record: Dict, priority: Priority = Priority.MEDIUM) -> Dict:
        """Process a single record with ML-based optimization"""
        with self.performance_profiler.profile("process_record"):
            with tracer.start_as_current_span("process_record") as span:
                span.set_attribute("record_size", len(str(record)))
                span.set_attribute("priority", priority.name)
                
                start_time = time.time()
                try:
                    # Process with enhanced monitoring
                    result = await self.api_client.process_record(record)
                    
                    # Update analytics
                    duration = time.time() - start_time
                    self.analytics_engine.add_metric(datetime.now(), {
                        'latency': duration,
                        'success': True,
                        'record_size': len(str(record)),
                        'priority': priority.value
                    })
                    
                    return result
                    
                except Exception as e:
                    duration = time.time() - start_time
                    self.analytics_engine.add_metric(datetime.now(), {
                        'latency': duration,
                        'success': False,
                        'error_type': type(e).__name__,
                        'record_size': len(str(record)),
                        'priority': priority.value
                    })
                    raise
                    
    async def get_analytics(self) -> Dict:
        """Get comprehensive analytics and insights"""
        return {
            'insights': self.analytics_engine.get_insights(),
            'performance_profile': self.performance_profiler.stats,
            'ml_metrics': {
                'anomaly_score': self.performance_predictor.model.score_samples(
                    self.performance_predictor.scaler.transform(
                        [[m['value'] for m in self.telemetry.get_metrics().values()]]
                    )
                )[0] if self.performance_predictor.is_trained else None
            }
        }

class BatchProcessor:
    """Handles batch processing of records using AsyncAPIClient"""
    
    def __init__(self, config: Config):
        self.config = config
        self.api_client = AsyncAPIClient(
            endpoint=config.api_endpoint,
            rate_limit_calls=config.rate_limit_calls,
            rate_limit_period=config.rate_limit_period,
            max_concurrent_requests=min(config.rate_limit_calls, 50),
            timeout=30,
            max_retries=3
        )
        
    async def process_partition(self, partition: List[Dict]) -> Dict:
        """Process a partition of records using async API client with enhanced metrics"""
        metrics = {
            'success_count': 0,
            'error_count': 0,
            'start_time': time.time(),
            'latencies': [],
            'rate_limits': 0,
            'retries': 0
        }
        
        async with self.api_client as client:
            for i in range(0, len(partition), self.config.batch_size):
                batch = partition[i:i + self.config.batch_size]
                results = await client.process_batch(batch)
                
                for result in results:
                    if result['success']:
                        metrics['success_count'] += 1
                        metrics['latencies'].append(result['duration'] * 1000)  # Convert to ms
                    else:
                        metrics['error_count'] += 1
                        if 'Rate limit exceeded' in result.get('error', ''):
                            metrics['rate_limits'] += 1
                        if 'retry' in result.get('error', '').lower():
                            metrics['retries'] += 1
        
        metrics['duration'] = time.time() - metrics['start_time']
        metrics['records_per_second'] = (
            metrics['success_count'] / metrics['duration']
            if metrics['duration'] > 0 else 0
        )
        metrics['avg_latency'] = (
            sum(metrics['latencies']) / len(metrics['latencies'])
            if metrics['latencies'] else 0
        )
        
        return metrics

class Processor:
    """Main processor class"""
    def __init__(self, config: Config):
        self.config = config
        self.reader = S3StreamReader(config.s3_bucket, config.s3_key)
        self.api_client = AsyncAPIClient(
            config.api_endpoint,
            config.rate_limit_calls,
            config.rate_limit_period
        )
        self.queue = MemoryEfficientQueue()
        self.metrics = ProcessingMetrics()
        self.telemetry = TelemetryManager()
        self.error_count = 0
        self.lock = threading.Lock()

    async def process_batch(self, batch: list[str]) -> None:
        """Process a batch of records"""
        async with self.api_client as client:
            for record in batch:
                try:
                    await client.process_record(record)
                    self.queue.increment_processed()
                except Exception as e:
                    with self.lock:
                        self.error_count += 1
                    logger.error(f"Error processing record: {str(e)}")

    async def consumer(self) -> None:
        """Consumer coroutine function"""
        batch = []
        while True:
            try:
                record = self.queue.get()
                if record is None:
                    if batch:
                        await self.process_batch(batch)
                    break
                
                batch.append(record)
                if len(batch) >= self.config.batch_size:
                    await self.process_batch(batch)
                    batch = []
            except queue.Empty:
                break

    async def run(self) -> None:
        """Main processing function"""
        start_time = datetime.now()
        logger.info("Starting processing...")
        logger.info(f"Total records to process: {self.reader.total_records:,}")

        try:
            # Start consumer tasks
            consumers = [
                asyncio.create_task(self.consumer())
                for _ in range(self.config.max_workers)
            ]
            
            # Producer: Read from S3 and feed the queue
            try:
                with tqdm(total=self.reader.total_records, desc="Processing records") as pbar:
                    last_count = 0
                    for record in self.reader.read_records():
                        self.queue.put(record)
                        current_count = self.queue.processed_count
                        pbar.update(current_count - last_count)
                        last_count = current_count
            except Exception as e:
                logger.error(f"Error reading from S3: {str(e)}")
            finally:
                # Signal consumers to stop
                for _ in range(self.config.max_workers):
                    self.queue.put(None)

            # Wait for all consumers to finish
            await asyncio.gather(*consumers)

        finally:
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info(f"""
Processing completed:
- Total processed: {self.queue.processed_count:,} records
- Error count: {self.error_count:,} records
- Duration: {duration}
- Average speed: {self.queue.processed_count / duration.total_seconds():,.2f} records/second
""")

class SparkProcessor:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.metrics = {}
        self.api_calls = 0
        self.errors = 0
        self.start_time = time.time()
        
    def _call_api(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Call external API with rate limiting and retries.
        
        Args:
            record: Dictionary containing record data
            
        Returns:
            API response as dictionary
        """
        try:
            # Implement rate limiting
            if self.api_calls >= self.config.rate_limit:
                sleep_time = 1.0  # Wait 1 second
                time.sleep(sleep_time)
                self.api_calls = 0
                
            # Make API call
            response = requests.post(
                self.config.api_endpoint,
                json=record,
                timeout=30
            )
            
            self.api_calls += 1
            
            if response.status_code == 429:  # Rate limit exceeded
                sleep_time = float(response.headers.get('Retry-After', 1.0))
                time.sleep(sleep_time)
                return self._call_api(record)  # Retry
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.errors += 1
            error_msg = f"API call failed: {str(e)}"
            self.logger.error(error_msg)
            return {"error": error_msg}
            
    def process_batch(self, batch_df: Union[pd.DataFrame, "pyspark.sql.DataFrame"]) -> Union[pd.DataFrame, "pyspark.sql.DataFrame"]:
        """Process a batch of data.
        
        Args:
            batch_df: Either a pandas DataFrame or PySpark DataFrame
            
        Returns:
            Processed DataFrame of the same type as input
        """
        try:
            # Check if it's a pandas DataFrame
            if isinstance(batch_df, pd.DataFrame):
                # Process using pandas
                batch_df['api_result'] = batch_df.apply(
                    lambda row: self._call_api(row.to_dict()),
                    axis=1
                )
                return batch_df
            else:
                # Process using PySpark
                process_udf = udf(lambda x: self._call_api(x.asDict()), StringType())
                return batch_df.withColumn("api_result", process_udf(col("*")))
                
        except Exception as e:
            self.logger.error(f"Error processing batch: {str(e)}")
            raise
            
    def get_worker_metrics(self) -> Dict[str, Any]:
        """Get metrics from worker nodes."""
        duration = time.time() - self.start_time
        return {
            "api_calls": self.api_calls,
            "errors": self.errors,
            "duration_seconds": duration,
            "calls_per_second": self.api_calls / duration if duration > 0 else 0
        }

def main():
    # Ensure logs directory exists
    os.makedirs('logs', exist_ok=True)
    
    config = Config(
        s3_bucket=os.getenv('S3_BUCKET'),
        s3_key=os.getenv('S3_KEY'),
        api_endpoint=os.getenv('API_ENDPOINT')
    )
    
    processor = Processor(config)
    
    # Run the async processor
    asyncio.run(processor.run())

if __name__ == "__main__":
    main() 