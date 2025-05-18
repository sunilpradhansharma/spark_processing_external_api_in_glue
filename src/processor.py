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

from src.exceptions import APIError, RateLimitExceeded, CircuitBreakerError
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
    rate_limit_calls: int = 5000
    rate_limit_period: int = 1
    retry_attempts: int = 3
    retry_delay: float = 1.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60
    chunk_size: int = 8192  # 8KB chunks for reading from S3
    max_retries: int = 3
    rate_limit: int = 5000  # requests per second
    log_dir: str = "logs"  # Directory for log files

class MemoryEfficientQueue:
    """A memory-efficient queue with size limit"""
    def __init__(self, maxsize: int = 10000):
        self.queue = asyncio.Queue(maxsize=maxsize)
        self.processed_count = 0
        self._lock = threading.Lock()
        
    async def put(self, item: Any) -> None:
        """Put an item into the queue."""
        await self.queue.put(item)
        
    async def get(self) -> Any:
        """Get an item from the queue."""
        return await self.queue.get()
            
    def increment_processed(self) -> None:
        """Increment the processed count in a thread-safe way."""
        with self._lock:
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
    def __init__(self, failure_threshold: int, recovery_timeout: int):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.last_failure_time = None
        self.state = "CLOSED"
        
    def record_failure(self):
        self.failures += 1
        self.last_failure_time = datetime.now()
        
        if self.failures >= self.failure_threshold:
            self.state = "OPEN"
    
    def record_success(self):
        self.failures = 0
        self.state = "CLOSED"
    
    def can_execute(self) -> bool:
        if self.state == "CLOSED":
            return True
            
        if self.state == "OPEN":
            # Check if recovery timeout has passed
            if self.last_failure_time:
                elapsed = (datetime.now() - self.last_failure_time).total_seconds()
                if elapsed >= self.recovery_timeout:
                    self.state = "HALF-OPEN"
                    return True
            return False
            
        return self.state == "HALF-OPEN"

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

class PerformancePredictor:
    """Predictive rate limiting using time series analysis"""
    def __init__(self, initial_rate: int = 100, history_size: int = 1000):
        self.current_rate = initial_rate
        self.success_history = deque(maxlen=history_size)
        self.latency_history = deque(maxlen=history_size)
        self.rate_history = deque(maxlen=history_size)
        self.last_adjustment = time.time()
        
    def predict_optimal_rate(self) -> int:
        """Predict optimal rate based on historical data."""
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
        """Update metrics with new data point."""
        self.success_history.append(1 if success else 0)
        self.latency_history.append(latency)
        self.rate_history.append(self.current_rate)
        
        if time.time() - self.last_adjustment > 5:  # Adjust every 5 seconds
            self.current_rate = self.predict_optimal_rate()
            self.last_adjustment = time.time()
            
    def get_current_rate(self) -> int:
        """Get the current rate limit."""
        return self.current_rate

class PerformanceProfiler:
    """Profiles code performance using cProfile"""
    def __init__(self):
        self.profiler = cProfile.Profile()
        self.stats = {}
        self._lock = threading.Lock()
        
    @contextlib.contextmanager
    def profile(self, name: str):
        """Context manager for profiling a code block."""
        try:
            self.profiler.enable()
            yield
        finally:
            self.profiler.disable()
            s = io.StringIO()
            ps = pstats.Stats(self.profiler, stream=s).sort_stats('cumulative')
            ps.print_stats()
            
            with self._lock:
                self.stats[name] = s.getvalue()
                
    def get_stats(self, name: str = None) -> Union[Dict[str, str], str]:
        """Get profiling statistics."""
        with self._lock:
            if name:
                return self.stats.get(name, "No profile data available")
            return self.stats.copy()
            
    def clear(self):
        """Clear profiling data."""
        with self._lock:
            self.stats.clear()
            self.profiler = cProfile.Profile()

class RetryStrategy:
    """Implements exponential backoff retry strategy for API calls"""
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        
    def get_next_retry_delay(self, attempt: int) -> float:
        """Calculate delay for next retry using exponential backoff."""
        if attempt >= self.max_retries:
            return -1  # Signal that we should not retry
            
        delay = min(self.max_delay, self.base_delay * (2 ** attempt))
        jitter = random.uniform(0, 0.1 * delay)  # Add 0-10% jitter
        return delay + jitter
        
    async def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function with retry logic."""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                delay = self.get_next_retry_delay(attempt)
                
                if delay < 0:  # Max retries exceeded
                    break
                    
                await asyncio.sleep(delay)
                
        raise last_exception

class AdaptiveConnectionPool:
    """Manages a pool of connections with adaptive sizing"""
    def __init__(self, initial_size: int = 10, min_size: int = 5, max_size: int = 50):
        self.min_size = min_size
        self.max_size = max_size
        self.current_size = initial_size
        self.active_connections = 0
        self.response_times = deque(maxlen=1000)
        self._lock = threading.Lock()
        
    def acquire(self) -> bool:
        """Attempt to acquire a connection from the pool."""
        with self._lock:
            if self.active_connections < self.current_size:
                self.active_connections += 1
                return True
            return False
            
    def release(self, response_time: float = None):
        """Release a connection back to the pool."""
        with self._lock:
            self.active_connections -= 1
            if response_time is not None:
                self.response_times.append(response_time)
                self._adjust_pool_size()
                
    def _adjust_pool_size(self):
        """Adjust pool size based on response times."""
        if len(self.response_times) < 10:
            return
            
        avg_response_time = statistics.mean(self.response_times)
        utilization = self.active_connections / self.current_size
        
        if avg_response_time > 1.0 and utilization > 0.8:
            # High latency and high utilization - grow pool
            self.current_size = min(self.max_size, int(self.current_size * 1.2))
        elif avg_response_time < 0.5 and utilization < 0.5:
            # Low latency and low utilization - shrink pool
            self.current_size = max(self.min_size, int(self.current_size * 0.8))

class FaultIsolation:
    """Implements fault isolation patterns for API calls"""
    def __init__(self, error_threshold: int = 5, isolation_period: int = 300):
        self.error_threshold = error_threshold
        self.isolation_period = isolation_period
        self.error_counts = {}  # endpoint -> error count
        self.isolation_times = {}  # endpoint -> isolation start time
        self._lock = threading.Lock()
        
    def record_error(self, endpoint: str):
        """Record an error for an endpoint."""
        with self._lock:
            if endpoint not in self.error_counts:
                self.error_counts[endpoint] = 0
            self.error_counts[endpoint] += 1
            
            if self.error_counts[endpoint] >= self.error_threshold:
                self.isolation_times[endpoint] = time.time()
                
    def record_success(self, endpoint: str):
        """Record a successful call for an endpoint."""
        with self._lock:
            self.error_counts[endpoint] = 0
            
    def is_isolated(self, endpoint: str) -> bool:
        """Check if an endpoint is currently isolated."""
        with self._lock:
            if endpoint not in self.isolation_times:
                return False
                
            isolation_time = self.isolation_times[endpoint]
            if time.time() - isolation_time > self.isolation_period:
                # Isolation period has expired
                del self.isolation_times[endpoint]
                self.error_counts[endpoint] = 0
                return False
                
            return True

class HealthCheck:
    """Manages health checks for API endpoints"""
    def __init__(self, check_interval: int = 60):
        self.check_interval = check_interval
        self.checks = {}  # name -> check function
        self.results = {}  # name -> (timestamp, result)
        self._lock = threading.Lock()
        
    def add_check(self, name: str, check_func: Callable[[], Dict]):
        """Add a health check function."""
        with self._lock:
            self.checks[name] = check_func
            
    async def run_checks(self) -> Dict[str, Any]:
        """Run all registered health checks."""
        results = {}
        current_time = time.time()
        
        for name, check_func in self.checks.items():
            # Skip if check was run recently
            if name in self.results:
                last_time, last_result = self.results[name]
                if current_time - last_time < self.check_interval:
                    results[name] = last_result
                    continue
            
            try:
                result = await check_func()
                self.results[name] = (current_time, result)
                results[name] = result
            except Exception as e:
                results[name] = {
                    "status": "error",
                    "error": str(e)
                }
                
        return results
        
    def is_healthy(self) -> bool:
        """Check if all health checks are passing."""
        current_time = time.time()
        with self._lock:
            for name, (timestamp, result) in self.results.items():
                # Check is stale
                if current_time - timestamp > self.check_interval:
                    return False
                # Check failed
                if result.get("status") != "healthy":
                    return False
        return True

class PredictiveRateLimiter:
    """Predictive rate limiting using time series analysis"""
    def __init__(self, initial_rate: int = 100, history_size: int = 1000):
        self.current_rate = initial_rate
        self.success_history = deque(maxlen=history_size)
        self.latency_history = deque(maxlen=history_size)
        self.rate_history = deque(maxlen=history_size)
        self.last_adjustment = time.time()
        
    def predict_optimal_rate(self) -> int:
        """Predict optimal rate based on historical data."""
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
        """Update metrics with new data point."""
        self.success_history.append(1 if success else 0)
        self.latency_history.append(latency)
        self.rate_history.append(self.current_rate)
        
        if time.time() - self.last_adjustment > 5:  # Adjust every 5 seconds
            self.current_rate = self.predict_optimal_rate()
            self.last_adjustment = time.time()
            
    def get_current_rate(self) -> int:
        """Get the current rate limit."""
        return self.current_rate

class AnalyticsEngine:
    """Analytics engine for processing metrics and generating insights"""
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.metrics = deque(maxlen=window_size)
        self._lock = threading.Lock()
        
    def add_metric(self, timestamp: datetime, metric: Dict[str, Any]):
        """Add a new metric data point."""
        with self._lock:
            self.metrics.append({
                'timestamp': timestamp,
                **metric
            })
            
    def get_insights(self) -> Dict[str, Any]:
        """Generate insights from collected metrics."""
        with self._lock:
            if not self.metrics:
                return {
                    'latency_stats': {},
                    'error_rate': 0,
                    'throughput': 0,
                    'anomalies': []
                }
                
            # Calculate basic statistics
            latencies = [m['latency'] for m in self.metrics]
            successes = sum(1 for m in self.metrics if m.get('success', False))
            
            # Detect anomalies (points > 2 std dev from mean)
            mean_latency = statistics.mean(latencies)
            std_latency = statistics.stdev(latencies) if len(latencies) > 1 else 0
            anomalies = [
                {
                    'timestamp': m['timestamp'],
                    'latency': m['latency'],
                    'z_score': (m['latency'] - mean_latency) / std_latency if std_latency > 0 else 0
                }
                for m in self.metrics
                if abs(m['latency'] - mean_latency) > 2 * std_latency
            ]
            
            # Group by priority if available
            priority_stats = {}
            for m in self.metrics:
                if 'priority' in m:
                    if m['priority'] not in priority_stats:
                        priority_stats[m['priority']] = {'count': 0, 'latencies': []}
                    priority_stats[m['priority']]['count'] += 1
                    priority_stats[m['priority']]['latencies'].append(m['latency'])
            
            for stats in priority_stats.values():
                stats['avg_latency'] = statistics.mean(stats['latencies'])
            
            return {
                'latency_stats': {
                    'mean': mean_latency,
                    'p50': np.percentile(latencies, 50),
                    'p90': np.percentile(latencies, 90),
                    'p99': np.percentile(latencies, 99)
                },
                'error_rate': 1 - (successes / len(self.metrics)),
                'throughput': len(self.metrics) / (
                    (self.metrics[-1]['timestamp'] - self.metrics[0]['timestamp']).total_seconds()
                    if len(self.metrics) > 1 else 1
                ),
                'anomalies': anomalies,
                'priority_stats': priority_stats
            }
            
    def clear(self):
        """Clear all collected metrics."""
        with self._lock:
            self.metrics.clear()

class APIClient:
    def __init__(self, endpoint: str, rate_limit: int, rate_period: int):
        self.endpoint = endpoint
        self.rate_limit = rate_limit
        self.rate_period = rate_period
        self.circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
        self.semaphore = asyncio.Semaphore(10)  # Limit concurrent connections
        self.session = aiohttp.ClientSession()
        self.telemetry = Telemetry()
        self.adaptive_rate_limiter = AdaptiveRateLimiter()
        self.performance_predictor = PerformancePredictor()
        self.retry_strategy = RetryStrategy()
        self.connection_pool = AdaptiveConnectionPool()
        self.fault_isolation = FaultIsolation()
        self.health_check = HealthCheck()
        self.analytics_engine = AnalyticsEngine()
        self.metrics = ProcessingMetrics()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def process_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        if not self.circuit_breaker.can_execute():
            raise CircuitBreakerError("Circuit breaker is OPEN")
            
        start_time = datetime.now()
        
        try:
            async with self.semaphore:
                async with self.session.post(self.endpoint, json=record) as response:
                    if response.status == 429:  # Rate limit exceeded
                        self.metrics.record_rate_limit_hit()
                        retry_after = int(response.headers.get('Retry-After', 1))
                        raise RateLimitExceeded(f"Rate limit exceeded. Retry after {retry_after} seconds")
                        
                    response.raise_for_status()
                    result = await response.json()
                    
                    # Record success metrics
                    self.circuit_breaker.record_success()
                    self.metrics.record_api_latency((datetime.now() - start_time).total_seconds() * 1000)
                    
                    return result
                    
        except aiohttp.ClientError as e:
            self.circuit_breaker.record_failure()
            self.metrics.record_error('api_error')
            raise APIError(f"API request failed: {str(e)}")

class BatchProcessor:
    def __init__(self, config: Config):
        self.config = config
        self.metrics = ProcessingMetrics()
        self.logger = logging.getLogger(__name__)
        
    async def process_partition(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        start_time = datetime.now()
        success_count = 0
        error_count = 0
        
        # Process records in batches
        async with APIClient(self.config.api_endpoint, 
                           self.config.rate_limit_calls,
                           self.config.rate_limit_period) as api_client:
                               
            for i in range(0, len(records), self.config.batch_size):
                batch = records[i:i + self.config.batch_size]
                
                # Process each record in the batch
                for record in batch:
                    try:
                        await self._process_with_retry(api_client, record)
                        success_count += 1
                    except Exception as e:
                        self.logger.error(f"Failed to process record: {str(e)}")
                        error_count += 1
                        self.metrics.record_error(type(e).__name__)
                
                # Calculate progress
                progress = (i + len(batch)) / len(records) * 100
                self.logger.info(f"Progress: {progress:.1f}% ({i + len(batch)}/{len(records)})")
                
        # Calculate processing speed
        duration = (datetime.now() - start_time).total_seconds()
        records_per_second = len(records) / duration if duration > 0 else 0
        
        return {
            'success_count': success_count,
            'error_count': error_count,
            'records_per_second': records_per_second,
            'duration_seconds': duration
        }
    
    async def _process_with_retry(self, api_client: APIClient, record: Dict[str, Any]) -> None:
        """Process a single record with retry logic"""
        for attempt in range(self.config.retry_attempts):
            try:
                await api_client.process_record(record)
                return
            except RateLimitExceeded:
                # Wait before retrying
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                self.metrics.record_retry()
            except CircuitBreakerError:
                # Wait for circuit breaker timeout
                await asyncio.sleep(self.config.circuit_breaker_timeout)
                self.metrics.record_retry()
            except Exception as e:
                if attempt == self.config.retry_attempts - 1:
                    raise
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                self.metrics.record_retry()

class Processor:
    """Main processor class"""
    def __init__(self, config: Config):
        self.config = config
        self.reader = S3StreamReader(config.s3_bucket, config.s3_key)
        self.api_client = APIClient(
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
                    # Parse the JSON string into a dictionary
                    record_dict = json.loads(record)
                    await client.process_record(record_dict)
                    self.queue.increment_processed()
                except json.JSONDecodeError as e:
                    with self.lock:
                        self.error_count += 1
                    logger.error(f"Error parsing record JSON: {str(e)}")
                except Exception as e:
                    with self.lock:
                        self.error_count += 1
                    logger.error(f"Error processing record: {str(e)}")

    async def consumer(self) -> None:
        """Consumer coroutine function"""
        batch = []
        while True:
            try:
                record = await self.queue.get()  # Use await here
                if record is None:
                    if batch:
                        await self.process_batch(batch)
                    break
                
                batch.append(record)
                if len(batch) >= self.config.batch_size:
                    await self.process_batch(batch)
                    batch = []
            except Exception as e:  # Handle any queue errors
                logger.error(f"Error in consumer: {str(e)}")
                if batch:  # Process any remaining records
                    await self.process_batch(batch)
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
                        await self.queue.put(record)  # Use await here
                        current_count = self.queue.processed_count
                        pbar.update(current_count - last_count)
                        last_count = current_count
            except Exception as e:
                logger.error(f"Error reading from S3: {str(e)}")
            finally:
                # Signal consumers to stop
                for _ in range(self.config.max_workers):
                    await self.queue.put(None)  # Use await here

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