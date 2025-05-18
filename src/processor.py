import boto3
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator, Any, Dict, List, Optional, NamedTuple, Set, Union, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import queue
import threading
import requests
from ratelimit import limits, sleep_and_retry, RateLimitException
import smart_open
import os
from dotenv import load_dotenv
from tqdm import tqdm
import aiohttp
import asyncio
from src.exceptions import APIError, RateLimitExceeded
import gzip
import json
import statistics
from collections import deque
import numpy as np
from cachetools import TTLCache, LRUCache
import aioredis
from enum import Enum
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
from prometheus_client import Counter, Histogram, Gauge
import aiodns.error
import socket
from typing import Callable
import signal
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import jwt
from cryptography.fernet import Fernet
import hashlib
import secrets
import pandas as pd
import functools
import cProfile
import pstats
import io
import contextlib
import psutil

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

class MemoryEfficientQueue:
    """A memory-efficient queue with size limit"""
    def __init__(self, maxsize: int):
        self.queue = queue.Queue(maxsize=maxsize)
        self.active = True
        self._processed_count = 0
        self._lock = threading.Lock()
        
    @property
    def processed_count(self) -> int:
        with self._lock:
            return self._processed_count
            
    def increment_processed(self) -> None:
        with self._lock:
            self._processed_count += 1
        
    def put(self, item: Any) -> None:
        while self.active:
            try:
                self.queue.put(item, timeout=1)
                break
            except queue.Full:
                time.sleep(0.1)
                
    def get(self) -> Any:
        while self.active:
            try:
                return self.queue.get(timeout=1)
            except queue.Empty:
                if not self.active:
                    raise queue.Empty
                time.sleep(0.1)
                
    def stop(self) -> None:
        self.active = False

class S3StreamReader:
    """Memory efficient S3 reader"""
    def __init__(self, bucket: str, key: str, chunk_size: int):
        self.s3_uri = f's3://{bucket}/{key}'
        self.chunk_size = chunk_size
        self.total_records = self._count_records()
        
    def _count_records(self) -> int:
        """Count total records in S3 file"""
        count = 0
        with smart_open.open(self.s3_uri, 'r', transport_params={'buffer_size': self.chunk_size}) as f:
            for _ in f:
                count += 1
        return count
        
    def read_records(self) -> Iterator[str]:
        """Stream records from S3 file"""
        with smart_open.open(self.s3_uri, 'r', transport_params={'buffer_size': self.chunk_size}) as f:
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

class RequestCache:
    """Multi-level cache for API requests"""
    def __init__(self, ttl: int = 300):
        self.memory_cache = TTLCache(maxsize=1000, ttl=ttl)
        self.redis_client = None
        self._lock = threading.Lock()
        
    async def init_redis(self, redis_url: str):
        self.redis_client = await aioredis.create_redis_pool(redis_url)
        
    async def get(self, key: str) -> Optional[Dict]:
        # Check memory cache first
        with self._lock:
            if key in self.memory_cache:
                return self.memory_cache[key]
        
        # Check Redis if available
        if self.redis_client:
            cached_data = await self.redis_client.get(key)
            if cached_data:
                data = json.loads(cached_data)
                with self._lock:
                    self.memory_cache[key] = data
                return data
                
        return None
        
    async def set(self, key: str, value: Dict, ttl: int = 300):
        with self._lock:
            self.memory_cache[key] = value
            
        if self.redis_client:
            await self.redis_client.setex(
                key,
                ttl,
                json.dumps(value)
            )

class RetryStrategy:
    """Intelligent retry strategy with backoff and jitter"""
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.error_patterns = LRUCache(maxsize=1000)
        
    def should_retry(self, error: Exception, attempt: int) -> bool:
        if attempt >= self.max_retries:
            return False
            
        error_key = f"{type(error).__name__}:{str(error)}"
        error_count = self.error_patterns.get(error_key, 0) + 1
        self.error_patterns[error_key] = error_count
        
        # Don't retry if we see the same error too frequently
        if error_count > 10:
            return False
            
        if isinstance(error, aiohttp.ClientError):
            if isinstance(error, aiohttp.ClientResponseError):
                # Don't retry on 4xx errors except specific cases
                if 400 <= error.status < 500 and error.status not in [429, 408]:
                    return False
            return True
            
        return False
        
    def get_delay(self, attempt: int) -> float:
        # Exponential backoff with jitter
        delay = self.base_delay * (2 ** attempt)
        jitter = random.uniform(0, 0.1 * delay)
        return delay + jitter

class HealthCheck:
    """Health check implementation"""
    def __init__(self):
        self.last_check = time.time()
        self.status = "healthy"
        self.checks: Dict[str, Callable] = {}
        self._lock = threading.Lock()
        
    def add_check(self, name: str, check_func: Callable) -> None:
        """Add a health check function"""
        with self._lock:
            self.checks[name] = check_func
            
    async def run_checks(self) -> Dict:
        """Run all health checks"""
        results = {}
        with self._lock:
            for name, check_func in self.checks.items():
                try:
                    result = await check_func()
                    results[name] = {"status": "healthy", "details": result}
                except Exception as e:
                    results[name] = {"status": "unhealthy", "error": str(e)}
        return results

class AdaptiveConnectionPool:
    """Adaptive connection pool with automatic scaling"""
    def __init__(self, min_size: int = 10, max_size: int = 100,
                 target_utilization: float = 0.7):
        self.min_size = min_size
        self.max_size = max_size
        self.target_utilization = target_utilization
        self.current_size = min_size
        self.active_connections = 0
        self._lock = threading.Lock()
        self.metrics = deque(maxlen=100)
        
    def acquire(self) -> None:
        """Acquire a connection from the pool"""
        with self._lock:
            self.active_connections += 1
            self._adjust_pool_size()
            ACTIVE_CONNECTIONS.set(self.active_connections)
            
    def release(self) -> None:
        """Release a connection back to the pool"""
        with self._lock:
            self.active_connections -= 1
            self._adjust_pool_size()
            ACTIVE_CONNECTIONS.set(self.active_connections)
            
    def _adjust_pool_size(self) -> None:
        """Adjust pool size based on utilization"""
        utilization = self.active_connections / self.current_size
        self.metrics.append(utilization)
        
        avg_utilization = statistics.mean(self.metrics)
        if avg_utilization > self.target_utilization and self.current_size < self.max_size:
            self.current_size = min(self.max_size, int(self.current_size * 1.5))
        elif avg_utilization < self.target_utilization / 2 and self.current_size > self.min_size:
            self.current_size = max(self.min_size, int(self.current_size * 0.8))

class FaultIsolation:
    """Fault isolation implementation"""
    def __init__(self):
        self.error_counts: Dict[str, int] = {}
        self.isolated_endpoints: Set[str] = set()
        self._lock = threading.Lock()
        
    def record_error(self, endpoint: str) -> None:
        """Record an error for an endpoint"""
        with self._lock:
            self.error_counts[endpoint] = self.error_counts.get(endpoint, 0) + 1
            if self.error_counts[endpoint] >= 10:
                self.isolated_endpoints.add(endpoint)
                
    def record_success(self, endpoint: str) -> None:
        """Record a success for an endpoint"""
        with self._lock:
            self.error_counts[endpoint] = max(0, self.error_counts.get(endpoint, 0) - 1)
            if endpoint in self.isolated_endpoints and self.error_counts[endpoint] < 5:
                self.isolated_endpoints.remove(endpoint)
                
    def is_isolated(self, endpoint: str) -> bool:
        """Check if an endpoint is isolated"""
        return endpoint in self.isolated_endpoints

class SecurityManager:
    """Manages security features including encryption and authentication"""
    def __init__(self, encryption_key: Optional[str] = None):
        self.encryption_key = encryption_key or Fernet.generate_key()
        self.cipher_suite = Fernet(self.encryption_key)
        self.jwt_secret = secrets.token_hex(32)
        self._rate_limit_store = {}
        self._ip_blacklist = set()
        
    def encrypt_payload(self, data: Dict) -> bytes:
        """Encrypt request payload"""
        return self.cipher_suite.encrypt(json.dumps(data).encode())
        
    def decrypt_payload(self, encrypted_data: bytes) -> Dict:
        """Decrypt response payload"""
        return json.loads(self.cipher_suite.decrypt(encrypted_data))
        
    def generate_token(self, payload: Dict) -> str:
        """Generate JWT token"""
        return jwt.encode(payload, self.jwt_secret, algorithm='HS256')
        
    def verify_token(self, token: str) -> Dict:
        """Verify JWT token"""
        return jwt.decode(token, self.jwt_secret, algorithms=['HS256'])
        
    def is_rate_limited(self, ip: str, limit: int = 100, window: int = 60) -> bool:
        """Check if IP is rate limited"""
        now = time.time()
        if ip not in self._rate_limit_store:
            self._rate_limit_store[ip] = deque()
        
        # Clean old requests
        while (self._rate_limit_store[ip] and 
               self._rate_limit_store[ip][0] < now - window):
            self._rate_limit_store[ip].popleft()
            
        if len(self._rate_limit_store[ip]) >= limit:
            return True
            
        self._rate_limit_store[ip].append(now)
        return False

class PerformancePredictor:
    """ML-based performance prediction and anomaly detection"""
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.scaler = StandardScaler()
        self.model = IsolationForest(contamination=0.1)
        self.features = deque(maxlen=window_size)
        self.is_trained = False
        
    def add_observation(self, latency: float, error_rate: float, 
                       cpu_usage: float, memory_usage: float):
        """Add new observation"""
        self.features.append([latency, error_rate, cpu_usage, memory_usage])
        
        if len(self.features) == self.window_size and not self.is_trained:
            self._train_model()
            
    def _train_model(self):
        """Train the anomaly detection model"""
        X = np.array(self.features)
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled)
        self.is_trained = True
        
    def predict_performance(self, latency: float, error_rate: float,
                          cpu_usage: float, memory_usage: float) -> Tuple[bool, float]:
        """Predict if current performance is anomalous"""
        if not self.is_trained:
            return False, 0.0
            
        X = np.array([[latency, error_rate, cpu_usage, memory_usage]])
        X_scaled = self.scaler.transform(X)
        score = self.model.score_samples(X_scaled)[0]
        is_anomaly = score < self.model.threshold_
        return is_anomaly, score

class PerformanceProfiler:
    """Performance profiling and analysis"""
    def __init__(self):
        self.profiler = cProfile.Profile()
        self.stats = None
        
    def start(self):
        """Start profiling"""
        self.profiler.enable()
        
    def stop(self):
        """Stop profiling and collect stats"""
        self.profiler.disable()
        s = io.StringIO()
        ps = pstats.Stats(self.profiler, stream=s).sort_stats('cumulative')
        ps.print_stats()
        self.stats = s.getvalue()
        return self.stats
        
    @contextlib.contextmanager
    def profile(self, name: str):
        """Context manager for profiling specific operations"""
        try:
            self.start()
            yield
        finally:
            stats = self.stop()
            logger.info(f"Profile for {name}:\n{stats}")

class AnalyticsEngine:
    """Real-time analytics and insights"""
    def __init__(self, window_size: int = 3600):
        self.window_size = window_size
        self.data = pd.DataFrame()
        self.anomaly_threshold = 2.0
        
    def add_metric(self, timestamp: datetime, metrics: Dict):
        """Add metrics to analytics"""
        df = pd.DataFrame([metrics], index=[timestamp])
        self.data = pd.concat([self.data, df]).last(self.window_size)
        
    def get_insights(self) -> Dict:
        """Generate insights from collected metrics"""
        if len(self.data) < 2:
            return {}
            
        return {
            'trends': self._calculate_trends(),
            'anomalies': self._detect_anomalies(),
            'correlations': self._find_correlations(),
            'patterns': self._identify_patterns()
        }
        
    def _calculate_trends(self) -> Dict:
        """Calculate metric trends"""
        trends = {}
        for column in self.data.columns:
            if self.data[column].dtype in (np.float64, np.int64):
                trends[column] = {
                    'slope': np.polyfit(range(len(self.data)), 
                                      self.data[column], 1)[0],
                    'mean': self.data[column].mean(),
                    'std': self.data[column].std()
                }
        return trends
        
    def _detect_anomalies(self) -> Dict:
        """Detect anomalies in metrics"""
        anomalies = {}
        for column in self.data.columns:
            if self.data[column].dtype in (np.float64, np.int64):
                mean = self.data[column].mean()
                std = self.data[column].std()
                anomalies[column] = self.data[
                    abs(self.data[column] - mean) > self.anomaly_threshold * std
                ].index.tolist()
        return anomalies
        
    def _find_correlations(self) -> Dict:
        """Find correlations between metrics"""
        numeric_cols = self.data.select_dtypes(
            include=[np.float64, np.int64]).columns
        return self.data[numeric_cols].corr().to_dict()
        
    def _identify_patterns(self) -> Dict:
        """Identify patterns in metrics"""
        patterns = {}
        for column in self.data.columns:
            if self.data[column].dtype in (np.float64, np.int64):
                patterns[column] = {
                    'seasonality': self._check_seasonality(self.data[column]),
                    'spikes': self._detect_spikes(self.data[column])
                }
        return patterns
        
    def _check_seasonality(self, series: pd.Series) -> Dict:
        """Check for seasonality in time series"""
        if len(series) < 24:
            return {}
            
        hourly_means = series.groupby(series.index.hour).mean()
        return {
            'hourly_pattern': hourly_means.to_dict(),
            'variance': hourly_means.var()
        }
        
    def _detect_spikes(self, series: pd.Series) -> List[datetime]:
        """Detect sudden spikes in metrics"""
        if len(series) < 2:
            return []
            
        diff = series.diff()
        mean_diff = diff.mean()
        std_diff = diff.std()
        
        return series[
            abs(diff - mean_diff) > 2 * std_diff
        ].index.tolist()

class AsyncAPIClient:
    """Enhanced async API client with ML-based optimization and security"""
    
    def __init__(self,
                 endpoint: str,
                 rate_limit_calls: int = 100,
                 rate_limit_period: int = 60,
                 max_concurrent_requests: int = 50,
                 timeout: int = 30,
                 max_retries: int = 3,
                 compress_threshold: int = 1024,
                 cache_ttl: int = 300,
                 redis_url: Optional[str] = None,
                 encryption_key: Optional[str] = None):
        
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
        self.cache = RequestCache(ttl=cache_ttl)
        self.retry_strategy = RetryStrategy(max_retries=max_retries)
        self.redis_url = redis_url
        self.connection_pool = AdaptiveConnectionPool()
        self.fault_isolation = FaultIsolation()
        self.health_check = HealthCheck()
        self.security_manager = SecurityManager(encryption_key)
        self.performance_predictor = PerformancePredictor()
        self.performance_profiler = PerformanceProfiler()
        self.analytics_engine = AnalyticsEngine()
        
        # Initialize health checks
        self.health_check.add_check("api", self._check_api_health)
        self.health_check.add_check("redis", self._check_redis_health)
        
        # Setup signal handlers
        self._setup_signal_handlers()
        
        # Enhanced logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.handlers.RotatingFileHandler(
            'api_client.log', maxBytes=10*1024*1024, backupCount=5)
        self.logger.addHandler(handler)
        
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
        if self.cache.redis_client:
            self.cache.redis_client.close()
            await self.cache.redis_client.wait_closed()
            
    async def _check_api_health(self) -> Dict:
        """Check API endpoint health"""
        try:
            async with self._session.get(f"{self.endpoint}/health") as response:
                return {"status": response.status, "response_time": response.elapsed.total_seconds()}
        except Exception as e:
            raise RuntimeError(f"API health check failed: {str(e)}")
            
    async def _check_redis_health(self) -> Dict:
        """Check Redis health"""
        if not self.cache.redis_client:
            return {"status": "disabled"}
        try:
            await self.cache.redis_client.ping()
            return {"status": "healthy"}
        except Exception as e:
            raise RuntimeError(f"Redis health check failed: {str(e)}")
            
    async def process_record(self, record: Dict, priority: Priority = Priority.MEDIUM) -> Dict:
        """Process a single record with ML-based optimization"""
        with self.performance_profiler.profile("process_record"):
            with tracer.start_as_current_span("process_record") as span:
                span.set_attribute("record_size", len(str(record)))
                span.set_attribute("priority", priority.name)
                
                start_time = time.time()
                try:
                    # Encrypt sensitive data
                    encrypted_record = self.security_manager.encrypt_payload(record)
                    
                    # Check for performance anomalies
                    metrics = self.telemetry.get_metrics()
                    is_anomaly, score = self.performance_predictor.predict_performance(
                        metrics['latency']['p90'],
                        metrics['error_rate'],
                        psutil.cpu_percent(),
                        psutil.virtual_memory().percent
                    )
                    
                    if is_anomaly:
                        self.logger.warning(
                            f"Performance anomaly detected! Score: {score}")
                        
                    # Process with enhanced monitoring
                    result = await super().process_record(
                        self.security_manager.decrypt_payload(encrypted_record),
                        priority
                    )
                    
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
            'security_metrics': {
                'rate_limited_ips': len(self.security_manager._rate_limit_store),
                'blacklisted_ips': len(self.security_manager._ip_blacklist)
            },
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
        self.reader = S3StreamReader(config.s3_bucket, config.s3_key, config.chunk_size)
        self.api_client = AsyncAPIClient(
            config.api_endpoint,
            config.rate_limit_calls,
            config.rate_limit_period
        )
        self.queue = MemoryEfficientQueue(maxsize=config.batch_size * 2)
        self.error_count = 0
        self.lock = threading.Lock()
        
    def process_batch(self, batch: list[str]) -> None:
        """Process a batch of records"""
        for record in batch:
            try:
                self.api_client.process_record(record)
                self.queue.increment_processed()
            except Exception as e:
                with self.lock:
                    self.error_count += 1
                logger.error(f"Error processing record: {str(e)}")

    def consumer(self) -> None:
        """Consumer thread function"""
        batch = []
        while True:
            try:
                record = self.queue.get()
                if record is None:
                    if batch:
                        self.process_batch(batch)
                    break
                
                batch.append(record)
                if len(batch) >= self.config.batch_size:
                    self.process_batch(batch)
                    batch = []
            except queue.Empty:
                break

    def run(self) -> None:
        """Main processing function"""
        start_time = datetime.now()
        logger.info("Starting processing...")
        logger.info(f"Total records to process: {self.reader.total_records:,}")

        # Start consumer threads
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            consumers = [
                executor.submit(self.consumer)
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
                self.queue.stop()

            # Wait for all consumers to finish
            for future in consumers:
                future.result()

        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"""
Processing completed:
- Total processed: {self.queue.processed_count:,} records
- Error count: {self.error_count:,} records
- Duration: {duration}
- Average speed: {self.queue.processed_count / duration.total_seconds():,.2f} records/second
""")

def main():
    # Ensure logs directory exists
    os.makedirs('logs', exist_ok=True)
    
    config = Config(
        s3_bucket=os.getenv('S3_BUCKET'),
        s3_key=os.getenv('S3_KEY'),
        api_endpoint=os.getenv('API_ENDPOINT')
    )
    
    processor = Processor(config)
    processor.run()

if __name__ == "__main__":
    main() 