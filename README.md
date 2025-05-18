# Spark Processing with External API Integration in AWS Glue

A high-performance Python application designed to process large-scale data efficiently using both pandas and PySpark DataFrames. The system is optimized to handle 1 million records from S3 while maintaining strict API rate limits and optimizing resource usage. 

## System Overview

The application implements a robust data processing pipeline that:
- Streams data from S3 in memory-efficient chunks
- Processes records using either pandas or PySpark based on data size and requirements
- Makes rate-limited API calls (5000 TPS) with circuit breaker protection
- Implements async processing for improved throughput
- Provides comprehensive monitoring and metrics collection
- Handles errors gracefully with automatic retries and failure tracking
- Scales efficiently with configurable batch sizes and worker counts

Key processing capabilities:
- Handles various data formats (CSV, JSON, Parquet)
- Supports both synchronous and asynchronous processing modes
- Provides real-time progress tracking and performance metrics
- Implements intelligent rate limiting and backoff strategies
- Offers detailed logging and monitoring capabilities

## Features

- Memory-efficient S3 file streaming
- Rate-limited API calls (5000 TPS)
- Async processing with aiohttp
- Support for both pandas and PySpark DataFrames
- Progress tracking with tqdm
- Comprehensive logging and metrics collection
- Resource optimization
- Error handling and retries
- Performance monitoring and analytics

## Requirements

- Python 3.8+
- AWS credentials configured
- Access to target API endpoint
- Apache Spark (optional, for PySpark usage)
- Minimum 4GB RAM recommended
- Stable internet connection for API calls

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd spark_processing_external_api_in_glue
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Configuration

Create a `.env` file with the following variables:

```env
# Required
S3_BUCKET=your-bucket-name
S3_KEY=path/to/your/file.csv
API_ENDPOINT=https://your-api-endpoint.com/process

# Optional
BATCH_SIZE=1000
MAX_WORKERS=20
RATE_LIMIT_CALLS=5000
RATE_LIMIT_PERIOD=1
CHUNK_SIZE=8192
LOG_LEVEL=INFO
```

## Testing

### 1. Unit Tests

Run the unit test suite:
```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_processor.py

# Run with coverage report
pytest --cov=src tests/
```

### 2. Performance Tests

Test performance metrics and scalability:
```bash
# Run performance tests
pytest tests/test_performance_metrics.py

# Test with different batch sizes
BATCH_SIZE=500 pytest tests/test_performance_metrics.py
BATCH_SIZE=2000 pytest tests/test_performance_metrics.py
```

### 3. Integration Tests

Test the complete processing pipeline:
```bash
# Run integration tests
pytest tests/test_integration.py

# Test with mock API
pytest tests/test_integration.py --mock-api
```

### 4. Load Testing

Test system behavior under load:
```bash
# Test with 100k records
pytest tests/test_performance_metrics.py --num-records=100000

# Test with maximum load (1M records)
pytest tests/test_performance_metrics.py --num-records=1000000
```

### 5. API Integration Testing

Test API connectivity and rate limiting:
```bash
# Test API health check
pytest tests/test_api_client.py -k test_health_check

# Test rate limiting
pytest tests/test_api_client.py -k test_rate_limiting
```

### 6. Error Handling Tests

Verify error handling capabilities:
```bash
# Test retry mechanism
pytest tests/test_processor.py -k test_retry_mechanism

# Test circuit breaker
pytest tests/test_processor.py -k test_circuit_breaker
```

### Test Data Generation

Generate test data for local testing:
```bash
# Generate sample data
python scripts/generate_test_data.py --records 1000

# Generate data with specific characteristics
python scripts/generate_test_data.py --records 1000 --error-rate 0.1 --batch-size 100
```

## Usage

Run the processor:

```bash
# Basic usage
python src/processor.py

# With specific configuration
BATCH_SIZE=500 MAX_WORKERS=10 python src/processor.py

# With detailed logging
LOG_LEVEL=DEBUG python src/processor.py
```

## Architecture

The system is built with the following components:

### Data Processing
- Supports both pandas and PySpark DataFrames
- Streams S3 data for memory efficiency
- Processes records in configurable batches
- Async processing with aiohttp for improved throughput

### Performance Features
- Adaptive rate limiting
- Circuit breaker pattern for API resilience
- Performance prediction and anomaly detection
- Real-time analytics and insights
- Telemetry collection

### Monitoring and Metrics
- Real-time progress tracking
- Detailed performance metrics
- API health monitoring
- Resource utilization tracking
- Error rate monitoring

## Performance Optimization

The application is optimized for:

### Memory Usage
- Streams S3 data instead of loading entire file
- Uses bounded queue for producer-consumer pattern
- Processes records in configurable batches
- Efficient garbage collection

### CPU Utilization
- Async I/O for network operations
- Batch processing to reduce overhead
- Efficient data structures
- Session reuse for API calls

### Network Optimization
- Adaptive rate limiting
- Connection pooling
- Circuit breaker pattern
- Batch processing of records

## Monitoring

- Real-time progress bar shows processing status
- Detailed logging to both console and file
- Processing statistics including:
  - Total records processed
  - Error count
  - Duration
  - Average processing speed
  - API response times
  - Resource utilization

## Logs

Logs are stored in the `logs` directory:
- `processor.log`: Contains detailed processing information
- `api_client.log`: API interaction logs and metrics

## Error Handling

- Circuit breaker for API failure protection
- Automatic retry with exponential backoff
- Error logging with stack traces
- Graceful shutdown on interruption
- Failed record tracking
- Anomaly detection for performance issues

## Performance Metrics

The system collects and reports:
- Processing throughput
- API response times
- Error rates
- Resource utilization
- Batch processing statistics
- Performance anomalies

## Troubleshooting

Common issues and solutions:

### API Rate Limiting
If encountering rate limit errors:
1. Check current rate limit settings in .env
2. Monitor api_client.log for rate limit patterns
3. Adjust BATCH_SIZE and MAX_WORKERS accordingly

### Memory Usage
If experiencing memory issues:
1. Reduce BATCH_SIZE in .env
2. Monitor process memory usage in logs
3. Consider enabling garbage collection logging

### Processing Speed
To optimize processing speed:
1. Experiment with different BATCH_SIZE values
2. Monitor CPU usage and adjust MAX_WORKERS
3. Check network latency to API endpoint

### Error Handling
When encountering errors:
1. Check logs/processor.log for error details
2. Verify API endpoint health
3. Review circuit breaker status in metrics