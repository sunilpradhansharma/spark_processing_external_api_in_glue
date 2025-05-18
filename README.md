# Spark Processing with External API Integration

This project implements a scalable data processing system using Apache Spark and AWS Glue that processes millions of records while interacting with external APIs. The system includes comprehensive monitoring, error handling, and local testing capabilities.

## Features

- **Scalable Processing**: Handles millions of records efficiently using Apache Spark
- **API Integration**: Robust external API interaction with:
  - Rate limiting
  - Circuit breaker pattern
  - Retry mechanisms
  - Adaptive connection pooling
- **Monitoring & Telemetry**:
  - OpenTelemetry integration
  - Prometheus metrics
  - Custom analytics engine
  - Performance profiling
- **Error Handling**:
  - Circuit breaker implementation
  - Fault isolation
  - Comprehensive error tracking
- **Local Testing**:
  - Docker-based test environment
  - Mocked S3 interactions
  - Comprehensive test suite

## Architecture

### Components

1. **Processor**:
   - Main processing logic
   - S3 data streaming
   - Batch processing capabilities
   - Memory-efficient queue implementation

2. **API Client**:
   - Robust HTTP client implementation
   - Rate limiting and circuit breaker
   - Connection pooling
   - Telemetry collection

3. **Monitoring**:
   - Real-time metrics collection
   - Performance analytics
   - Health checks
   - Predictive rate limiting

4. **Infrastructure**:
   - AWS Glue integration
   - S3 bucket configuration
   - IAM roles and permissions
   - CloudWatch monitoring

## Local Development

### Prerequisites

- Docker Desktop
- Python 3.11+
- AWS CLI configured with test credentials

### Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd spark_processing_external_api_in_glue
   ```

2. Build and run tests:
   ```bash
   docker-compose build
   docker-compose up
   ```

### Testing

The project includes several test types:

1. **Unit Tests**:
   - API client functionality
   - Batch processing
   - Data transformation

2. **Integration Tests**:
   - End-to-end workflow
   - S3 interactions (mocked)
   - API integration

3. **Performance Tests**:
   - Batch processing efficiency
   - Memory usage monitoring
   - API latency tracking

## Configuration

Key configuration parameters:

```python
@dataclass
class Config:
    s3_bucket: str              # S3 bucket for data
    s3_key: str                 # S3 key for input file
    api_endpoint: str           # External API endpoint
    batch_size: int = 1000      # Records per batch
    max_workers: int = 20       # Concurrent workers
    rate_limit: int = 5000      # API rate limit
    rate_limit_period: int = 1  # Rate limit window
```

## Dependencies

Main dependencies:

- pyspark==3.5.0
- pytest==7.3.1
- boto3==1.26.0
- aiohttp==3.9.3
- pandas==2.2.0
- opentelemetry-api==1.23.0
- prometheus_client==0.20.0
- scikit-learn==1.4.1.post1

## Project Structure

```
.
├── src/
│   ├── processor.py       # Main processing logic
│   ├── exceptions.py      # Custom exceptions
│   ├── metrics.py         # Metrics collection
│   └── monitor.py         # Monitoring implementation
├── tests/
│   ├── test_local.py     # Local test suite
│   └── data/             # Test data
├── infrastructure/        # AWS CDK infrastructure
├── Dockerfile            # Local testing environment
└── docker-compose.yml    # Test orchestration
```

## Current Status

- ✅ Infrastructure deployment
- ✅ Local testing environment
- ✅ Core processing logic
- ✅ API client implementation
- ✅ Monitoring setup
- ✅ Basic test suite
- 🚧 Advanced error handling
- 🚧 Performance optimization
- 🚧 Production deployment

## Next Steps

1. Implement advanced error handling patterns
2. Optimize performance for large-scale processing
3. Add comprehensive monitoring dashboards
4. Deploy to production environment
5. Add more test coverage

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.